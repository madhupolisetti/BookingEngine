using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.IO;

namespace BookingEngine
{
    /// <summary></summary>
    public class ApplicationController
    {
        private IAmazonSQS SQSCLIENT = AWSClientFactory.CreateAmazonSQSClient();
        private Thread[] _bookingClients = null;
        private Thread[] _subscribers = null;
        private byte _bookingClientsRunning = 0;
        private byte _subscribersRunning = 0;
        Mutex _bookingClientsMutex = new Mutex();
        private Queue<BookingMessage> _messageQueue = new Queue<BookingMessage>();
        private SqlConnection _createJobIdSqlConnection = null;
        private SqlConnection _lockSqlConnection = null;
        private SqlConnection _releaseSqlConnection = null;
        private SqlConnection _confirmSqlConnection = null;
        private SqlConnection _releaseProceedSqlConnection = null;
        private SqlConnection _manualBookingSqlConnection = null;
        private SqlConnection _notifyParametersSqlConnection = null;
        private SqlConnection _updateNotifyResponseSqlConnection = null;
        
        public ApplicationController()
        {
            SharedClass.HasStopSignal = false;
            SharedClass.IsServiceCleaned = false;
            this.LoadConfig();
            this.UpdateServiceStatus(false);
        }        
        public void Start()
        {
            SharedClass.Logger.Info("Starting Service");
            this._createJobIdSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._lockSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._releaseSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._confirmSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._releaseProceedSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._manualBookingSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._notifyParametersSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._updateNotifyResponseSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._subscribers = new Thread[SharedClass.SubscribersCount];
            if (this._subscribers.Length == 1)
            {
                this._subscribers[0] = new Thread(new ThreadStart(this.Subscribe));
                this._subscribers[0].Name = "Subscriber";
                this._subscribers[0].Start();
            }
            else
            {
                for (byte index = 1; index <= this._subscribers.Length; index++)
                {
                    this._subscribers[index - 1] = new Thread(new ThreadStart(this.Subscribe));
                    this._subscribers[index - 1].Name = "Subscriber_" + index.ToString();
                    this._subscribers[index - 1].Start();
                }
            }
            this._bookingClients = new Thread[SharedClass.BookingClientsCount];
            if (this._bookingClients.Length == 1)
            {
                this._bookingClients[0] = new Thread(new ThreadStart(this.StartBookingClient));
                this._bookingClients[0].Name = "BookingClient";
                this._bookingClients[0].Start();
            }
            else
            {
                for (byte index = 1; index <= this._bookingClients.Length; index++)
                {
                    this._bookingClients[index - 1] = new Thread(new ThreadStart(this.StartBookingClient));
                    this._bookingClients[index - 1].Name = "BookingClient_" + index.ToString();
                    this._bookingClients[index - 1].Start();
                }
            }
        }
        public void Stop()
        {
            SharedClass.HasStopSignal = true;
            SharedClass.Logger.Info("Processing Stop Signal");
            while (this._subscribersRunning > 0)
            {
                SharedClass.Logger.Info("Still " + this._subscribersRunning.ToString() + " Subscribers Running");
                Thread.Sleep(2000);
            }
            while (this._bookingClientsRunning > 0)
            {
                SharedClass.Logger.Info("Still " + this._bookingClientsRunning.ToString() + " Booking Clients Running");
                Thread.Sleep(2000);
            }
            while (this.QueueCount() > 0)
            {
                SharedClass.Logger.Info("Queue Count : " + this.QueueCount().ToString());
                BookingMessage bookingMessage = this.DeQueue();
                if (bookingMessage != null)
                    this.ChangeMessageVisibility(bookingMessage, 20);
            }
            this.UpdateServiceStatus(true);
            SharedClass.IsServiceCleaned = true;
            SharedClass.Logger.Info("Stop Signal Processed");
        }
        public void Subscribe()
        {
            while (!this._bookingClientsMutex.WaitOne())
                Thread.Sleep(100);
            ++this._subscribersRunning;
            this._bookingClientsMutex.ReleaseMutex();
            SharedClass.Logger.Info("Started");
            ReceiveMessageRequest receiveRequest = null;
            ReceiveMessageResponse receiveResponse = null;
            List<string> attributesList = new List<string>();
            attributesList.Add("All");
            while (!SharedClass.HasStopSignal)
            {
                try
                {
                    receiveResponse = null;
                    receiveRequest = new ReceiveMessageRequest(SharedClass.SQSQueueUrl);
                    receiveRequest.WaitTimeSeconds = SharedClass.ReceiveMessageWaitTime;
                    receiveRequest.MaxNumberOfMessages = SharedClass.DeQueueBatchCount;
                    receiveRequest.AttributeNames = attributesList;
                    receiveRequest.VisibilityTimeout = SharedClass.MessageVisibilityTimeOut;
                    deQueueRetry:
                    try
                    {
                        receiveResponse = SQSCLIENT.ReceiveMessage(receiveRequest);
                    }
                    catch (AmazonSQSException e)
                    {
                        SharedClass.Logger.Error("AmazonSQSException : " + e.ToString());
                        goto deQueueRetry;
                    }
                    if (receiveResponse != null && receiveResponse.Messages.Count > 0)
                    {
                        SharedClass.Logger.Info("Received " + receiveResponse.Messages.Count.ToString() + " New Messages");
                        foreach (Message message in receiveResponse.Messages)
                        {
                            BookingMessage bookingMessage = new BookingMessage();
                            bookingMessage.ReceivedTimeStamp = DateTime.Now.ToUnixTimeStamp();
                            try
                            {   
                                bookingMessage.Instructions = message;
                                this.EnQueue(bookingMessage);
                            }
                            catch (Exception e)
                            {
                                SharedClass.Logger.Error("Exception while EnQueuing " + bookingMessage.PrintIdentifiers() + ", Reason : " + e.ToString());
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    SharedClass.Logger.Error(e.ToString());
                }
            }
            SharedClass.Logger.Info("Exit");
            while (!this._bookingClientsMutex.WaitOne())
                Thread.Sleep(100);
            --this._subscribersRunning;
            this._bookingClientsMutex.ReleaseMutex();
        }
        public void StartBookingClient()
        {
            while (!this._bookingClientsMutex.WaitOne())
                Thread.Sleep(100);
            ++this._bookingClientsRunning;
            this._bookingClientsMutex.ReleaseMutex();
            SharedClass.Logger.Info("Started");
            BookingMessage bookingMessage = null;
            JObject messageBody = null;            
            long jobId = 0;
            while (!SharedClass.HasStopSignal)
            {
                if (this.QueueCount() > 0)
                {
                    bookingMessage = this.DeQueue();
                    if (bookingMessage != null)
                    {                        
                        try
                        {
                            bookingMessage.ActionStartTimeStamp = DateTime.Now.ToUnixTimeStamp();
                            SharedClass.Logger.Info("Started Processing " + bookingMessage.PrintIdentifiers() + ", MessageBody : " + bookingMessage.Instructions.Body);
                            if (!IsValidJSon(bookingMessage.Instructions.Body, out messageBody))
                            {
                                SharedClass.Logger.Error("Invalid JSon");
                                continue;
                            }
                            else if (messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString().Equals(BookingActions.STATS))
                            {
                                this.PushStats();
                                continue;
                            }
                            else if (messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString().Equals(BookingActions.SYNC_MOVIES))
                            {
                                this.SyncMovies(ref bookingMessage);
                                continue;
                            }
                            else if (messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString().Equals(BookingActions.RELEASE_ON_DEMAND, StringComparison.CurrentCultureIgnoreCase))
                            {
                                //RELEASE
                                continue;
                            }                            
                            jobId = 0;
                            bookingMessage.PublishTimeStamp = Convert.ToInt64(messageBody.SelectToken(MessageBodyAttributes.PUBLISH_TIME_STAMP).ToString());
                            switch (messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString())
                            {
                                case BookingActions.LOCK:
                                    this.CreateJobIdForBookingMessage(bookingMessage, messageBody, out jobId);
                                    break;
                                //case BookingActions.RELEASE:
                                //    this.CreateJobIdForBookingMessage(bookingMessage, messageBody, out jobId);
                                //    break;
                                case BookingActions.CONFIRM:
                                    this.CreateJobIdForBookingMessage(bookingMessage, messageBody, out jobId);
                                    break;
                                case BookingActions.MANUAL_BOOKING:
                                    this.CreateJobIdForBookingMessage(bookingMessage, messageBody, out jobId);
                                    break;
                                case BookingActions.CANCEL:
                                    this.CreateJobIdForBookingMessage(bookingMessage, messageBody, out jobId);
                                    break;
                                default:
                                    SharedClass.Logger.Error(string.Format("Invalid Action {0}", messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString()));
                                    break;
                            }
                            if (jobId > 0)
                            {
                                bookingMessage.JobId = jobId;
                                switch (messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString())
                                {
                                    case BookingActions.LOCK:
                                        this.LockSeats(ref bookingMessage);
                                        break;
                                    case BookingActions.CANCEL:
                                        if (this.ReleaseProceed(bookingMessage.JobId))
                                            this.ReleaseSeats(ref bookingMessage);
                                        break;
                                    case BookingActions.CONFIRM:
                                        this.ConfirmSeats(ref bookingMessage);
                                        break;
                                    case BookingActions.MANUAL_BOOKING:
                                        this.ManualBooking(ref bookingMessage);
                                        break;                                
                                }
                                NotifyWebServer(ref bookingMessage);
                            }
                        }
                        catch (Exception e)
                        {
                            SharedClass.Logger.Error(string.Format("Exception while processing Message {0}, Reason : {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
                        }
                        finally
                        {
                            this.DeleteMessageFromSQSQueue(bookingMessage);
                        }
                    }
                    else
                    {
                        try
                        { Thread.Sleep(SharedClass.BookingClientSleepTimeInSeconds * 1000); }
                        catch (Exception e)
                        { }
                    }
                }
                else
                {
                    try
                    { Thread.Sleep(SharedClass.BookingClientSleepTimeInSeconds * 1000); }
                    catch (Exception e)
                    { }
                }
            }
            SharedClass.Logger.Info("Exit");
            while (!this._bookingClientsMutex.WaitOne())
                Thread.Sleep(100);
            --this._bookingClientsRunning;
            this._bookingClientsMutex.ReleaseMutex();
        }
        /// <summary>Creates the job identifier for booking message.</summary>
        /// <param name="bookingMessage">The booking message.</param>
        /// <param name="messageBody">The message body.</param>
        /// <param name="jobId">The job identifier.</param>
        private void CreateJobIdForBookingMessage(BookingMessage bookingMessage, JObject messageBody, out long jobId)
        {
            SharedClass.Logger.Info(string.Format("Creating JobId. {0}", bookingMessage.PrintIdentifiers()));
            jobId = 0;
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.CREATE_JOB_ID_FOR_MESSAGE, this._createJobIdSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.BOX_OFFICE_BOOKING_USER_NAME, SqlDbType.VarChar, 20).Value = 
                    messageBody.SelectToken(MessageBodyAttributes.BOX_OFFICE_BOOKING_USER_NAME) != null ? messageBody.SelectToken(MessageBodyAttributes.BOX_OFFICE_BOOKING_USER_NAME).ToString() : SharedClass.DefaultBOBookingUserName;

                sqlCmd.Parameters.Add(DataBaseParameters.BOX_OFFICE_SHOW_ID, SqlDbType.Int).Value = 
                    Convert.ToInt32(messageBody.SelectToken(MessageBodyAttributes.BOX_OFFICE_SHOW_ID).ToString());
                
                if(messageBody.SelectToken(MessageBodyAttributes.BOX_OFFICE_MOVIE_ID) != null)
                    sqlCmd.Parameters.Add(DataBaseParameters.BOX_OFFICE_MOVIE_ID_FROM_WEB, SqlDbType.Int).Value =
                        Convert.ToInt32(messageBody.SelectToken(MessageBodyAttributes.BOX_OFFICE_MOVIE_ID).ToString());
                
                if(messageBody.SelectToken(MessageBodyAttributes.BOX_OFFICE_SCREEN_ID) != null)
                    sqlCmd.Parameters.Add(DataBaseParameters.BOX_OFFICE_SCREEN_ID_FROM_WEB, SqlDbType.TinyInt).Value =
                        Convert.ToInt16(messageBody.SelectToken(MessageBodyAttributes.BOX_OFFICE_SCREEN_ID).ToString());

                if(messageBody.SelectToken(MessageBodyAttributes.SHOW_TIME) != null)
                    sqlCmd.Parameters.Add(DataBaseParameters.BOX_OFFICE_SHOW_TIME_FROM_WEB, SqlDbType.DateTime).Value =
                        DateTime.Parse(messageBody.SelectToken(MessageBodyAttributes.SHOW_TIME).ToString());

                sqlCmd.Parameters.Add(DataBaseParameters.SEAT_NUMBERS, SqlDbType.VarChar, 500).Value =
                    messageBody.SelectToken(MessageBodyAttributes.SEAT_NUMBERS).ToString();

                sqlCmd.Parameters.Add(DataBaseParameters.UNIQUE_KEY, SqlDbType.VarChar, 10).Value =
                    messageBody.SelectToken(MessageBodyAttributes.UNIQUE_KEY).ToString();

                sqlCmd.Parameters.Add(DataBaseParameters.LOCK_KEY, SqlDbType.VarChar, 10).Value =
                    messageBody.SelectToken(MessageBodyAttributes.LOCK_KEY) == null ? string.Empty : messageBody.SelectToken(MessageBodyAttributes.LOCK_KEY).ToString();

                sqlCmd.Parameters.Add(DataBaseParameters.BOOKING_ID, SqlDbType.BigInt).Value =
                    (messageBody.SelectToken(MessageBodyAttributes.BOOKING_ID) == null) ? 0 : Convert.ToInt64(messageBody.SelectToken(MessageBodyAttributes.BOOKING_ID).ToString());

                sqlCmd.Parameters.Add(DataBaseParameters.BOOKING_ACTION, SqlDbType.VarChar, 20).Value =
                    messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString();

                sqlCmd.Parameters.Add(DataBaseParameters.LOCK_FOR_MINUTES, SqlDbType.TinyInt).Value =
                    messageBody.SelectToken(MessageBodyAttributes.LOCK_FOR_MINUTES) == null ? 10 : Convert.ToByte(messageBody.SelectToken(MessageBodyAttributes.LOCK_FOR_MINUTES).ToString());

                sqlCmd.Parameters.Add(DataBaseParameters.BOOKING_CONFIRMATION_CODE, SqlDbType.VarChar, 10).Value =
                    messageBody.SelectToken(MessageBodyAttributes.BOOKING_CONFIRMATION_CODE) == null ? string.Empty : messageBody.SelectToken(MessageBodyAttributes.BOOKING_CONFIRMATION_CODE).ToString();

                sqlCmd.Parameters.Add(DataBaseParameters.CARD_NUMBER, SqlDbType.VarChar, 20).Value =
                    messageBody.SelectToken(MessageBodyAttributes.CARD_NUMBER) == null ? string.Empty : messageBody.SelectToken(MessageBodyAttributes.CARD_NUMBER).ToString();

                sqlCmd.Parameters.Add(DataBaseParameters.COMBO_ID, SqlDbType.Int).Value =
                    messageBody.SelectToken(MessageBodyAttributes.COMBO_ID) == null ? 0 : Convert.ToInt16(messageBody.SelectToken(MessageBodyAttributes.COMBO_ID).ToString());
                sqlCmd.Parameters.Add(DataBaseParameters.NUMBER_OF_COMBOS, SqlDbType.TinyInt).Value =
                    messageBody.SelectToken(MessageBodyAttributes.COMBO_COUNT) == null ? 0 : Convert.ToByte(messageBody.SelectToken(MessageBodyAttributes.COMBO_COUNT).ToString());

                sqlCmd.Parameters.Add(DataBaseParameters.APPROXIMATE_FIRST_RECEIVE_TIME_STAMP, SqlDbType.BigInt).Value =
                    Math.Ceiling(Convert.ToDouble(bookingMessage.Instructions.Attributes[MessageAttributes.APPROXIMATE_FIRST_RECEIVE_TIME_STAMP]) / 1000);

                sqlCmd.Parameters.Add(DataBaseParameters.APPROXIMATE_RECEIVE_COUNT, SqlDbType.TinyInt).Value =
                    bookingMessage.Instructions.Attributes[MessageAttributes.APPROXIMATE_RECEIVE_COUNT];

                sqlCmd.Parameters.Add(DataBaseParameters.SENT_TIME_STAMP, SqlDbType.BigInt).Value =
                    Math.Ceiling(Convert.ToDouble(bookingMessage.Instructions.Attributes[MessageAttributes.SENT_TIME_STAMP]) / 1000);

                sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE_ID, SqlDbType.VarChar, 500).Value = bookingMessage.Instructions.MessageId;

                sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.SUCCESS, SqlDbType.Bit).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE, SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                if (this._createJobIdSqlConnection.State != ConnectionState.Open)
                    this._createJobIdSqlConnection.Open();
                sqlCmd.ExecuteNonQuery();
                if (sqlCmd.Parameters[DataBaseParameters.JOB_ID].Value != DBNull.Value)
                    jobId = Convert.ToInt64(sqlCmd.Parameters[DataBaseParameters.JOB_ID].Value);
                else
                    SharedClass.Logger.Error(string.Format("JobId Creation Unsuccessful {0}, Reason : {0}", bookingMessage.PrintIdentifiers(), sqlCmd.Parameters[DataBaseParameters.MESSAGE].Value.ToString()));
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while creating JobId {0} to DB. Reason : {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
            }
            finally
            {
                try
                {
                    sqlCmd.Dispose();
                }
                catch (Exception e)
                { 
                    //if(e is NullReferenceException)
                    sqlCmd = null;
                }
            }
        }
        private void LockSeats(ref BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info(string.Format("Executing LOCK {0}", bookingMessage.PrintIdentifiers()));
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.LOCK_SEATS, this._lockSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Value = bookingMessage.JobId;
                sqlCmd.Parameters.Add(DataBaseParameters.SUCCESS, SqlDbType.Bit).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE, SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                if (this._lockSqlConnection.State != ConnectionState.Open)
                    this._lockSqlConnection.Open();
                sqlCmd.ExecuteNonQuery();
                bookingMessage.IsBookingActionSuccess = Convert.ToBoolean(sqlCmd.Parameters[DataBaseParameters.SUCCESS].Value);
                bookingMessage.BookingActionResponseMessage = sqlCmd.Parameters[DataBaseParameters.MESSAGE].Value.ToString();
                SharedClass.Logger.Info(string.Format("LOCK Result for {0} Is {1}. Message: {2}", bookingMessage.PrintIdentifiers(), bookingMessage.IsBookingActionSuccess, bookingMessage.BookingActionResponseMessage));
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while executing LOCK {0}, Reason: {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
                bookingMessage.IsBookingActionSuccess = false;
                bookingMessage.BookingActionResponseMessage = e.Message;
            }
            finally
            {
                try
                {
                    sqlCmd.Dispose();
                }
                catch (Exception e)
                {
                    //if(e is NullReferenceException)
                }
                sqlCmd = null;
            }
        }
        private void ConfirmSeats(ref BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info(string.Format("Executing CONFIRM {0}", bookingMessage.PrintIdentifiers()));
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.CONFIRM_SEATS, this._confirmSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Value = bookingMessage.JobId;
                sqlCmd.Parameters.Add(DataBaseParameters.SUCCESS, SqlDbType.Bit).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE, SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                if (this._confirmSqlConnection.State != ConnectionState.Open)
                    this._confirmSqlConnection.Open();
                sqlCmd.ExecuteNonQuery();
                bookingMessage.IsBookingActionSuccess = Convert.ToBoolean(sqlCmd.Parameters[DataBaseParameters.SUCCESS].Value);
                bookingMessage.BookingActionResponseMessage = sqlCmd.Parameters[DataBaseParameters.MESSAGE].Value.ToString();
                SharedClass.Logger.Info(string.Format("CONFIRM Result for {0} Is {1}, Message: {2}", bookingMessage.PrintIdentifiers(), bookingMessage.IsBookingActionSuccess, bookingMessage.BookingActionResponseMessage));
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while executing CONFIRM {0}, Reason: {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
                bookingMessage.IsBookingActionSuccess = false;
                bookingMessage.BookingActionResponseMessage = e.Message;
            }
            finally
            {
                try
                { sqlCmd.Dispose(); }
                catch (Exception e)
                { 
                    //if(e is NullReferenceException)
                }
                sqlCmd = null;
            }
        }
        private void ReleaseSeats(ref BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info(string.Format("Executing CANCEL {0}", bookingMessage.PrintIdentifiers()));
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.RELEASE_SEATS, this._releaseSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Value = bookingMessage.JobId;
                sqlCmd.Parameters.Add(DataBaseParameters.SUCCESS, SqlDbType.Bit).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE, SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                if (this._releaseSqlConnection.State != ConnectionState.Open)
                    this._releaseSqlConnection.Open();
                sqlCmd.ExecuteNonQuery();
                bookingMessage.IsBookingActionSuccess = Convert.ToBoolean(sqlCmd.Parameters[DataBaseParameters.SUCCESS].Value);
                bookingMessage.BookingActionResponseMessage = sqlCmd.Parameters[DataBaseParameters.MESSAGE].Value.ToString();
                SharedClass.Logger.Info(string.Format("CANCEL Result for {0} Is {1}, Message: {2}", bookingMessage.PrintIdentifiers(), bookingMessage.IsBookingActionSuccess, bookingMessage.BookingActionResponseMessage));
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while executing CANCEL {0}, Reason: {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
                bookingMessage.IsBookingActionSuccess = false;
                bookingMessage.BookingActionResponseMessage = e.Message;
            }
            finally
            {
                try
                { sqlCmd.Dispose(); }
                catch (Exception e)
                { 
                    //if(e is NullReferenceException)
                }
                sqlCmd = null;
            }
        }
        private void ManualBooking(ref BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info(string.Format("Executing MANUAL_CONFIRM {0}", bookingMessage.PrintIdentifiers()));
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.MANUAL_BOOKING, this._manualBookingSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Value = bookingMessage.JobId;
                sqlCmd.Parameters.Add(DataBaseParameters.SUCCESS, SqlDbType.Bit).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE, SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                if (this._manualBookingSqlConnection.State != ConnectionState.Open)
                    this._manualBookingSqlConnection.Open();
                sqlCmd.ExecuteNonQuery();
                bookingMessage.IsBookingActionSuccess = Convert.ToBoolean(sqlCmd.Parameters[DataBaseParameters.SUCCESS].Value);
                bookingMessage.BookingActionResponseMessage = sqlCmd.Parameters[DataBaseParameters.MESSAGE].Value.ToString();
                SharedClass.Logger.Info(string.Format("MANUAL_CONFIRM Result for {0} Is {1}, Message: {2}", bookingMessage.PrintIdentifiers(), bookingMessage.IsBookingActionSuccess, bookingMessage.BookingActionResponseMessage));
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while executing CANCEL {0}, Reason: {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
                bookingMessage.IsBookingActionSuccess = false;
                bookingMessage.BookingActionResponseMessage = e.Message;
            }
            finally
            {
                try
                {
                    sqlCmd.Dispose();
                }
                catch (Exception e)
                {
                    //if(e is NullReferenceException)
                    sqlCmd = null;
                }
            }
        }
        private void PushStats()
        {
            SharedClass.Logger.Info("Pushing Stats");
            SqlConnection sqlCon = new SqlConnection(SharedClass.ConnectionString);
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.PUSH_BOX_OFFICE_STATS, sqlCon);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            try
            {
                sqlCmd.CommandTimeout = 0;
                sqlCon.Open();
                sqlCmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while pushing BoxOfficeStats. {0}", e.ToString()));
            }
            finally
            {
                if (sqlCon.State == ConnectionState.Open)
                    sqlCon.Close();
                try
                {
                    sqlCmd.Dispose();
                }
                catch (Exception e)
                {
                    //if(e is NullReferenceException)
                    sqlCmd = null;
                }
                try
                {
                   sqlCon.Dispose();
                }
                catch (Exception e)
                {
                    //if(e is NullReferenceException)
                    sqlCon = null;
                }
            }
        }
        private void SyncMovies(ref BookingMessage bookingMessage)
        {
            SqlConnection sqlCon = new SqlConnection(SharedClass.ConnectionString);
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.GET_MOVIES_TO_SYNC, sqlCon);
            SqlDataAdapter da = null;
            DataSet ds = null;
            System.Xml.XmlDocument xmlDocument = new System.Xml.XmlDocument();
            System.Xml.XmlElement rootElement = xmlDocument.CreateElement("Movies");
            try
            {
                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.Parameters.Add(DataBaseParameters.INVOKE_SYNC_FROM_BOX_OFFICE, SqlDbType.Bit).Value = true;
                sqlCmd.Parameters.Add(DataBaseParameters.SUCCESS, SqlDbType.Bit).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE, SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.CINEMA_ID, SqlDbType.Int).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.CINEMA_NAME, SqlDbType.VarChar, 50).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.NOTIFY_URL, SqlDbType.VarChar, 200).Direction = ParameterDirection.Output;
                da = new SqlDataAdapter();
                da.SelectCommand = sqlCmd;
                ds = new DataSet();
                da.Fill(ds);
                if (Convert.ToBoolean(sqlCmd.Parameters[DataBaseParameters.SUCCESS].Value))
                {
                    if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
                    {
                        SharedClass.Logger.Info(string.Format("Movies To Sync : {0}", ds.Tables[0].Rows.Count.ToString()));
                        rootElement.SetAttribute("CinemaId", sqlCmd.Parameters[DataBaseParameters.CINEMA_ID].Value.ToString());
                        rootElement.SetAttribute("CinemaName", sqlCmd.Parameters[DataBaseParameters.CINEMA_NAME].Value.ToString());
                        xmlDocument.AppendChild(rootElement);
                        foreach (DataRow movieRow in ds.Tables[0].Rows)
                        {
                            System.Xml.XmlElement movieElement = xmlDocument.CreateElement("Movie");
                            foreach (DataColumn movieProperty in movieRow.Table.Columns)
                            {
                                movieElement.SetAttribute(movieProperty.ColumnName, movieRow[movieProperty.ColumnName].ToString());
                            }
                            rootElement.AppendChild(movieElement);
                        }
                        this.Notify(sqlCmd.Parameters[DataBaseParameters.NOTIFY_URL].Value.ToString(), xmlDocument.OuterXml, ref bookingMessage);
                    }
                    else
                    {
                        SharedClass.Logger.Info("No Movies found to Sync");
                    }
                }
                else
                {
                    SharedClass.Logger.Error(string.Format("MoviesSync ProcedureCall unsuccessful. {0}", sqlCmd.Parameters[DataBaseParameters.MESSAGE].Value.ToString()));
                }
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while syncing movies. Reason : {0}", e.ToString()));
            }
        }
        private void NotifyWebServer(ref BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info(string.Format("Getting Notify Parameters {0}", bookingMessage.PrintIdentifiers()));
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.GET_NOTIFY_PARAMETERE, this._notifyParametersSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter da = new SqlDataAdapter();
            DataSet ds = new DataSet();
            JObject postingObject = new JObject();
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Value = bookingMessage.JobId;                
                sqlCmd.Parameters.Add(DataBaseParameters.NOTIFY_URL, SqlDbType.VarChar, 200).Direction = ParameterDirection.Output;
                da.SelectCommand = sqlCmd;
                da.Fill(ds);
                if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
                {
                    foreach (DataRow dataRow in ds.Tables[0].Rows)
                        foreach (DataColumn dataColumn in dataRow.Table.Columns)
                            postingObject.Add(new JProperty(dataColumn.ColumnName, dataRow[dataColumn.ColumnName]));
                    postingObject.Add(new JProperty("PublishTimeStamp", bookingMessage.PublishTimeStamp));
                    postingObject.Add(new JProperty("QueueWaitTime", bookingMessage.SQSQueueWaitTime));
                    postingObject.Add(new JProperty("TransactionTimeTaken", DateTime.Now.ToUnixTimeStamp() - bookingMessage.ActionStartTimeStamp));
                    postingObject.Add(new JProperty("ReceiveCount", bookingMessage.Instructions.Attributes[MessageAttributes.APPROXIMATE_RECEIVE_COUNT]));
                    this.Notify(sqlCmd.Parameters[DataBaseParameters.NOTIFY_URL].Value.ToString(), postingObject.ToString(), ref bookingMessage);
                }
                else
                {
                    SharedClass.Logger.Error(string.Format("No Notify Parameters found {0}", bookingMessage.PrintIdentifiers()));
                }
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while getting notify parameters. {0}, Reason: {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
            }
            finally
            {
                try
                {
                    sqlCmd.Dispose();
                }
                catch (Exception e)
                { }
                sqlCmd = null;
            }
        }
        private void Notify(string notifyUrl, string payload, ref BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info(string.Format("Notifying {0}, Url: {1}, Payload: {2}", bookingMessage.PrintIdentifiers(), notifyUrl, payload));
            HttpWebRequest request = null;
            HttpWebResponse response = null;
            CredentialCache credentialCache = null;
            StreamWriter streamWriter = null;
            StreamReader streamReader = null;
            byte attempt = 1;
            notifyRetry:
            try
            {
                request = WebRequest.Create(notifyUrl) as HttpWebRequest;
                request.Method = HttpMethod.POST;
                if (SharedClass.NotifyAuthUserName.Length > 0 && SharedClass.NotifyAuthPassword.Length > 0)
                {
                    credentialCache = new CredentialCache();
                    credentialCache.Add(new Uri(notifyUrl), "Basic", new NetworkCredential(SharedClass.NotifyAuthUserName, SharedClass.NotifyAuthPassword));
                    request.Credentials = credentialCache;
                }   
                request.ContentType = "application/form-url-encoded";
                streamWriter = new StreamWriter(request.GetRequestStream());
                streamWriter.Write(payload);
                streamWriter.Flush();
                streamWriter.Close();
                response = request.GetResponse() as HttpWebResponse;
                streamReader = new StreamReader(response.GetResponseStream());
                bookingMessage.NotifyResponse = streamReader.ReadToEnd();
                bookingMessage.IsNotifySuccess = true;
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while notifying {0}, Reason: {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
                if (attempt <= SharedClass.MaximumCallBackRetries)
                {
                    ++attempt;
                    SharedClass.Logger.Info(string.Format("NotifyAttempt ({0}). {1}", attempt, bookingMessage.PrintIdentifiers()));
                    goto notifyRetry;
                }
                else
                {
                    bookingMessage.NotifyResponse = string.Format("EXCEPTION:{0}", e.ToString());
                    SharedClass.Logger.Error(string.Format("Max Failed Attempts ({0}) Reached {1}", SharedClass.MaximumCallBackRetries, bookingMessage.PrintIdentifiers()));
                } 
            }            
            UpdateNotifyResponse(ref bookingMessage);
        }
        private void UpdateNotifyResponse(ref BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info(string.Format("Updating NotifyResponse {0}, Response: {1}", bookingMessage.PrintIdentifiers(), bookingMessage.NotifyResponse));
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.UPDATE_NOTIFY_RESPONSE, this._updateNotifyResponseSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Value = bookingMessage.JobId;
                sqlCmd.Parameters.Add(DataBaseParameters.NOTIFY_RESPONSE, SqlDbType.VarChar, -1).Value = bookingMessage.NotifyResponse;
                if (this._updateNotifyResponseSqlConnection.State != ConnectionState.Open)
                    this._updateNotifyResponseSqlConnection.Open();
                sqlCmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while updating notify response {0}, Reason: {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
            }
            finally
            {
                try
                { sqlCmd.Dispose(); }
                catch (Exception e)
                { 
                    //if(e is NullReferenceException)
                }
                sqlCmd = null;
            }
        }
        private bool IsValidJSon(string input, out JObject outputJObject)
        {
            outputJObject = null;
            try
            {
                outputJObject = JObject.Parse(input);
                return true;
            }
            catch (Exception e)
            {   
                return false;
            }
        }
        private bool ReleaseProceed(long jobId)
        {
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.RELEASE_PROCEED, this._releaseProceedSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Value = jobId;
            sqlCmd.Parameters.Add(DataBaseParameters.SUCCESS, SqlDbType.Bit).Direction = ParameterDirection.Output;
            sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE, SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
            try
            {
                if (this._releaseProceedSqlConnection.State != ConnectionState.Open)
                    this._releaseProceedSqlConnection.Open();
                sqlCmd.ExecuteNonQuery();
                return Convert.ToBoolean(sqlCmd.Parameters[DataBaseParameters.SUCCESS].Value.ToString());
            }
            catch (Exception e)
            {
                return false;
            }
            finally
            {
                try
                { sqlCmd.Dispose(); }
                catch (Exception e)
                { }
                sqlCmd = null;
            }
        }
        private void DeleteMessageFromSQSQueue(BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info(string.Format("Deleting From SQS Queue {0}", bookingMessage.PrintIdentifiers()));
            byte attempt = 1;
            DeleteMessageRequest deleteRequest = null;
            DeleteMessageResponse deleteResponse = null;
            deleteRetry:
            try
            {
                deleteRequest = new DeleteMessageRequest();
                deleteRequest.QueueUrl = SharedClass.SQSQueueUrl;
                deleteRequest.ReceiptHandle = bookingMessage.Instructions.ReceiptHandle;
                deleteResponse = SQSCLIENT.DeleteMessage(deleteRequest);
            }
            catch (Exception e)
            {
                //if(e is InvalidIdFormatException || e is ReceiptHandleIsInvalidException)
                attempt += 1;
                if (attempt <= 5)
                    goto deleteRetry;
                else
                    SharedClass.Logger.Error(string.Format("Exception while deleting {0}, Reason: {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
            }
        }
        private void ChangeMessageVisibility(BookingMessage bookingMessage, byte visibilityTimeOutInSeconds)
        {
            SharedClass.Logger.Info(string.Format("Changing Message Visibility {0}", bookingMessage.PrintIdentifiers()));
            ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest();
            ChangeMessageVisibilityResponse response = null;
            request.QueueUrl = SharedClass.SQSQueueUrl;
            request.ReceiptHandle = bookingMessage.Instructions.ReceiptHandle;
            request.VisibilityTimeout = visibilityTimeOutInSeconds;
            try
            {
                response = SQSCLIENT.ChangeMessageVisibility(request);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while changing visibility of Message {0}, Reason: {1}", bookingMessage.PrintIdentifiers(), e.ToString()));
            }
        }
        private void ChangeMessageVisibilityBatch(List<BookingMessage> bookingMessages)
        {
            ChangeMessageVisibilityBatchRequest request = new ChangeMessageVisibilityBatchRequest();
            ChangeMessageVisibilityBatchResponse response = null;
            request.QueueUrl = SharedClass.SQSQueueUrl;
            foreach (BookingMessage bookingMessage in bookingMessages)
                request.Entries.Add(new ChangeMessageVisibilityBatchRequestEntry(bookingMessage.Instructions.MessageId, bookingMessage.Instructions.ReceiptHandle));
            try
            {
                response = SQSCLIENT.ChangeMessageVisibilityBatch(request);                
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while changing MessageVisibility for Batch : {0}" , e.ToString()));
            }
        }
        private void EnQueue(BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info(string.Format("EnQueuing {0}", bookingMessage.PrintIdentifiers()));
            lock (this._messageQueue)
                this._messageQueue.Enqueue(bookingMessage);
        }
        private int QueueCount()
        {
            lock (this._messageQueue)
                return this._messageQueue.Count;
        }
        private BookingMessage DeQueue()
        {
            lock (this._messageQueue)
                return this._messageQueue.Dequeue();
        }
        private void LoadConfig()
        {
            SharedClass.InitializeLogger();
            SharedClass.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;
            SharedClass.SQSQueueUrl = System.Configuration.ConfigurationManager.AppSettings["SQSQueueURL"];
            SharedClass.Logger.Info("SQSQueueURL: " + SharedClass.SQSQueueUrl);
            SharedClass.Logger.Info("ConnectionString: " + SharedClass.ConnectionString);
            if (System.Configuration.ConfigurationManager.AppSettings["DefaultBOBookingUserName"] != null)
                SharedClass.DefaultBOBookingUserName = System.Configuration.ConfigurationManager.AppSettings["DefaultBOBookingUserName"].ToString();
            SharedClass.Logger.Info("DefaultBOBookingUserName: " + SharedClass.DefaultBOBookingUserName);
            if(System.Configuration.ConfigurationManager.AppSettings["SQSSubscribersCount"] != null)
            {
                byte tempValue = SharedClass.SubscribersCount;
                if (byte.TryParse(System.Configuration.ConfigurationManager.AppSettings["SQSSubscribersCount"].ToString(), out tempValue))
                    SharedClass.SubscribersCount = tempValue;
            }
            SharedClass.Logger.Info("SQSSubscribersCount: " + SharedClass.SubscribersCount.ToString());
            if (System.Configuration.ConfigurationManager.AppSettings["MaximumCallBackRetries"] != null)
            {
                byte tempValue = SharedClass.MaximumCallBackRetries;
                if(byte.TryParse(System.Configuration.ConfigurationManager.AppSettings["MaximumCallBackRetries"].ToString(), out tempValue))
                    SharedClass.MaximumCallBackRetries = tempValue;
            }
            SharedClass.Logger.Info("MaximumCallBackRetries : " + SharedClass.MaximumCallBackRetries.ToString());
            if (System.Configuration.ConfigurationManager.AppSettings["BookingClientSleepTimeInSeconds"] != null)
            {
                byte tempValue = SharedClass.BookingClientSleepTimeInSeconds;
                if (byte.TryParse(System.Configuration.ConfigurationManager.AppSettings["BookingClientSleepTimeInSeconds"].ToString(), out tempValue))
                    SharedClass.BookingClientSleepTimeInSeconds = tempValue;
            }
            SharedClass.Logger.Info("BookingClientSleepTimeInSeconds : " + SharedClass.BookingClientSleepTimeInSeconds.ToString());
            if(System.Configuration.ConfigurationManager.AppSettings["BookingClientsCount"] != null)
            {
                byte tempValue = SharedClass.BookingClientsCount;
                if (byte.TryParse(System.Configuration.ConfigurationManager.AppSettings["BookingClientsCount"].ToString(), out tempValue))
                    SharedClass.BookingClientsCount = tempValue;
            }
            SharedClass.Logger.Info("BookingClientsCount : " + SharedClass.BookingClientsCount);
        }
        private void UpdateServiceStatus(bool isStopped)
        {
            SharedClass.Logger.Info(string.Format("Updating ServiceStatus In Database. IsStopped : {0}", isStopped.ToString()));
            SqlConnection sqlCon = new SqlConnection(SharedClass.ConnectionString);
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.UPDATE_SERVICE_STATUS, sqlCon);
            try
            {   
                string serviceName = this.GetServiceName();
                serviceName = serviceName.Length > 0 ? serviceName : "BookingEngine";
                SharedClass.Logger.Info("Service Name : " + serviceName);

                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.Parameters.Add(DataBaseParameters.SERVICE_NAME, SqlDbType.VarChar, 32).Value = serviceName;
                sqlCmd.Parameters.Add(DataBaseParameters.IS_STOPPED, SqlDbType.Bit).Value = isStopped;
                sqlCon.Open();
                sqlCmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error(string.Format("Exception while updating service status. {0}", e.ToString()));
            }
            finally
            {
                if (sqlCon.State == ConnectionState.Open)
                    sqlCon.Close();
                try
                {
                    sqlCmd.Dispose();
                    sqlCon.Dispose();
                }
                catch (Exception e)
                {
                }
            }
        }
        private string GetServiceName()
        {
            string serviceName = string.Empty;
            try
            {
                int processId = System.Diagnostics.Process.GetCurrentProcess().Id;
                System.Management.ManagementObjectSearcher searcher = new System.Management.ManagementObjectSearcher("SELECT * FROM Win32_Service where ProcessId = " + processId);
                System.Management.ManagementObjectCollection collection = searcher.Get();
                serviceName = (string)collection.Cast<System.Management.ManagementBaseObject>().First()["Name"];
            }
            catch(Exception e)
            {
                serviceName = string.Empty;
                SharedClass.Logger.Error(string.Format("Exception while fetching the service name from OS. Reason : {0}", e.ToString()));
            }
            return serviceName;
        }
    }
}
