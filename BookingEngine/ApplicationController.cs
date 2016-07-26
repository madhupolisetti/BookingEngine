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
    public class ApplicationController
    {
        private IAmazonSQS SQSCLIENT = AWSClientFactory.CreateAmazonSQSClient(Amazon.RegionEndpoint.USEast1);
        private Thread[] _bookingClients = null;
        private Thread[] _subscribers = null;
        private byte _bookingClientsRunning = 0;
        private byte _subscribersRunning = 0;
        Mutex _bookingClientsMutex = new Mutex();
        private Queue<BookingMessage> _messageQueue = new Queue<BookingMessage>();
        private SqlConnection _dumpMessageSqlConnection = null;
        private SqlConnection _lockSqlConnection = null;
        private SqlConnection _releaseSqlConnection = null;
        private SqlConnection _confirmSqlConnection = null;
        private SqlConnection _releaseProceedSqlConnection = null;
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
            this._dumpMessageSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._lockSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._releaseSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._confirmSqlConnection = new SqlConnection(SharedClass.ConnectionString);
            this._releaseProceedSqlConnection = new SqlConnection(SharedClass.ConnectionString);
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
            SharedClass.Logger.Info("Stop Signal Received");
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
                    receiveRequest = new ReceiveMessageRequest(SharedClass.SQSQueueArn);
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
                            if (messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString().Equals(BookingActions.STATS))
                            {
                                //ReportStats;
                                this.DeleteMessageFromSQSQueue(bookingMessage);
                                continue;
                            }
                            jobId = 0;
                            switch (messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString())
                            {
                                case BookingActions.LOCK:
                                    this.DumpMessage(bookingMessage, messageBody, out jobId);
                                    break;
                                case BookingActions.RELEASE:
                                    this.DumpMessage(bookingMessage, messageBody, out jobId);
                                    break;
                                case BookingActions.CONFIRM:
                                    this.DumpMessage(bookingMessage, messageBody, out jobId);
                                    break;
                                default:
                                    SharedClass.Logger.Error("Invalid Action " + messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString());
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
                                    case BookingActions.RELEASE:
                                        if(this.ReleaseProceed(bookingMessage.JobId))
                                            this.ReleaseSeats(ref bookingMessage);
                                        break;
                                    case BookingActions.CONFIRM:
                                        this.ConfirmSeats(ref bookingMessage);
                                        break;
                                }
                                NotifyWebServer(ref bookingMessage);
                            }
                        }
                        catch (Exception e)
                        {
                            SharedClass.Logger.Error("Exception while processing Message " + bookingMessage.PrintIdentifiers() + ", Reason : " + e.ToString());
                        }
                        finally
                        {
                            this.DeleteMessageFromSQSQueue(bookingMessage);
                        }
                    }
                    else
                    {
                        Thread.Sleep(1000);
                    }
                }
                else
                {
                    Thread.Sleep(2000);
                }
            }
            SharedClass.Logger.Info("Exit");
            while (!this._bookingClientsMutex.WaitOne())
                Thread.Sleep(100);
            --this._bookingClientsRunning;
            this._bookingClientsMutex.ReleaseMutex();
        }
        private void DumpMessage(BookingMessage bookingMessage, JObject messageBody, out long jobId)
        {
            SharedClass.Logger.Info("Creating JobId. " + bookingMessage.PrintIdentifiers());
            jobId = 0;
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.DUMP_MESSAGE, this._dumpMessageSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.BOX_OFFICE_SHOW_ID, SqlDbType.Int).Value = 
                    Convert.ToInt32(messageBody.SelectToken(MessageBodyAttributes.BOX_OFFICE_SHOW_ID).ToString());

                sqlCmd.Parameters.Add(DataBaseParameters.BOX_OFFICE_MOVIE_ID_FROM_WEB, SqlDbType.Int).Value =
                    Convert.ToInt32(messageBody.SelectToken(MessageBodyAttributes.BOX_OFFICE_MOVIE_ID).ToString());

                sqlCmd.Parameters.Add(DataBaseParameters.BOX_OFFICE_SCREEN_ID_FROM_WEB, SqlDbType.TinyInt).Value =
                    Convert.ToInt16(messageBody.SelectToken(MessageBodyAttributes.BOX_OFFICE_SCREEN_ID).ToString());

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

                sqlCmd.Parameters.Add(DataBaseParameters.BOOKING_ACTION, SqlDbType.VarChar, 10).Value =
                    messageBody.SelectToken(MessageBodyAttributes.BOOKING_ACTION).ToString();

                sqlCmd.Parameters.Add(DataBaseParameters.LOCK_FOR_MINUTES, SqlDbType.TinyInt).Value =
                    messageBody.SelectToken(MessageBodyAttributes.LOCK_FOR_MINUTES) == null ? 10 : Convert.ToByte(messageBody.SelectToken(MessageBodyAttributes.LOCK_FOR_MINUTES).ToString());

                sqlCmd.Parameters.Add(DataBaseParameters.BOOKING_CONFIRMATION_CODE, SqlDbType.VarChar, 10).Value =
                    messageBody.SelectToken(MessageBodyAttributes.BOOKING_CONFIRMATION_CODE) == null ? string.Empty : messageBody.SelectToken(MessageBodyAttributes.BOOKING_CONFIRMATION_CODE).ToString();

                sqlCmd.Parameters.Add(DataBaseParameters.CARD_NUMBER, SqlDbType.VarChar, 20).Value =
                    messageBody.SelectToken(MessageBodyAttributes.CARD_NUMBER) == null ? string.Empty : messageBody.SelectToken(MessageBodyAttributes.CARD_NUMBER).ToString();

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
                if (this._dumpMessageSqlConnection.State != ConnectionState.Open)
                    this._dumpMessageSqlConnection.Open();
                sqlCmd.ExecuteNonQuery();
                if (Convert.ToBoolean(sqlCmd.Parameters[DataBaseParameters.SUCCESS].Value))
                {
                    jobId = Convert.ToInt64(sqlCmd.Parameters[DataBaseParameters.JOB_ID].Value);
                }
                else
                {
                    SharedClass.Logger.Error("JobId Creation Unsuccessful " + bookingMessage.PrintIdentifiers() + ", Reason : " + sqlCmd.Parameters[DataBaseParameters.MESSAGE].Value.ToString());
                }
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception while creating JobId " + bookingMessage.PrintIdentifiers() + " to DB. Reason : " + e.ToString());
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
            SharedClass.Logger.Info("Executing LOCK " + bookingMessage.PrintIdentifiers());
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.LOCK_SEATS, this._lockSqlConnection);
            SqlDataAdapter da = new SqlDataAdapter();
            DataSet ds = new DataSet();
            sqlCmd.CommandType = CommandType.StoredProcedure;
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Value = bookingMessage.JobId;
                sqlCmd.Parameters.Add(DataBaseParameters.SUCCESS, SqlDbType.Bit).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE, SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                da.SelectCommand = sqlCmd;
                da.Fill(ds);
                bookingMessage.IsBookingActionSuccess = Convert.ToBoolean(sqlCmd.Parameters[DataBaseParameters.SUCCESS].Value);
                bookingMessage.BookingActionResponseMessage = sqlCmd.Parameters[DataBaseParameters.MESSAGE].Value.ToString();
                bookingMessage.BookingActionResult = ds;
                SharedClass.Logger.Info("LOCK Result for " + bookingMessage.PrintIdentifiers() + ". " + bookingMessage.IsBookingActionSuccess + ", Message : " + bookingMessage.BookingActionResponseMessage);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception while executing LOCK " + bookingMessage.PrintIdentifiers() + ", Reason : " + e.ToString());
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
        private void ConfirmSeats(ref BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info("Executing CONFIRM " + bookingMessage.PrintIdentifiers());
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.CONFIRM_SEATS, this._confirmSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter da = new SqlDataAdapter();
            DataSet ds = new DataSet();
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Value = bookingMessage.JobId;
                sqlCmd.Parameters.Add(DataBaseParameters.SUCCESS, SqlDbType.Bit).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE, SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                da.SelectCommand = sqlCmd;
                da.Fill(ds);
                bookingMessage.IsBookingActionSuccess = Convert.ToBoolean(sqlCmd.Parameters[DataBaseParameters.SUCCESS].Value);
                bookingMessage.BookingActionResponseMessage = sqlCmd.Parameters[DataBaseParameters.MESSAGE].Value.ToString();
                bookingMessage.BookingActionResult = ds;
                SharedClass.Logger.Info("CONFIRM Result for " + bookingMessage.PrintIdentifiers() + ". " + bookingMessage.IsBookingActionSuccess + ", Message : " + bookingMessage.BookingActionResponseMessage);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception while executing CONFIRM " + bookingMessage.PrintIdentifiers() + ", Reason : " + e.ToString());
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
            SharedClass.Logger.Info("Executing CANCEL " + bookingMessage.PrintIdentifiers());
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.RELEASE_SEATS, this._confirmSqlConnection);
            sqlCmd.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter da = new SqlDataAdapter();
            DataSet ds = new DataSet();
            try
            {
                sqlCmd.Parameters.Add(DataBaseParameters.JOB_ID, SqlDbType.BigInt).Value = bookingMessage.JobId;
                sqlCmd.Parameters.Add(DataBaseParameters.SUCCESS, SqlDbType.Bit).Direction = ParameterDirection.Output;
                sqlCmd.Parameters.Add(DataBaseParameters.MESSAGE, SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                da.SelectCommand = sqlCmd;
                da.Fill(ds);
                bookingMessage.IsBookingActionSuccess = Convert.ToBoolean(sqlCmd.Parameters[DataBaseParameters.SUCCESS].Value);
                bookingMessage.BookingActionResponseMessage = sqlCmd.Parameters[DataBaseParameters.MESSAGE].Value.ToString();
                bookingMessage.BookingActionResult = ds;
                SharedClass.Logger.Info("CANCEL Result for " + bookingMessage.PrintIdentifiers() + ". " + bookingMessage.IsBookingActionSuccess + ", Message : " + bookingMessage.BookingActionResponseMessage);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception while executing CANCEL " + bookingMessage.PrintIdentifiers() + ", Reason : " + e.ToString());
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
        private void NotifyWebServer(ref BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info("Getting Notify Parameters " + bookingMessage.PrintIdentifiers());
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
                    //postingObject.Add(new JProperty("CinemaId", SharedClass.NodeId));
                    postingObject.Add(new JProperty("PublishTimeStamp", bookingMessage.PublishTimeStamp));
                    postingObject.Add(new JProperty("QueueWaitTime", bookingMessage.SQSQueueWaitTime));
                    postingObject.Add(new JProperty("TransactionTimeTaken", DateTime.Now.ToUnixTimeStamp() - bookingMessage.ActionStartTimeStamp));
                    postingObject.Add(new JProperty("ReceiveCount", bookingMessage.Instructions.Attributes[MessageAttributes.APPROXIMATE_RECEIVE_COUNT]));
                    this.Notify(sqlCmd.Parameters[DataBaseParameters.NOTIFY_URL].Value.ToString(), postingObject.ToString(), ref bookingMessage);
                }
                else
                {
                    SharedClass.Logger.Error("No Notify Parameters found " + bookingMessage.PrintIdentifiers());
                }
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception while getting notify parameters. " + bookingMessage.PrintIdentifiers() + ", Reason : " + e.ToString());
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
            SharedClass.Logger.Info("Notifying " + bookingMessage.PrintIdentifiers() + ", Url : " + notifyUrl + ", Payload : " + payload);
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
                SharedClass.Logger.Error("Exception while notifying " + bookingMessage.PrintIdentifiers() + ", Reason : " + e.ToString());
                if (attempt <= SharedClass.NotifyMaxFailedAttempts)
                {
                    SharedClass.Logger.Info("NotifyAttempt (" + attempt + ")" + bookingMessage.PrintIdentifiers());
                    goto notifyRetry;
                }
                else
                {
                    bookingMessage.NotifyResponse = "EXCEPTION:" + e.ToString();
                    SharedClass.Logger.Error("Max Failed Attempts (" + SharedClass.NotifyMaxFailedAttempts + ") Reached " + bookingMessage.PrintIdentifiers());
                } 
            }            
            UpdateNotifyResponse(ref bookingMessage);
        }
        private void UpdateNotifyResponse(ref BookingMessage bookingMessage)
        {
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
                SharedClass.Logger.Error("Exception while updating notify response " + bookingMessage.PrintIdentifiers() + ", Reason : " + e.ToString());
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
            SharedClass.Logger.Info("Deleting From SQS Queue " + bookingMessage.PrintIdentifiers());
            byte attempt = 1;
            DeleteMessageRequest deleteRequest = null;
            DeleteMessageResponse deleteResponse = null;
            deleteRetry:
            try
            {
                deleteRequest = new DeleteMessageRequest();
                deleteRequest.QueueUrl = SharedClass.SQSQueueArn;
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
                    SharedClass.Logger.Error("Exception while deleting " + bookingMessage.PrintIdentifiers() + " , Reason : " + e.ToString());
            }
        }
        private void ChangeMessageVisibility(BookingMessage bookingMessage, byte visibilityTimeOut)
        {
            SharedClass.Logger.Info("Changing Message Visibility " + bookingMessage.PrintIdentifiers());
            ChangeMessageVisibilityRequest request = new ChangeMessageVisibilityRequest();
            ChangeMessageVisibilityResponse response = null;
            request.QueueUrl = SharedClass.SQSQueueArn;
            request.ReceiptHandle = bookingMessage.Instructions.ReceiptHandle;
            request.VisibilityTimeout = visibilityTimeOut;
            try
            {
                response = SQSCLIENT.ChangeMessageVisibility(request);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception while changing visibility of Message " + bookingMessage.PrintIdentifiers());
            }
        }
        private void EnQueue(BookingMessage bookingMessage)
        {
            SharedClass.Logger.Info("EnQueuing " + bookingMessage.PrintIdentifiers());
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
            SharedClass.SQSQueueArn = System.Configuration.ConfigurationManager.AppSettings["SQSQueueArn"];
            if (System.Configuration.ConfigurationManager.AppSettings["NotifyMaxFailedAttempts"] != null)
            {
                byte tempValue = SharedClass.NotifyMaxFailedAttempts;
                if(byte.TryParse(System.Configuration.ConfigurationManager.AppSettings["NotifyMaxFailedAttempts"].ToString(), out tempValue))
                    SharedClass.NotifyMaxFailedAttempts = tempValue;
            }
            if (System.Configuration.ConfigurationManager.AppSettings["PollingInterval"] != null)
            {
                byte tempValue = SharedClass.PollingInterval;
                if (byte.TryParse(System.Configuration.ConfigurationManager.AppSettings["PollingInterval"].ToString(), out tempValue))
                    SharedClass.PollingInterval = tempValue;
            }
        }
        private void UpdateServiceStatus(bool isStopped)
        {
            SharedClass.Logger.Info("Updating ServiceStatus In Database. IsStopped : " + isStopped.ToString());
            SqlConnection sqlCon = new SqlConnection(SharedClass.ConnectionString);
            SqlCommand sqlCmd = new SqlCommand(StoredProcedures.UPDATE_SERVICE_STATUS, sqlCon);
            try
            {
                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.Parameters.Add(DataBaseParameters.SERVICE_NAME, SqlDbType.VarChar, 20).Value = "BookingEngine";
                sqlCmd.Parameters.Add(DataBaseParameters.IS_STOPPED, SqlDbType.Bit).Value = false;
                sqlCon.Open();
                sqlCmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception while updating service status. " + e.ToString());
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
    }
}
