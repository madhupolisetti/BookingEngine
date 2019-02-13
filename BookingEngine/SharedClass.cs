using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace BookingEngine
{
    public static class SharedClass
    {
        #region PRIVATE VARIABLES
        private static string _connectionString = string.Empty;
        private static ILog _logger = null;
        private static bool _hasStopSignal = true;
        private static bool _isServiceCleaned = true;
        private static byte _bookingClientsCount = 1;
        private static byte _subscribersCount = 1;
        private static byte _maximumCallBackRetries = 3;
        private static string _sqsQueueUrl = string.Empty;
        private static string _notifyAuthUserName = string.Empty;
        private static string _notifyAuthPassword = string.Empty;
        private static byte _receiveMessageWaitTime = 20;
        private static byte _deQueueBatchCount = 10;
        private static byte _messageVisibilityTimeOut = 120;
        private static byte _extendMessageVisibilityBy = 60;
        private static byte _bookingClientSleepTimeInSeconds = 2;
        //private static string _defaultBOBookingUserName = "ONLINE";
        #endregion
        #region METHODS
        internal static void InitializeLogger()
        {
            GlobalContext.Properties["LogName"] = DateTime.Now.ToString("yyyyMMdd");
            log4net.Config.XmlConfigurator.Configure();
            _logger = LogManager.GetLogger("Log");
            _logger.Info("Log Initialized");
        }
        #endregion
        #region PROPERTIES
        internal static string ConnectionString
        {
            get { return _connectionString; }
            set { _connectionString = value; }
        }
        internal static ILog Logger
        {
            get { return _logger; }
        }
        internal static bool HasStopSignal
        {
            get { return _hasStopSignal; }
            set { _hasStopSignal = value; }
        }
        internal static bool IsServiceCleaned
        {
            get { return _isServiceCleaned; }
            set { _isServiceCleaned = value; }
        }
        internal static byte MaximumCallBackRetries
        {
            get { return _maximumCallBackRetries; }
            set { _maximumCallBackRetries = value; }
        }
        internal static string SQSQueueUrl
        {
            get { return _sqsQueueUrl; }
            set { _sqsQueueUrl = value; }
        }
        internal static string NotifyAuthUserName
        {
            get { return _notifyAuthUserName; }
            set { _notifyAuthUserName = value; }
        }
        internal static string NotifyAuthPassword
        {
            get { return _notifyAuthPassword; }
            set { _notifyAuthPassword = value; }
        }
        internal static byte ReceiveMessageWaitTime
        {
            get { return _receiveMessageWaitTime; }
            set { _receiveMessageWaitTime = value; }
        }
        public static byte DeQueueBatchCount
        {
            get { return _deQueueBatchCount; }
            set { _deQueueBatchCount = value; }
        }
        internal static byte MessageVisibilityTimeOut
        {
            get { return _messageVisibilityTimeOut; }
            set { _messageVisibilityTimeOut = value; }
        }
        internal static byte BookingClientsCount
        {
            get { return _bookingClientsCount; }
            set { _bookingClientsCount = value; }
        }
        internal static byte SubscribersCount
        {
            get { return _subscribersCount; }
            set { _subscribersCount = value; }
        }
        internal static byte BookingClientSleepTimeInSeconds
        {
            get { return _bookingClientSleepTimeInSeconds; }
            set { _bookingClientSleepTimeInSeconds = value; }
        }
        //internal static string DefaultBOBookingUserName
        //{
        //    get { return _defaultBOBookingUserName; }
        //    set { _defaultBOBookingUserName = value; }
        //}
        #endregion
    }
}
