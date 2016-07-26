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
        private static byte _notifyMaxFailedAttempts = 3;
        private static byte _pollingInterval = 5;
        private static string _sqsQueueArn = string.Empty;
        private static string _notifyAuthUserName = string.Empty;
        private static string _notifyAuthPassword = string.Empty;
        private static byte _receiveMessageWaitTime = 20;
        private static byte _deQueueBatchCount = 10;
        private static byte _messageVisibilityTimeOut = 120;
        private static byte _extendMessageVisibilityBy = 60;
        #endregion
        #region METHODS
        public static void InitializeLogger()
        {
            GlobalContext.Properties["LogName"] = DateTime.Now.ToString("yyyyMMdd");
            log4net.Config.XmlConfigurator.Configure();
            _logger = LogManager.GetLogger("Log");
        }
        #endregion
        #region PROPERTIES
        public static string ConnectionString
        {
            get { return _connectionString; }
            set { _connectionString = value; }
        }
        public static ILog Logger
        {
            get { return _logger; }
        }
        public static bool HasStopSignal
        {
            get { return _hasStopSignal; }
            set { _hasStopSignal = value; }
        }
        public static bool IsServiceCleaned
        {
            get { return _isServiceCleaned; }
            set { _isServiceCleaned = value; }
        }
        public static byte NotifyMaxFailedAttempts
        {
            get { return _notifyMaxFailedAttempts; }
            set { _notifyMaxFailedAttempts = value; }
        }
        public static byte PollingInterval
        {
            get { return _pollingInterval; }
            set { _pollingInterval = value; }
        }
        public static string SQSQueueArn
        {
            get { return _sqsQueueArn; }
            set { _sqsQueueArn = value; }
        }
        public static string NotifyAuthUserName
        {
            get { return _notifyAuthUserName; }
            set { _notifyAuthUserName = value; }
        }
        public static string NotifyAuthPassword
        {
            get { return _notifyAuthPassword; }
            set { _notifyAuthPassword = value; }
        }
        public static byte ReceiveMessageWaitTime
        {
            get { return _receiveMessageWaitTime; }
            set { _receiveMessageWaitTime = value; }
        }
        public static byte DeQueueBatchCount
        {
            get { return _deQueueBatchCount; }
            set { _deQueueBatchCount = value; }
        }
        public static byte MessageVisibilityTimeOut
        {
            get { return _messageVisibilityTimeOut; }
            set { _messageVisibilityTimeOut = value; }
        }
        public static byte BookingClientsCount
        {
            get { return _bookingClientsCount; }
            set { _bookingClientsCount = value; }
        }
        public static byte SubscribersCount
        {
            get { return _subscribersCount; }
            set { _subscribersCount = value; }
        }
        #endregion
    }
}
