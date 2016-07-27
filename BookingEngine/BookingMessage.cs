using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using System.Data;

namespace BookingEngine
{
    public class BookingMessage
    {
        private long _jobId = 0;
        private bool _isBookingActionSuccess = false;
        private string _bookingActionResponseMessage = string.Empty;
        private DataSet _bookingActionResult = new DataSet();
        private bool _isNotifySuccess = false;
        private string _notifyResponse = string.Empty;
        private Message _instructions = null;
        private long _publishTimeStamp = 0;
        private long _receivedTimeStamp = 0;
        private long _actionStartTimeStamp = 0;
        private long _sqsQueueWaitTime = 0;
        public long JobId
        {
            get { return this._jobId; }
            set { this._jobId = value; }
        }
        public bool IsBookingActionSuccess
        {
            get { return this._isBookingActionSuccess; }
            set { this._isBookingActionSuccess = value; }
        }
        public string BookingActionResponseMessage
        {
            get { return this._bookingActionResponseMessage; }
            set { this._bookingActionResponseMessage = value; }
        }
        public DataSet BookingActionResult
        {
            get { return this._bookingActionResult; }
            set { this._bookingActionResult = value; }
        }
        public bool IsNotifySuccess
        {
            get { return this._isNotifySuccess; }
            set { this._isNotifySuccess = value; }
        }
        public string NotifyResponse
        {
            get { return this._notifyResponse; }
            set { this._notifyResponse = value; }
        }
        public Message Instructions
        {
            get { return this._instructions; }
            set { this._instructions = value; }
        }
        public long PublishTimeStamp
        {
            get { return this._publishTimeStamp; }
            set { this._publishTimeStamp = value; }
        }
        public long ReceivedTimeStamp
        {
            get { return this._receivedTimeStamp; }
            set { this._receivedTimeStamp = value; }
        }
        public long ActionStartTimeStamp
        {
            get { return this._actionStartTimeStamp; }
            set { this._actionStartTimeStamp = value; }
        }
        public long SQSQueueWaitTime
        {
            get
            {
                return Convert.ToInt64(this.Instructions.Attributes[MessageAttributes.APPROXIMATE_FIRST_RECEIVE_TIME_STAMP]) -
                    Convert.ToInt64(this.Instructions.Attributes[MessageAttributes.SENT_TIME_STAMP]);
            }
        }
        public string PrintIdentifiers()
        {
            return "JobId : " + this.JobId.ToString() + ", MessageId : " + this.Instructions.MessageId;
        }
    }
}
