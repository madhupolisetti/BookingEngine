using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookingEngine
{
    public static class StoredProcedures
    {
        public const string CREATE_JOB_ID_FOR_MESSAGE = "CreateJobIdForBookingMessage";
        public const string LOCK_SEATS = "LockSeats";
        public const string RELEASE_SEATS = "ReleaseSeats";
        public const string CONFIRM_SEATS = "ConfirmSeats";
        public const string RELEASE_PROCEED = "ReleaseProceed";
        public const string MANUAL_BOOKING = "ManualBooking";
        public const string GET_NOTIFY_PARAMETERE = "GetNotifyParameters";
        public const string UPDATE_NOTIFY_RESPONSE = "UpdateNotifyResponse";
        public const string PUSH_BOX_OFFICE_STATS = "PushBoxOfficeStats";
        public const string UPDATE_SERVICE_STATUS = "UpdateServiceStatus";
        public const string GET_MOVIES_TO_SYNC = "GetMoviesToSync";
    }
}
