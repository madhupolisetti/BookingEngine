using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookingEngine
{
    public static class StoredProcedures
    {
        public const string DUMP_MESSAGE = "DumpMessage";
        public const string LOCK_SEATS = "LockSeats";
        public const string RELEASE_SEATS = "ReleaseSeats";
        public const string CONFIRM_SEATS = "ConfirmSeats";
        public const string RELEASE_PROCEED = "ReleaseProceed";
        public const string GET_NOTIFY_PARAMETERE = "GetNotifyParameters";
        public const string UPDATE_NOTIFY_RESPONSE = "UpdateNotifyResponse";
        public const string UPDATE_SERVICE_STATUS = "UpdateServiceStatus";
    }
}
