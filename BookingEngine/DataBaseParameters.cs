using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookingEngine
{
    public static class DataBaseParameters
    {
        public const string BOX_OFFICE_SHOW_ID = "@BoxOfficeShowId";
        public const string BOX_OFFICE_MOVIE_ID_FROM_WEB = "@BoxOfficeMovieIdFromWeb";
        public const string BOX_OFFICE_SCREEN_ID_FROM_WEB = "@BoxOfficeScreenIdFromWeb";
        public const string BOX_OFFICE_SHOW_TIME_FROM_WEB = "@BoxOfficeShowTimeFromWeb";
        public const string SEAT_NUMBERS = "@SeatNumbers";
        public const string UNIQUE_KEY = "@UniqueKey";
        public const string LOCK_KEY = "@LockKey";
        public const string BOOKING_ID = "@BookingId";
        public const string BOOKING_ACTION = "@BookingAction";
        public const string LOCK_FOR_MINUTES = "@LockForMinutes";
        public const string BOOKING_CONFIRMATION_CODE = "@BookingConfirmationCode";
        public const string CARD_NUMBER = "@CardNumber";
        public const string COMBO_ID = "@ComboId";
        public const string NUMBER_OF_COMBOS = "@NumberOfCombos";
        public const string APPROXIMATE_FIRST_RECEIVE_TIME_STAMP = "@ApproximateFirstReceiveTimeStamp";
        public const string APPROXIMATE_RECEIVE_COUNT = "@ApproximateReceiveCount";
        public const string SENT_TIME_STAMP = "@SentTimeStamp";
        public const string MESSAGE_ID = "@MessageId";
        public const string JOB_ID = "@JobId";        
        public const string SUCCESS = "@Success";
        public const string MESSAGE = "@Message";
        public const string NOTIFY_URL = "@NotifyUrl";
        public const string NOTIFY_RESPONSE = "@NotifyResponse";

        public const string RELEASE_SEATS_TABLE = "@ReleaseSeatsTable";
        public const string RELEASE_TYPE = "@ReleaseType";
        public const string BOX_OFFICE_TRANSACTION_TIME_STAMP = "@BoxOfficeTransactionTimeStamp";
        public const string ROWS_AFFECTED = "@RowsAffected";

        public const string SERVICE_NAME = "@ServiceName";
        public const string IS_STOPPED = "@IsStopped";
    }
}
