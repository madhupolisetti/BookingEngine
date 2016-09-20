using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookingEngine
{
    public static class BookingActions
    {
        public const string LOCK = "block";
        public const string CONFIRM = "confirm";
        public const string RELEASE = "cancel";
        public const string MANUAL_BOOKING = "manual_booking";
        public const string STATS = "stats";
        public const string RELEASE_ON_DEMAND = "ROD";
        public const string SYNC_MOVIES = "sync_movies";
    }
}
