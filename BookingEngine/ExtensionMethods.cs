using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookingEngine
{
    public static class ExtensionMethods
    {
        public static long ToUnixTimeStamp(this DateTime input)
        {
            return Convert.ToInt64((input - new DateTime(1970, 0, 0)).TotalSeconds);
        }
    }
}
