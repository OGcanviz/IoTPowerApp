using System;

namespace producer
{
    public class TDrivePosition
    {
        public string Name;
        public DateTime DateTime;
        public double Latitude;
        public double Longitude;
        public double Distance;
        public TDrivePosition(string des)
        {
            var items = des.Split(',');
            Name = items[0];
            DateTime = DateTime.Parse(items[1]);
            Longitude = Double.Parse(items[2]);
            Latitude = Double.Parse(items[3]);
        }
    }
}
