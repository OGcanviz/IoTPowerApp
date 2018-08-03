using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace producer
{
    public class TDriveSimulator
    {
        public IList<TDrivePosition> PositionCollection { get; set; }
        public string DriveName { get; set; }
        public DeviceClient Client { get; set; }
        public int Interval { get; set; } = 1;
        public int Speed { get; set; }
        public double Mileage { get; set; }

        private System.Timers.Timer _timer;
        private int _current = 0;
        private long _timeSpan;
        private Random rand = new Random();

        public TDriveSimulator()
        {
            _timer = new System.Timers.Timer();
            _timer.Enabled = false;
            _timer.Elapsed += TimerElapsed;
        }

        private void TimerElapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (_current < PositionCollection.Count)
            {
                var position = PositionCollection[_current];
                Mileage += position.Distance;
                _timer.Stop();
                try
                {
                    var data = new
                    {
                        PartitionKey = position.Name,
                        RowKey = _current.ToString(),
                        Latitude = position.Latitude.ToString(),
                        Longitude = position.Longitude.ToString(),
                        Mileage
                    };
                    var json = JsonConvert.SerializeObject(data);
                    var message = new Message(Encoding.UTF8.GetBytes(json));
                    Client.SendEventAsync(message).GetAwaiter().GetResult();
                    Console.Write('.');
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {ex.Message}");
                }
                for (int i = 1; i < Speed && _current + i < PositionCollection.Count; i++)
                {
                    Mileage += PositionCollection[_current + i].Distance;
                }
                _current += Speed;
                _timer.Start();
            }
        }

        public void LoadPositionCollection(string fileName)
        {
            PositionCollection = new List<TDrivePosition>();
            using (StreamReader sr = new StreamReader(fileName))
            {
                while (sr.Peek() > 0)
                {
                    var source = sr.ReadLine();
                    PositionCollection.Add(new TDrivePosition(source));
                }
            }
            for (int i = 1; i < PositionCollection.Count; i++)
            {
                PositionCollection[i].Distance = PositionCollection[i].DistanceTo(PositionCollection[i - 1]);
            }
        }

        public void Start()
        {
            _timeSpan = (long)(DateTime.Now - PositionCollection[0].DateTime).TotalSeconds;
            _timer.Interval = Interval * 1000;
            _timer.Start();
        }

        public void Stop()
        {
            _timer.Stop();
        }
    }
}