using Microsoft.Azure.Devices.Client;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace producer
{
    class Program
    {
        static void Main(string[] args)
        {
            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .AddCommandLine(args).Build();
            var ioTHub = config.GetSection("IoT Hub");
            var baseMileages = config.GetSection("baseMileages");
            var clientFiles = config["clientFiles"];
            int speed;
            if (!int.TryParse(config["speed"], out speed))
            {
                speed = 1;
            }

            Console.WriteLine("Press any key to quit");

            DeviceClient deviceClient = DeviceClient.CreateFromConnectionString(ioTHub["connectionString"], TransportType.Mqtt);

            var files = clientFiles.Split(new char[] { ',', ';' })
                .Select(s => (IEnumerable<string>)Directory.GetFiles(".", s, SearchOption.AllDirectories))
                .Aggregate((a, b) => a.Concat(b))
                .ToArray();

            var driveSimulators = files
                .Select(o =>
                {
                    var driveName = Path.GetFileNameWithoutExtension(o);
                    double mileage;
                    double.TryParse(baseMileages[driveName], out mileage);
                    var sim = new TDriveSimulator()
                    {
                        DriveName = driveName,
                        Client = deviceClient,
                        Speed = speed,
                        Mileage = mileage
                    };
                    sim.LoadPositionCollection(o);
                    sim.Start();
                    return sim;
                }).ToArray();

            Console.ReadLine();

            foreach (var driveSimulator in driveSimulators)
            {
                driveSimulator.Stop();
            }
        }
    }
}
