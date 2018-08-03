#load "EmailService.csx"
#load "OilChangeService.csx"
#r "Microsoft.ServiceBus"
#r "Newtonsoft.Json"

using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.Azure.WebJobs;
using Newtonsoft.Json;

public static readonly string SendEmailFunctionURL = Environment.GetEnvironmentVariable("SendEmailFunctionURL", EnvironmentVariableTarget.Process);
public static readonly string ConnectionString = Environment.GetEnvironmentVariable("WEBSITE_CONTENTAZUREFILECONNECTIONSTRING", EnvironmentVariableTarget.Process);

public static readonly string MaintenanceManager = "huberts@canviz.com";
public static readonly string MailSubject = "Oil Change Notification";
public static readonly double OilChangeMileageThreshold = 4000;
public static readonly double OilChangeMileageHighestLimit = 5000;

[Singleton("{PartitionKey}")]
public static async Task Run(EventData myEventHubMessage, TraceWriter log)
{
    var messageBody = Encoding.UTF8.GetString(myEventHubMessage.GetBytes());
    log.Info($"Event: {messageBody}");
    if(string.IsNullOrEmpty(messageBody))
    {
        return;
    }
    
    var message = JsonConvert.DeserializeObject<IoTHubMessage>(messageBody);
    log.Info($"TaxiId: {message.PartitionKey}, Mileage: {message.Mileage}");
    var oilChangeService = new OilChangeService(ConnectionString);
    var lastChange = await oilChangeService.GetLastChangeMileage(message.PartitionKey);
    var lastChangeMileage = 0d;
    var lastWarningTime = DateTime.MinValue;
    var lastErrorTime = DateTime.MinValue;

    if (lastChange != null)
    {
        lastWarningTime = lastChange.FirstThresholdNotificationSent;
        lastErrorTime = lastChange.SecondThresholdNotificationSent;
        lastChangeMileage = lastChange.Mileage;
        log.Info($"Last 1st Threshold Notification Sent: {lastWarningTime}, Last 2nd Threshold Notification Sent: {lastErrorTime}");
    }

    var mileageSinceLastChange = message.Mileage - lastChangeMileage;

    if (mileageSinceLastChange <= OilChangeMileageThreshold
        || lastErrorTime >= DateTime.Today
        || (lastWarningTime >= DateTime.Today && mileageSinceLastChange <= OilChangeMileageHighestLimit))
    {
        return;
    }

    var info = $"Taxi {message.PartitionKey} has accumulated {mileageSinceLastChange} miles since it's last oil change.";
    log.Info(info);

    var body = info + $" It has passed the recommended oil change mileague threshold ({OilChangeMileageThreshold} miles) ";
    var timestamp = DateTime.Now;
    if (mileageSinceLastChange > OilChangeMileageHighestLimit)
    {
        body += $"and has exceeded the maximum mileague limit ({OilChangeMileageHighestLimit} miles) without an oil change.  This taxi must must be taken out of service until the oil is changed.";
        await oilChangeService.UpdateDateField(message.PartitionKey, nameof(lastChange.SecondThresholdNotificationSent), timestamp);
    }
    else if (mileageSinceLastChange > OilChangeMileageThreshold)
    {
        body += $"but has not exceeded the maximum mileague limit ({OilChangeMileageHighestLimit} miles) without an oil change.  This taxi can remain in service until the maximum limit is reached.";
        await oilChangeService.UpdateDateField(message.PartitionKey, nameof(lastChange.FirstThresholdNotificationSent), timestamp);
    }
    var emailService = new EmailService(SendEmailFunctionURL);
    await emailService.SendAsync(new[] { MaintenanceManager }, MailSubject, body);
    log.Info("Email sent.");
}

public class IoTHubMessage
{
    public string PartitionKey { get; set; }
    public double Mileage { get; set; }
}