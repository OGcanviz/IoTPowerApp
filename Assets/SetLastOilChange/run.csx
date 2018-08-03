using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    log.Info("C# HTTP trigger function processed a request.");

    var connectionString = Environment.GetEnvironmentVariable("WEBSITE_CONTENTAZUREFILECONNECTIONSTRING", EnvironmentVariableTarget.Process);
    var tableName = "TaxiLastOilChange";

    // Connect to the Storage account.
    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

    CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

    CloudTable table = tableClient.GetTableReference(tableName);

    await table.CreateIfNotExistsAsync();

    var data = await req.Content.ReadAsAsync<TaxiLastOilChangeEntity[]>();
    
    if(data != null && data.Length > 0)
    {
        var batchInsert = new TableBatchOperation();
        foreach(var item in data)
        {
            var entity = new DynamicTableEntity();
            entity.PartitionKey = "001";
            entity.RowKey = item.TaxiId;
            entity.Properties.Add(nameof(item.Mileage), new EntityProperty(item.Mileage));
            entity.Properties.Add(nameof(item.DriverId), new EntityProperty(item.DriverId));
            var tableOperation = TableOperation.InsertOrMerge(entity);
            batchInsert.Add(tableOperation);
        }
        await table.ExecuteBatchAsync(batchInsert);
    }

    return req.CreateResponse(HttpStatusCode.OK, string.Empty, "application/json");

}

public class TaxiLastOilChangeEntity : TableEntity
{
    public string TaxiId { get; set; }
    public double Mileage { get; set; }
    public string DriverId { get; set; }
}
