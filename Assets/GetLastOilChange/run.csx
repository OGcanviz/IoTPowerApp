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

    List<TaxiLastOilChangeEntity> result = new List<TaxiLastOilChangeEntity>();

    // This is the maximum by default: https://docs.microsoft.com/en-us/rest/api/storageservices/Query-Operators-Supported-for-the-Table-Service?redirectedfrom=MSDN
    int top = 1000;

    // Convert the req name value pairs to a nice Dictorary
    IDictionary<string, string> queryParams = req.GetQueryNameValuePairs()
        .ToDictionary(x => x.Key, x => x.Value);

    // Check if we received a top parameter on the query string to limit the number of results
    if (queryParams.Keys.Contains("top"))
    {
        top = int.Parse(queryParams["top"]);
    }

    // Check if we received a partitionKey parameter on the query string
    if (queryParams.Keys.Contains("partitionKey"))
    {
        string[] partitionKeys = queryParams["partitionKey"].Split(new char[] { ',', ';' });

        if (partitionKeys.Length > 0)
        {
            foreach (var partitionKey in partitionKeys)
            {
                GetTaxiData(result, table, top, partitionKey);
            }
        }
        else
        {
            GetTaxiData(result, table, top, string.Empty);
        }
    }
    else
    {
        GetTaxiData(result, table, top, string.Empty);
    }

    return req.CreateResponse(HttpStatusCode.OK, result.Select(s=>new {TaxiId=s.RowKey, s.Mileage,s.DriverId}), "application/json");

}

private static void GetTaxiData(List<TaxiLastOilChangeEntity> list, CloudTable table, int top, string partitionKey)
{
    TableQuery<TaxiLastOilChangeEntity> query = new TableQuery<TaxiLastOilChangeEntity>()
    {
        SelectColumns = new List<string>()
        {
            "PartitionKey", "RowKey", "Mileage", "DriverId"
        }
    };

    if (!string.IsNullOrEmpty(partitionKey))
    {
        query = query.Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
    }

    // Always sort the output so we return the oldest rows first (that is still newer than the provided timestamp parameter)
    // Unfortunately at time of writing, the TableQuery class does not support sorting, which means we have to perform sorting (and paging) here
    // https://docs.microsoft.com/en-us/rest/api/storageservices/Query-Operators-Supported-for-the-Table-Service?redirectedfrom=MSDN
    var queryOutput = table.ExecuteQuery<TaxiLastOilChangeEntity>(query).OrderBy(row => row.PartitionKey).Take(top);
    list.AddRange(queryOutput);
}


public class TaxiLastOilChangeEntity : TableEntity
{
    public TaxiLastOilChangeEntity(string skey, string srow)
    {
        this.PartitionKey = skey;
        this.RowKey = srow;
    }

    public TaxiLastOilChangeEntity() { }

    public double Mileage { get; set; }

    public string DriverId { get;set; }

}
