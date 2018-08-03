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
    var tableName = "TaxiDataFromIoTHub";

    // Connect to the Storage account.
    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

    CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

    CloudTable table = tableClient.GetTableReference(tableName);

    await table.CreateIfNotExistsAsync();

    List<TaxiDataEntity> result = new List<TaxiDataEntity>();

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

    string timestamp = string.Empty;
    // Check if we received a top parameter on the query string to limit the number of results
    if (queryParams.Keys.Contains("timestamp"))
    {
        timestamp = queryParams["timestamp"];
    }

    // Check if we received a partitionKey parameter on the query string
    if (queryParams.Keys.Contains("partitionKey"))
    {
        string[] partitionKeys = queryParams["partitionKey"].Split(new char[] { ',', ';' });

        if (partitionKeys.Length > 0)
        {
            foreach (var partitionKey in partitionKeys)
            {
                GetTaxiData(result, table, top, timestamp, partitionKey);
            }
        }
        else
        {
            GetTaxiData(result, table, top, timestamp, string.Empty);
        }
    }
    else
    {
        GetTaxiData(result, table, top, timestamp, string.Empty);
    }

    return req.CreateResponse(HttpStatusCode.OK, result, "application/json");

}

private static void GetTaxiData(List<TaxiDataEntity> list, CloudTable table, int top, string timestamp, string partitionKey)
{
    TableQuery<TaxiDataEntity> query = new TableQuery<TaxiDataEntity>()
    {
        SelectColumns = new List<string>()
        {
            "PartitionKey", "RowKey", "Version", "Latitude", "Longitude", "Timestamp", "Mileage"
        }
    };

    if (!string.IsNullOrEmpty(partitionKey))
    {
        query = query.Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
    }

    // Make sure the timestamp parameter wasn't just empty
    if (!string.IsNullOrEmpty(timestamp))
    {

        // We expect the timestamp to be in the URL Decoded form of: 2018-05-31T23%3A46%3A33.454Z
        // Force parsing it as a DateTime
        DateTime timestampValue = DateTime.Parse(WebUtility.UrlDecode(timestamp));

        // Check first to see if we already had a predicate in our WHERE clause, otherwise create a new WHERE clause here
        if (query.FilterString is null)
        {
            // GreaterThan: even though this technically could mean we loose multiple events with the exact same timestamp. But for simplicity we will use >
            query = query.Where(TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, timestampValue));
        }
        else
        {
            // We already had a WHERE clause in our query, so combine the existing WHERE clause and add a predicate with the AND operator
            // GreaterThan: even though this technically could mean we loose multiple events with the exact same timestamp. But for simplicity we will use >
            query = query.Where(TableQuery.CombineFilters(query.FilterString, TableOperators.And, TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, timestampValue)));
        }
    }

    // Always sort the output so we return the oldest rows first (that is still newer than the provided timestamp parameter)
    // Unfortunately at time of writing, the TableQuery class does not support sorting, which means we have to perform sorting (and paging) here
    // https://docs.microsoft.com/en-us/rest/api/storageservices/Query-Operators-Supported-for-the-Table-Service?redirectedfrom=MSDN
    var queryOutput = table.ExecuteQuery<TaxiDataEntity>(query).OrderBy(row => row.Timestamp).Take(top);
    list.AddRange(queryOutput);
}


public class TaxiDataEntity : TableEntity
{
    public TaxiDataEntity(string skey, string srow)
    {
        this.PartitionKey = skey;
        this.RowKey = srow;
    }

    public TaxiDataEntity() { }

    public string Latitude { get; set; }
    public string Longitude { get; set; }
    public double Mileage { get; set; }

}
