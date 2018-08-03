#r "Newtonsoft.Json"

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

public class OilChangeService
{
    public static readonly string TableName = "TaxiLastOilChange";
    public static readonly string TaxiCompanyId = "001";

    private string connectionString;

    public OilChangeService(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public async Task<TaxiLastOilChangeEntity> GetLastChangeMileage(string taxiId)
    {
        var storageAccount = CloudStorageAccount.Parse(connectionString);
        var tableClient = storageAccount.CreateCloudTableClient();
        var table = tableClient.GetTableReference(TableName);
        await table.CreateIfNotExistsAsync();

        var retrieveOperation = TableOperation.Retrieve<TaxiLastOilChangeEntity>(TaxiCompanyId, taxiId);
        var retrievedResult = table.Execute(retrieveOperation);

        return (TaxiLastOilChangeEntity)retrievedResult.Result;
    }

    public async Task UpdateDateField(string taxiId, string fieldName, DateTime value)
    {
        var storageAccount = CloudStorageAccount.Parse(connectionString);
        var tableClient = storageAccount.CreateCloudTableClient();
        var table = tableClient.GetTableReference(TableName);
        await table.CreateIfNotExistsAsync();

        var entity = new DynamicTableEntity("001", taxiId);
        entity.Properties.Add(fieldName, new EntityProperty(value));
        var operation = TableOperation.InsertOrMerge(entity);
        await table.ExecuteAsync(operation);
    }
}

public class TaxiLastOilChangeEntity : TableEntity
{
    public TaxiLastOilChangeEntity(string taxiCompanyId, string taxiId)
    {
        this.PartitionKey = taxiCompanyId;
        this.RowKey = taxiId;
    }

    public TaxiLastOilChangeEntity() { }

    public double Mileage { get; set; }

    public DateTime FirstThresholdNotificationSent { get; set; }

    public DateTime SecondThresholdNotificationSent { get; set; }
}