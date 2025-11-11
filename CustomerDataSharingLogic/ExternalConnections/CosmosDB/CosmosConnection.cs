using CustomerDataSharingLogic.Helpers;
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CustomerDataSharingLogic.ExternalConnections.CosmosDB
{
    public class CosmosConnection
    {
        private static CosmosConnection defaultConnection;

        public static CosmosConnection Default
        {
            get
            {
                if (defaultConnection == null)
                {
                    defaultConnection = new CosmosConnection()
                    {
                        EndpointUri = MyApplicationSettings.GetSetting("CosmosConnection:EndpointUri"),
                        PrimaryKey = MyApplicationSettings.GetSetting("CosmosConnection:PrimaryKey", true),
                        DatabaseId = MyApplicationSettings.GetSetting("CosmosConnection:DatabaseId"),
                        ContainerId = MyApplicationSettings.GetSetting("CosmosConnection:ContainerId")
                    };
                }
                return defaultConnection;
            }
            set
            {
                defaultConnection = value;
            }
        }

        //protected string EndpointUri { get; private set; }
        //protected string PrimaryKey { get; private set; }
        //protected string DatabaseId { get; private set; }
        //protected string ContainerId { get; private set; }

        public string EndpointUri { get; set; }
        public string PrimaryKey { get; set; }
        public string DatabaseId { get; set; }
        public string ContainerId { get; set; }

        public async Task<List<T>> GetData<T>(String statement, string partitionKey = null)
        {
            using (CosmosClient client = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true, ConnectionMode = ConnectionMode.Gateway }))
            {
                var database = client.GetDatabase(DatabaseId);
                var container = database.GetContainer(ContainerId);

                PartitionKey? partitionKeySetting = null;
                if (partitionKey != null)
                    partitionKeySetting = new PartitionKey(partitionKey);

                List<T> result = new List<T>();
                using (FeedIterator<T> feedIterator = container.GetItemQueryIterator<T>(
                    statement,//"select * from ToDos t where t.cost > 9000",
                    null,
                    new QueryRequestOptions() { PartitionKey = partitionKeySetting }))
                {
                    while (feedIterator.HasMoreResults)
                    {
                        foreach (T item in await feedIterator.ReadNextAsync())
                        {
                            result.Add(item);
                        }
                    }
                }
                return result;
            }
        }

        public async Task<T> GetDataByID<T>(String id, string partitionKey)
        {
            using (CosmosClient client = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true }))
            {
                var database = client.GetDatabase(DatabaseId);
                var container = database.GetContainer(ContainerId);

                return await container.ReadItemAsync<T>(id, new PartitionKey(partitionKey));
            }
        }

        public async Task InsertMassData<T>(List<T> massData, string partitionKey)
        {
            List<Task> tasks = new List<Task>(massData.Count);
            foreach (var data in massData)
            {
                tasks.Add(Create(data, partitionKey)
                    .ContinueWith(itemResponse =>
                    {
                        if (!itemResponse.IsCompleted)
                        {
                            if (itemResponse.Exception == null)
                                return;
                            AggregateException innerExceptions = itemResponse.Exception.Flatten();
                            if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                            {
                                var error = itemResponse.Exception.ToString();
                                Console.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                            }
                            else
                            {
                                Console.WriteLine($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                            }
                        }
                    }));
            }
            await Task.WhenAll(tasks);
        }

        public async Task DeleteMassData<T>(List<String> massDataIds, string partitionKey)
        {
            List<Task> tasks = new List<Task>(massDataIds.Count);
            foreach (var id in massDataIds)
            {
                tasks.Add(Delete<T>(id, partitionKey)
                    .ContinueWith(itemResponse =>
                    {
                        if (!itemResponse.IsCompleted)
                        {
                            AggregateException innerExceptions = itemResponse.Exception.Flatten();
                            if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                            {
                                Console.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                            }
                            else
                            {
                                Console.WriteLine($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                            }
                        }
                    }));
            }
            await Task.WhenAll(tasks);
        }

        public async Task Create<T>(T item, string partitionKey)
        {
            using (CosmosClient client = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true }))
            {
                var database = client.GetDatabase(DatabaseId);
                var container = database.GetContainer(ContainerId);

                await container.CreateItemAsync(item, new PartitionKey(partitionKey));
            }
        }

        public async Task Delete<T>(string id, string partitionKey)
        {
            using (CosmosClient client = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true }))
            {
                var database = client.GetDatabase(DatabaseId);
                var container = database.GetContainer(ContainerId);

                await container.DeleteItemAsync<T>(id, new PartitionKey(partitionKey));
            }
        }

        public async Task Update<T>(T item, string partitionKey)
        {
            using (CosmosClient client = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true }))
            {
                var database = client.GetDatabase(DatabaseId);
                var container = database.GetContainer(ContainerId);

                await container.UpsertItemAsync<T>(item, new PartitionKey(partitionKey));
            }
        }
    }
}
