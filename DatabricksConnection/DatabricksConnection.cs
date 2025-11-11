using Microsoft.Azure.Databricks.Client;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text;

namespace DatabricksConnection
{
    public class DatabricksConnection
    {
        private const string token = "xxx";
        private static string clusterId = "1001-123250-xxx";
        private const string databricksInstance = "https://adb-xxx.18.azuredatabricks.net";

        public DatabricksConnection()
        {
            //Connect();
            
            Test();
        }

        private static void Test()
        {
            var httpClient = new HttpClient();

            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            var requestBody = new
            {
                statement = "select * from datahub.dts where product = '11152696' and operation = '7409'-- and equipment = '1ATF0102'",
                warehouse_id = clusterId
            };

            var content = new StringContent(JsonSerializer.Serialize(requestBody), Encoding.UTF8, "application/json");

            var responseThread = httpClient.PostAsync($"{databricksInstance}/api/2.0/sql/statements/", content);
            responseThread.Wait();
            var responseContentThread = responseThread.Result.Content.ReadAsStringAsync();
            responseContentThread.Wait();

            Console.WriteLine(responseContentThread.Result);

        }

        public static async void Connect()
        {
            var httpClient = new HttpClient();
            var databricksClient = DatabricksClient.CreateClient(
                databricksInstance,
                token
            );


            var clusterTask = databricksClient.Clusters.List();
            clusterTask.Wait();

            foreach (var cluster in clusterTask.Result)
            {
                
                Console.WriteLine($"Cluster: {cluster.ClusterName}, State: {cluster.State}");
            }

        }
    }
}
