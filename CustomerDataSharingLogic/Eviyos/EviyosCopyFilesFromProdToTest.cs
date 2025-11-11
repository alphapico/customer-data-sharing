using CustomerDataSharingLogic.ExternalConnections.ADLS;
using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using System;
using System.Collections.Generic;

namespace CustomerDataSharingLogic.Eviyos
{
    public class EviyosCopyFilesFromProdToTest
    {
        public static void Copy()
        {
            var deliveryIDs = new List<string>() { "51633219", "51633216", "51633421" };
            deliveryIDs.ForEach(d => CopyDelivery(d));
        }

        private static void CopyDelivery(string deliveryId)
        {
            Console.WriteLine($"handle delivery {deliveryId}");

            var outputConnection = StorageConnection.GetNewStorageConnection(
                "ec1ca250-c234-4d56-a76b-7dfb9eee0c46",
                "",
                 "eviyosdmcstorage",
                 "measurements"
                );

            var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.delivery_number = '{deliveryId}'");
            cosmosData.Wait();
            Console.WriteLine($"  {cosmosData.Result.Count} items found in cosmos db");
            List<EviyosBox> data = cosmosData.Result;
            foreach (var e in data)
            {
                foreach(var product in e.products)
                {
                    var icCode = product.ic_code;
                    var icCodeParts = icCode.Split('_');
                    var path = $"{icCodeParts[0]}/{icCodeParts[1]}/{icCode}.bin";

                    Console.WriteLine($"  handle file {path} -> download");
                    var storageDir = product.ic_code.Substring(0, product.ic_code.IndexOf("_", 6)).Replace("_", "/");
                    var fileContentTask = StorageConnection.Default.DownloadFileContent($"{storageDir}/", $"{product.ic_code}.bin");
                    fileContentTask.Wait();
                    var content = fileContentTask.Result;

                    Console.WriteLine($"  handle file {path} -> downloaded, upload now");
                    var fileUploadTask = outputConnection.UploadFileContent($"{storageDir}/", $"{product.ic_code}.bin", content, StorageConnection.WhenFileAlreadyExists.RenameOld);
                    fileUploadTask.Wait();
                    Console.WriteLine($"  handle file {path} -> uploaded");
                }
            }
        }
    }
}