using CustomerDataSharingLogic;
using CustomerDataSharingLogic.Capella;
using CustomerDataSharingLogic.Eviyos;
using CustomerDataSharingLogic.ExternalConnections.ADLS;
using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using CustomerDataSharingLogic.ExternalConnections.TMDB;
using CustomerDataSharingLogic.Gaudi;
using CustomerDataSharingLogic.Helpers;
using CustomerDataSharingLogic.IMSE5515;
using osram.OSAS.Logging;
using System.Text;

namespace CustomerDataSharing
{       
    internal class Program
    {
        static void Main(string[] args)
        {
            //new DatabricksConnection.DatabricksConnection();
            //return;
            //args = new string[] { "EviyosSampleDataCreation" };
            //args = new string[] { "EviyosDifferentBinVersionCheck" };
            //args = new string[] { "IMSE5515" };
            //args = new string[] { "Eviyos" };
            //args = new string[] { "Gaudi" };
            //args = new string[] { "EviyosDataExcelExport" };
            //args = new string[] { "EviyosMissingFileCheck" };

            //args = new string[] { "FilesArriedInCloud" };
            args = new string[] { "DeliveryCreated" };
            //args = new string[] { "Capella" };

            //EviyosDuplicateValidationCheck.EviyosGetValuesWithDuplicates();
            //return;

            try
            {
                //if the free disk size of the target drive is too low -> skip for now (start with 1GB as sample)
                DriveInfo drive = new DriveInfo("C");
                if ((drive.TotalFreeSpace / (1024 * 1024 * 1024)) < 1)
                {
                    LogHelper.Error(typeof(Program), $"free space of disk is less than 1GB ({drive.TotalFreeSpace / (1024 * 1024 * 1024)} GB) -> will skip the run for now to avoid issues");
                    return;
                }

                //var test = MyApplicationSettings.GetSetting("ConfigurationDirectory");
                //Console.WriteLine("test:" + test);

                if (args.Length == 0)
                {
                    Console.WriteLine("Please add the product as parameter, e.g. CustomerDataSharing.exe Eviyos/IMSE5515/Gaudi");
                    return;
                }

                if(!Enum.TryParse(args[0], out EProducts product))
                {
                    var supportedEnumValues = String.Join(" / ", Enum.GetValues(typeof(EProducts)).Cast<EProducts>());
                    Console.WriteLine($"invalid product found as first parameter, {supportedEnumValues} are supported");
                    return;
                }

                switch (product)
                {
                    case EProducts.Eviyos:
                        EviyosBusinessLogic.Execute();

                        if (args.Contains("SendMail"))
                            EviyosMissingFileCheck.Check();

                        EviyosDuplicateValidationCheck.EviyosGetValuesWithDuplicates();
                        break;
                    case EProducts.EviyosMissingFileCheck:
                        //compares the data in cosmos db with snowflake deliveries and checks if all data is available
                        EviyosMissingFileCheck.Check();
                        break;
                    case EProducts.EviyosDifferentBinVersionCheck:
                        EviyosDifferentBinVersionCheck.Check();
                        break;
                    case EProducts.EviyosDataExcelExport:
                        //exports data from cosmos db to an excel file
                        EviyosDataExportFromCosmos.Export();
                        break;
                    case EProducts.EviyosSampleDataCreation:
                        EviyosSampleDataGeneration.CreateSampleData();
                        break;
                    case EProducts.IMSE5515:
                        IMSE5515BusinessLogic.Execute();
                        break;
                    case EProducts.Gaudi:
                        TmdbDBConnection.Location = "PEN";
                        GaudiBusinessLogic.Execute();
                        break;
                    case EProducts.Capella:
                        CapellaBusinessLogic.Execute();
                        break;
                    case EProducts.FilesArriedInCloud:
                        CustomerDataSharingLogic.CloudLogic.FilesArrivedInCloudLogic.Execute();
                        break;
                    case EProducts.DeliveryCreated:
                        CustomerDataSharingLogic.CloudLogic.DeliveryCreated.Execute();
                        break;
                    default:
                        break;
                }
            }
            catch (Exception ex) 
            {
                LogHelper.Error(typeof(Program), "unhandled Exception for Customer Upload tool: " + ex.Message + Environment.NewLine + ex.StackTrace, ex);
            }
        }

        #region data corrections
        private static void ChangeUploadedFiles()
        {
            //due to an issue 4:1 files were shared instead of 3:1 -> replace wrong files

            string batchNumbers = @"1015105093
1015105149
1015181683
1015181691
1015181712
1015181724
1015194723
1015194724
1015194747
1015194779
1015194801
1015208662
1015230471
1015230487
";

            StorageConnection.Default.Initialize();
            StorageConnection.Backup.Initialize();

            foreach (var batchNumber in batchNumbers.Split(new string[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries))
            {
                var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c WHERE c.batch_number = \"{batchNumber}\"");
                cosmosData.Wait();

                foreach (var batchData in cosmosData.Result)
                {
                    foreach (var product in batchData.products)
                    {
                        var fileParts = product.ic_code.Split(new char[] { '_', '.' }, StringSplitOptions.RemoveEmptyEntries);
                        int waferLot;
                        int waferNumber;
                        int posX;
                        int posY;

                        if (Int32.TryParse(fileParts[0], out waferLot) &&
                                Int32.TryParse(fileParts[1], out waferNumber) &&
                                Int32.TryParse(fileParts[2], out posX) &&
                                Int32.TryParse(fileParts[3], out posY))
                        { }
                        else
                        {
                            Console.WriteLine($"Error parsing the ic code {product.ic_code}");
                            continue;
                        }
                        string icc_id = $"{waferLot}_{waferNumber}_{posX}_{posY}";

                        var storageDir = product.ic_code.Substring(0, product.ic_code.IndexOf("_", 6)).Replace("_", "/");

                        var newFile = ($"KEWGBBMD1U/{storageDir}/KEWGBBMD1U_{icc_id}_{product.dmc}");
                        if (newFile == null)
                            continue;

                        var newFileContentTask = StorageConnection.Backup.DownloadFileContent(newFile.Substring(0, newFile.LastIndexOf("/") + 1), newFile.Substring(newFile.LastIndexOf("/") + 1) + ".bin");
                        newFileContentTask.Wait();
                        var newFileContent = newFileContentTask.Result;
                        var test = Encoding.UTF8.GetString(newFileContent, 0, newFileContent.Length);

                        var oldFileContentTask = StorageConnection.Default.DownloadFileContent($"{storageDir}/", $"{product.ic_code}.bin");
                        oldFileContentTask.Wait();
                        var oldFileContent = oldFileContentTask.Result;
                        var test2 = Encoding.UTF8.GetString(oldFileContent, 0, oldFileContent.Length);

                        if (oldFileContent.SequenceEqual(newFileContent))
                            continue;

                        var fileUploadTask = StorageConnection.Default.UploadFileContent($"{storageDir}/", $"{product.ic_code}.bin", newFileContent, StorageConnection.WhenFileAlreadyExists.RenameOld);
                        fileUploadTask.Wait();
                    }
                }
            }
        }

        private static void CheckUploadStatus(int orderNumber)
        {
            StorageConnection.Default.Initialize();
            var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c WHERE c.order_number = \"{orderNumber}\"");
            cosmosData.Wait();

            foreach (var batch in cosmosData.Result)
            {
                foreach (var product in batch.products)
                {
                    var icParts = product.ic_code.Split("_");
                    var directory = $"{icParts[0]}/{icParts[1]}/";
                    var propertiesTask = StorageConnection.Default.GetFileProperties(directory, product.ic_code + ".bin");
                    propertiesTask.Wait();
                    if (propertiesTask.Exception != null || propertiesTask.Result.ContentLength <= 0)
                    {

                    }
                    else
                        Console.WriteLine($"  File: {product.ic_code}, Size: {propertiesTask.Result.ContentLength}");
                    //var status = propertiesTask.Status;
                }
            }
        }

        private static void CloneProdCosmosDBToTest()
        {
            string encryptionKey = @"IZ2tTf2QkFqtJnjzaC2He/IhMud9X0PxUZu2Lx401/c=";
            string encryptionVector = @"6LHzYzU3m0UO8OxAqHKHLw==";

            var prodConnection = new CosmosConnection()
            {
                EndpointUri = "https://eviyos-db-prod.documents.azure.com:443",
                PrimaryKey = EncryptionHelper.DecryptText("J0TROQSXMMTR4k9vP0HgJyY0suEFNFY5F0rJJlkScO+3aAiVdz8bQuOjxrStH7FxlfcdkuGXp36m+094y3TaCbxtTAvXF3oJR/SFLhSWA/trFITPC2CiBwXfSyielPvX", encryptionKey, encryptionVector), //encrypted //MyApplicationSettings.GetSetting("CosmosConnection:PrimaryKey", true),
                DatabaseId = "Eviyos",
                ContainerId = "orders"
            };


            var testConnection = new CosmosConnection()
            {
                EndpointUri = "https://eviyos-db-dev.documents.azure.com:443",
                PrimaryKey = EncryptionHelper.DecryptText("sBsW8H3uPykJaSzra/ts0AjIeD8Sn+HimE8KWZlCDytj6WJv7jdVTor7HXU02dmlyXhF847xSDjnP5KUm5XFGwvi24WxointzohG8hF3BJ9O4f3nFMcFZBReWdJdVH0/", encryptionKey, encryptionVector), //encrypted //MyApplicationSettings.GetSetting("CosmosConnection:PrimaryKey", true),
                DatabaseId = "Eviyos",
                ContainerId = "orders"
            };



            //clear old test data

            var cosmosData = testConnection.GetData<EviyosBox>($"SELECT * FROM c"); // where NOT IS_DEFINED(c.customer_group)
            cosmosData.Wait();
            Console.WriteLine($"{cosmosData.Result.Count} items found in cosmos db");
            List<EviyosBox> dataToDel = cosmosData.Result;

            foreach (var customerId in dataToDel.Select(d => d.customer_id).Distinct())
            {
                var customerData = dataToDel.Where(d => d.customer_id == customerId).Select(d => d.id).ToList();
                while (customerData.Count > 0)
                {
                    var toHandle = customerData.Take(50).ToList();
                    var deleteTask = testConnection.DeleteMassData<EviyosBox>(toHandle, customerId);
                    deleteTask.Wait();
                    customerData.RemoveAll(i => toHandle.Contains(i));
                }
            }


            var dataTask = prodConnection.GetData<EviyosBox>($"SELECT * FROM c");
            dataTask.Wait();
            var data = dataTask.Result;
            foreach (var customerId in data.Select(d => d.customer_id).Distinct())
            {
                var customerData = data.Where(d => d.customer_id == customerId).ToList();
                while (customerData.Count > 0)
                {
                    var toHandle = customerData.Take(50).ToList();
                    var insertTask = testConnection.InsertMassData<EviyosBox>(toHandle, customerId);
                    insertTask.Wait();
                    customerData.RemoveAll(i => toHandle.Contains(i));
                }
            }
        }
        #endregion data corrections
    }
}
