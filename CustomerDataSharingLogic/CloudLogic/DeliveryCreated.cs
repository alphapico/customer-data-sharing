using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using CustomerDataSharingLogic.Helpers;
using Newtonsoft.Json;
using osram.OSAS.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static CustomerDataSharingLogic.Eviyos.EviyosBusinessLogic;

namespace CustomerDataSharingLogic.CloudLogic
{
    public class DeliveryCreated
    {
        private static string inboxStorageSettings = $"DefaultEndpointsProtocol=https;AccountName={CloudLogicSharedFunctions.storageAccountName};AccountKey={CloudLogicSharedFunctions.storageAccountKey};QueueEndpoint=https://{CloudLogicSharedFunctions.storageAccountName}.queue.core.windows.net/";
        private const string ProductDirectoryName = "Eviyos";
        private const string binShare = @"\\nas-pen1001-01\PIXLOG_2217$\BIN";
        private const string tempZipFilePath = @"C:\Temp\CustomerDataSharingMessages\ZipToUpload\";

        private static BlobContainerClient inboxContainerClient;
        private static BlobContainerClient InboxContainerClient
        {
            get
            {
                if (inboxContainerClient == null)
                {
                    var inputBlobServiceContainerClient = new BlobServiceClient(inboxStorageSettings);
                    inboxContainerClient = inputBlobServiceContainerClient.GetBlobContainerClient("inbox");
                }
                return inboxContainerClient;
            }
        }

        private static BlobContainerClient outputContainerClient;
        private static BlobContainerClient OutputContainerClient
        {
            get
            {
                if (outputContainerClient == null)
                {
                    var outputBlobServiceContainerClient = new BlobServiceClient(inboxStorageSettings);
                    outputContainerClient = outputBlobServiceContainerClient.GetBlobContainerClient("output");
                }
                return outputContainerClient;
            }
        }

        private static BlobContainerClient measurementContainerClient;
        private static BlobContainerClient MeasurementContainerClient
        {
            get
            {
                if (measurementContainerClient == null)
                {
                    var measurementBlobServiceContainerClient = new BlobServiceClient(inboxStorageSettings);
                    measurementContainerClient = measurementBlobServiceContainerClient.GetBlobContainerClient("measurements");
                }
                return measurementContainerClient;
            }
        }

        public static void Execute()
        {
            DataLakeDirectoryClient apiRequestDirectoryClient = CloudLogicSharedFunctions.InputFileSystemClient.GetDirectoryClient($"ApiRequests/");
            var apiRequests = apiRequestDirectoryClient.GetPaths(false);
            foreach (var apiRequest in apiRequests)
            {
                LogHelper.Info(typeof(DeliveryCreated), $"handle file {apiRequest.Name}");
                if (!apiRequest.Name.EndsWith(".json"))
                {
                    LogHelper.Info(typeof(DeliveryCreated), $"  skip as it is no json");
                    continue;
                }

                DataLakeFileClient apiRequestFileClient = apiRequestDirectoryClient.GetFileClient(apiRequest.Name.Substring(apiRequest.Name.IndexOf("/") + 1));
                var apiCall = CloudLogicSharedFunctions.ReadFromFileClient(apiRequestFileClient);
                var apiContent = System.Text.Json.JsonSerializer.Deserialize<Customer[]>(apiCall);
                LogHelper.Info(typeof(DeliveryCreated), $"  file contains {apiContent.Length} customers and in total {apiContent.Sum(e => e.ITEMS.Length)} entries");

                //remove leading zeros of multiple fields
                foreach(var customer in apiContent)
                {
                    customer.CUSTOMER_GROUP = customer.CUSTOMER_GROUP.TrimStart('0');
                    customer.CUSTOMER_ID = customer.CUSTOMER_ID.TrimStart('0');
                    customer.SALES_ORDER_NUMBER = customer.SALES_ORDER_NUMBER?.TrimStart('0');
                    foreach(var item in customer.ITEMS)
                    {
                        item.DELIVERY_NUMBER = item.DELIVERY_NUMBER.TrimStart('0');
                        item.DELIVERY_ITEM = item.DELIVERY_ITEM.TrimStart('0');
                        item.MATERIAL_NUMBER_11 = item.MATERIAL_NUMBER_11.TrimStart('0');
                        item.SALES_ORDER_ITEM = item.SALES_ORDER_ITEM?.TrimStart('0');
                    }
                    if (customer.CUSTOMER_ID == "505460") //Silicon Application Corp.
                        customer.CUSTOMER_GROUP = "7000323";
                    else if (customer.CUSTOMER_ID == "512797") //Silicon Application Corp.
                        customer.CUSTOMER_GROUP = "7000323";
                    else if (customer.CUSTOMER_ID == "515341") //ELECXUS Co., Ltd.
                        customer.CUSTOMER_GROUP = "7000265";
                    else if (customer.CUSTOMER_ID == "513226") //WORLD PEACE INDUSTRIAL CO., LTD.
                        customer.CUSTOMER_GROUP = "7101372";
                    else if (customer.CUSTOMER_ID == "515379") //Comtech International (Hong Kong) L
                        customer.CUSTOMER_GROUP = "7000725";
                    else if (customer.CUSTOMER_ID == "511591") //Avnet Asia Pte Ltd
                        customer.CUSTOMER_GROUP = "7000336";
                }

                foreach (var file in apiContent)
                {
                    string error = null;
                    var itemsToHandle = new List<ItemToHandle>();
                    var deliveryNumbers = file.ITEMS.Select(i => i.DELIVERY_NUMBER).Distinct().ToList();
                    if (!file.ITEMS.Any(i => i.MATERIAL_NUMBER_Q_TEXT.Replace(" ", "").StartsWith("KEWGB")))
                    {
                        var outputFileName = apiRequest.Name.Replace("ApiRequests/", "ApiRequests/Ignore/");
                        CopyFile(InboxContainerClient, apiRequest.Name, InboxContainerClient, outputFileName);
                        InboxContainerClient.DeleteBlob(apiRequest.Name);
                        error = "NotConfiguredYet";
                        LogHelper.Info(typeof(DeliveryCreated), $"  product is no eviyos product and was not configured yet -> skip");
                        goto nextFile;
                    }
                    foreach(var deliveryNumber in deliveryNumbers)
                    {
                        LogHelper.Info(typeof(DeliveryCreated), $"  handle purchase order {file.CUSTOMER_PO_NUMBER} of customer {file.CUSTOMER_NAME}, delivery {deliveryNumber}");
                        var items = file.ITEMS.Where(i => i.DELIVERY_NUMBER  == deliveryNumber).ToList();

                        //check if the same batch/lot does already exist in Cosmos db -> if yes skip
                        var existingData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.delivery_number='{deliveryNumber}'"); // where NOT IS_DEFINED(c.customer_group)
                        existingData.Wait();
                        if (existingData.Result.Count > 0)
                        {
                            var lotsInDelivery = file.ITEMS.Select(i => i.LOT_ID).Where(i => !String.IsNullOrEmpty(i)).Distinct().ToList();
                            var lotsOfDeliveryAlreadyInCosmos = existingData.Result.Select(r => r.lot_id).Distinct().Where(l => lotsInDelivery.Contains(l)).ToList();
                            error = "Data already transferred";
                            LogHelper.Info(typeof(DeliveryCreated), $"  files for delivery {deliveryNumber} alredy exst in cosmos db ({lotsOfDeliveryAlreadyInCosmos.Count} out of {lotsInDelivery.Count}) -> skip");
                            goto nextFile;
                        }

                        foreach (var item in items)
                        {
                            if (String.IsNullOrEmpty(item.LOT_ID)) //json entry does not contain a lot id -> just skip entry as this is normal
                            {
                                LogHelper.Debug(typeof(DeliveryCreated), $"  delivery {item.DELIVERY_NUMBER} contains an item without a lot (normal for the first entry)");
                                continue;
                            }
                            if (item.ShipmentDate < new DateTime(2000, 1, 1)) //no shipment date maintained
                            {
                                LogHelper.Error(typeof(DeliveryCreated), $"  shipment date of delivery {item.DELIVERY_NUMBER} not maintained");
                                goto nextFile;
                            }

                            var itemToHandle = new ItemToHandle()
                            {
                                Customer = file,
                                Item = item
                            };
                            var lotClient = CloudLogicSharedFunctions.OutputFileSystemClient.GetDirectoryClient($"{ProductDirectoryName}/{item.LOT_ID}/");

                            if (!lotClient.Exists())//directory with the bin files does not exist
                            {
                                string expectedLotDirName = item.LOT_ID;
                                if (!expectedLotDirName.Contains("."))//if the lot is not in the format HRF2700F.12 but HRF2700F12 -> add the dot
                                    expectedLotDirName = expectedLotDirName.Substring(0, 8) + "." + expectedLotDirName.Substring(8);
                                string expectedDirectoryName = Path.Combine(binShare, expectedLotDirName);
                                //check if lot directory exists in share and needs to be uploaded first -> if yes, zip the files and upload to the cloud storage. then delete on the share
                                if(Directory.Exists(expectedDirectoryName))
                                {
                                    if (!Directory.Exists(tempZipFilePath))
                                        Directory.CreateDirectory(tempZipFilePath);

                                    //copy the bin directory to local to have a better zip performance
                                    var zipBufferDirectory = Path.Combine(tempZipFilePath, item.LOT_ID);
                                    Directory.CreateDirectory(zipBufferDirectory);
                                    var sourceDirectory = new DirectoryInfo(expectedDirectoryName);
                                    foreach (var sourceFile in sourceDirectory.GetFiles())
                                    {
                                        var targetFilePath = Path.Combine(zipBufferDirectory, sourceFile.Name);
                                        sourceFile.CopyTo(targetFilePath);
                                    }

                                    var zipFilePath = Path.Combine(tempZipFilePath, item.LOT_ID + ".zip");
                                    ZipFile.CreateFromDirectory(zipBufferDirectory, zipFilePath);

                                    var inboxFileClient = InboxContainerClient.GetBlobClient($"{ProductDirectoryName}/{item.LOT_ID}.zip");
                                    using (FileStream fileStream = File.OpenRead(zipFilePath))
                                        inboxFileClient.Upload(fileStream, false);
                                    File.Delete(zipFilePath);

                                    Directory.Delete(zipBufferDirectory, true);

#if DEBUG
                                    //nothing to do -> on test runs the network dir should not be touched
#else
                                    Directory.Delete(expectedDirectoryName, true);
#endif
                                    int maxRetry = 15;
                                    while(maxRetry > 0) //sleep for few minutes to let the serverless function handle the zip file in the cloud
                                    {
                                        Thread.Sleep(60 * 1000);
                                        if (lotClient.Exists())
                                            break;
                                        maxRetry--;
                                    }
                                    FilesArrivedInCloudLogic.Execute();
                                }
                                if (!lotClient.Exists())
                                {
                                    error = "Data not available";
                                    LogHelper.Error(typeof(DeliveryCreated), $"  no files for lot {item.LOT_ID} found (delivery {item.DELIVERY_NUMBER})");
                                    goto nextFile;
                                }
                            }

                            var allFiles = lotClient.GetPaths(true).ToList();
                            itemToHandle.AllBinFiles = allFiles
                                .Select(p => p.Name)
                                .Where(p => p.EndsWith(".bin"))
                                .ToList();

                            var materialStart = item.MATERIAL_NUMBER_Q_TEXT?.Replace("KEW GB", "KEWGB").Substring(0, 10); //will be found in the bin file name
                            if (materialStart == "KEWGBBMD2U")
                                itemToHandle.ValidBinFiles = GetBinFilesWithRatio(item, itemToHandle.AllBinFiles);
                            else
                                itemToHandle.ValidBinFiles = itemToHandle.AllBinFiles.Where(f => f.ToUpper().Contains(item.LOT_ID.ToUpper() + "/" + materialStart)).ToList();

                            if (item.AmountInBatch != itemToHandle.ValidBinFiles.Count)
                            {
                                error = "Data not available";
                                LogHelper.Error(typeof(DeliveryCreated), $"  wrong amount of files compared with shipment: {item.AMOUNT_IN_BATCH} products shipped and files should be there, but only {itemToHandle.ValidBinFiles.Count} files are in the lot directory");
                                goto nextFile;
                            }

                            DataLakeFileClient binInformationFileClient = lotClient.GetFileClient("binInformation.txt");
                            var binFilesInformationSerialized = CloudLogicSharedFunctions.ReadFromFileClient(binInformationFileClient);
                            var binFilesInformation = System.Text.Json.JsonSerializer.Deserialize<BinFilesInformation>(binFilesInformationSerialized);
                            itemToHandle.BinInformation = binFilesInformation;

                            itemsToHandle.Add(itemToHandle);
                        }
                    }

                    //only if the complete file is valid we can handle - otherwise keep it for now
                    if (itemsToHandle.Count == 0)
                        continue;

                    LogHelper.Info(typeof(DeliveryCreated), $"  {itemsToHandle.Count} items found to be handled - check if cosmos entries do already exist");

                    var batchList = String.Join(",", itemsToHandle.Select(i => i.Item.BATCH_NUMBER).Distinct().Select(i => $"'{i}'"));
                    var cosmosDataTask = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.product <> 'IMSE5515' and c.batch_number in ({batchList})"); // where NOT IS_DEFINED(c.customer_group)
                    cosmosDataTask.Wait();
                    var cosmosData = cosmosDataTask.Result;

                    foreach (var itemToHandle in itemsToHandle)
                        HandleItem(itemToHandle, cosmosData);

                    nextFile:;

                    foreach (var deliveryNumber in deliveryNumbers)
                    {
                        var seeburgerPostObject = new SeeburgerResult()
                        {
                            CUSTOMER_GROUP = file.CUSTOMER_GROUP,
                            CUSTOMER_GROUP_NAME = file.CUSTOMER_GROUP_NAME,
                            CUSTOMER_ID = file.CUSTOMER_ID,
                            CUSTOMER_NAME = file.CUSTOMER_NAME,
                            CUSTOMER_PO_NUMBER = file.CUSTOMER_PO_NUMBER,
                            DELIVERY_NUMBER = deliveryNumber.TrimStart('0'),
                            IDOC = file.IDOC,
                            STATUS = "Success",
                            ITEMS = itemsToHandle.Select(i =>
                                new SeeburgerResult.SeeburgerResultItems()
                                {
                                    BATCH_NUMBER = i.Item.BATCH_NUMBER,
                                }).ToArray()
                        };

                        if (error != null)
                        {
                            seeburgerPostObject.ITEMS = new SeeburgerResult.SeeburgerResultItems[0];
                            seeburgerPostObject.STATUS = error;
                        }
                        var seeburgerPostContent = System.Text.Json.JsonSerializer.Serialize(seeburgerPostObject);

                        try
                        {
                            var api = MyApplicationSettings.GetSetting("SeeburgerBISUrl");
                            var seeburgerTask = CallPostApi(api, seeburgerPostContent);
                            seeburgerTask.Wait();
                        }
                        catch(Exception ex)
                        {
                            LogHelper.Error(typeof(DeliveryCreated), $"  communication to Seeburger BIS failed: {ex.Message}\r\n  delivery number:{deliveryNumber}, file: {apiRequest.Name}\r\n" + ex.StackTrace);
                        }
                    }

                    if(error == null) //no error -> file handled, archive file
                    {
                        var outputFileName = apiRequest.Name.Replace("ApiRequests/", "ApiRequests/Archive/");
                        CopyFile(InboxContainerClient, apiRequest.Name, InboxContainerClient, outputFileName);
                        InboxContainerClient.DeleteBlob(apiRequest.Name);
                    }
                }
            }
        }

        private static void HandleItem(ItemToHandle item, List<EviyosBox> cosmosData)
        {
            LogHelper.Info(typeof(DeliveryCreated), "  handle item " + item.Item.LOT_ID);
           
            //var customerGroupID = Int64.Parse(item.Customer.CUSTOMER_GROUP);

            var box = new EviyosBox()
            {
                id = Guid.NewGuid().ToString(),
                customer_id = item.Customer.CUSTOMER_ID,
                customer_name = item.Customer.CUSTOMER_NAME,
                customer_group = item.Customer.CUSTOMER_GROUP,
                purchase_order_number = item.Customer.CUSTOMER_PO_NUMBER, //purchaseOrderNumber,
                order_number = item.Customer.SALES_ORDER_NUMBER, //orderId
                order_date = DateTime.ParseExact("20200101", "yyyyMMdd", CultureInfo.InvariantCulture),//DateTime.ParseExact(item.Item.ORDER_DATE, "yyyyMMdd", CultureInfo.InvariantCulture),
                order_pos_no = item.Item.SALES_ORDER_ITEM,
                quantity_ordered = 0,// String.IsNullOrEmpty(item.Item.ORDERED_QUANTITY) ? 0 : Int32.Parse(item.Item.ORDERED_QUANTITY),
                material_number = item.Item.MATERIAL_NUMBER_Q,
                material_text = item.Item.MATERIAL_NUMBER_Q_TEXT,
                material_number_11 = item.Item.MATERIAL_NUMBER_11,
                material_text_11 = item.Item.MATERIAL_NUMBER_11_TEXT,
                delivery_number = item.Item.DELIVERY_NUMBER.ToString().TrimStart('0'),
                shipment_date = DateTime.ParseExact(item.Item.SHIPMENT_DATE, "yyyyMMdd", CultureInfo.InvariantCulture),
                batch_number = item.Item.BATCH_NUMBER,
                lot_id = item.Item.LOT_ID,
                box_id = item.Item.BOX_ID
            };

            box.purchase_order_number = box.purchase_order_number.Replace(" CHARGEABL", "");
            box.material_text = box.material_text.Replace("-Device", "");
            box.batch_quantity = item.Customer.ITEMS.Where(e => !String.IsNullOrEmpty(e.LOT_ID)).Select(e => e.BATCH_NUMBER).Distinct().Count(); //amount of different batches
            box.quantity_in_batch = item.ValidBinFiles.Count;
            box.products = new EviyosProduct[item.ValidBinFiles.Count];

            var filesToUpload = new List<StandardizedUploadHelper.FilesToUpload>();

            String errorList = String.Empty;
            int productCounter = 0;
            int counter = 1;
            var invalidPixelAmount = new List<String>();
            foreach (var binFile in item.AllBinFiles) //Eviyos/HRG4505C52/KEWGBBMD1U_3239_4_8_4_0T0MIS0103.bin
            {
                LogHelper.Info(typeof(DeliveryCreated), $"  handle file {binFile} ({counter++} of {item.AllBinFiles.Count})");

                //var fileInfo = new FileInfo(binFile);
                var fileParts = binFile.Split(new char[] { '/', '_', '.' }, StringSplitOptions.RemoveEmptyEntries);
                string material;
                int waferLot;
                int waferNumber;
                int posX;
                int posY;

                //if (fileParts.Length == 8 &&
                //        fileParts[2].StartsWith("K") && //material
                //        Int32.TryParse(fileParts[3], out waferLot) &&
                //        Int32.TryParse(fileParts[4], out waferNumber) &&
                //        Int32.TryParse(fileParts[5], out posX) &&
                //        Int32.TryParse(fileParts[6], out posY) &&
                //        fileParts[7]?.ToLower() == "bin")
                //{ }
                //else
                if (fileParts.Length == 9 &&
                        fileParts[2].StartsWith("K") && //material
                        Int32.TryParse(fileParts[3], out waferLot) &&
                        Int32.TryParse(fileParts[4], out waferNumber) &&
                        Int32.TryParse(fileParts[5], out posX) &&
                        Int32.TryParse(fileParts[6], out posY) &&
                        fileParts[8]?.ToLower() == "bin")
                { }
                else
                {
                    errorList += $"Error parsing the file name {binFile}\r\n";
                    continue;
                }
                string dmc = fileParts[7];

                string icc_id = $"{waferLot}_{waferNumber}_{posX}_{posY}";

                //only upload the files where the product is matching the ordered one
                if (item.ValidBinFiles.Contains(binFile))
                {
                    var binFileInformation = item.BinInformation.BinFiles.FirstOrDefault(f => binFile.EndsWith(f.BinFileName));
                    box.products[productCounter++] = new EviyosProduct()
                    {
                        ic_code = icc_id,
                        dmc = dmc,
                        version = binFileInformation?.BinFileVersion
                    };

                    LogHelper.Info(typeof(DeliveryCreated), $"    copy into target measurement storage");

                    var binName = $"{waferLot}/{waferNumber}/{icc_id}.bin";
                    CopyFile(OutputContainerClient, binFile, MeasurementContainerClient, binName);

                    if (binFileInformation.PermanentOffPixels > 125)
                        invalidPixelAmount.Add($"Detected: {binFileInformation.PermanentOffPixels}, Allowed: 125, File: {binFileInformation.BinFileName}, Lot: {item.Item.LOT_ID}");
                }
                LogHelper.Info(typeof(DeliveryCreated), $"    file copied successful");
            }

            if (errorList.Length > 0)
            {
                LogHelper.Error(typeof(DeliveryCreated), $"  error while uploading files to measurement container (lot {item.Item.LOT_ID}): \r\n" + errorList);
                return;
            }

            if (invalidPixelAmount.Count > 0)
            {
                var errorMailSubject = $"Eviyos web portal - invalid Permanent-OFF pixel count identified";
                var errorMailText = $"For an Eviyos bin file the maximum allowed amount of Permanent-OFF pixel was detected higher than allowed:" + Environment.NewLine +
                    String.Join(Environment.NewLine, invalidPixelAmount) + Environment.NewLine +
                    Environment.NewLine +
                    $"Product was sent to customer {item.Customer.CUSTOMER_NAME} with delivery {item.Item.DELIVERY_NUMBER}" + Environment.NewLine +
                    Environment.NewLine +
                    "Please note - this is a warning / sanity check.  The upload of this file took place already and it might be available for download by customer or transferred";

                var errorPixelMailReceiver = MyApplicationSettings.GetSetting("ErrorPixelMailReceiver").Split(';', ',').ToList();
                MailHelper.SendMail(errorPixelMailReceiver, errorMailSubject, errorMailText);
            }

            var existingCosmosEntry = cosmosData.FirstOrDefault(c => c.batch_number == item.Item.BATCH_NUMBER);
            //if only new files need to be uploaded -> skip the new cosmos db entry
            if (existingCosmosEntry == null)
            {
                LogHelper.Info(typeof(DeliveryCreated), "  cosmos entry does not exist yet - create");
                var cosmosTask = CosmosConnection.Default.Create(box, box.customer_id);
                cosmosTask.Wait();
                LogHelper.Info(typeof(DeliveryCreated), "  cosmos entry created");
            }
            else
            {
                LogHelper.Info(typeof(DeliveryCreated), "  skip cosmos entry creation as there is already an entry with this batch number");
            }

            LogHelper.Info(typeof(DeliveryCreated), $"  archive lot");
            var lotClient = CloudLogicSharedFunctions.OutputFileSystemClient.GetDirectoryClient($"{ProductDirectoryName}/{item.Item.LOT_ID}/");
            string destinationDirectory = $"{ProductDirectoryName}/Archive/{item.Item.LOT_ID}";
            var renameTask = lotClient.RenameAsync(destinationDirectory);
            renameTask.Wait();
            LogHelper.Info(typeof(DeliveryCreated), $"  lot processing finished");
        }

        private static void CopyFile(BlobContainerClient sourceClient, string sourceFile, BlobContainerClient targetClient, string targetFileName)
        {
            var sourceBlobClient = sourceClient.GetBlobClient(sourceFile);
            var targetBlobClient = targetClient.GetBlobClient(targetFileName);
            var exists = targetBlobClient.Exists();
            if (exists.Value) //target blob already exists -> rename old blob
            {
                int index = 1;
                int dotPosition = targetFileName.LastIndexOf(".");

                string newFileName = null;
                BlobClient targetBlobClient2 = null;
                while (exists.Value)
                {
                    newFileName = targetFileName.Substring(0, dotPosition) + "_" + (index++) + targetFileName.Substring(dotPosition);
                    targetBlobClient2 = MeasurementContainerClient.GetBlobClient(newFileName);
                    exists = targetBlobClient2.Exists();
                }
                //copy the existing blob to a new name and delete the old one (rename is not supported)

                var task = targetBlobClient2.StartCopyFromUriAsync(targetBlobClient.Uri);
                task.Wait();
                var delTask = targetBlobClient.DeleteAsync();
                delTask.Wait();
            }
            var copyTask = targetBlobClient.StartCopyFromUriAsync(sourceBlobClient.Uri);
            copyTask.Wait();
        }

        private static async Task CallPostApi(string url, string requestBody)
        {
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    var authenticationString = $"DE_CDS:osram123";
                    var base64EncodedAuthenticationString = Convert.ToBase64String(System.Text.ASCIIEncoding.ASCII.GetBytes(authenticationString));

                    // Serialize the request body to JSON
                    HttpContent content = new StringContent(requestBody, Encoding.UTF8, "application/json");

                    // Send the POST request
                    var requestMessage = new HttpRequestMessage(HttpMethod.Post, url);
                    requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Basic", base64EncodedAuthenticationString);
                    requestMessage.Content = content;

                    HttpResponseMessage response = await client.SendAsync(requestMessage);
                    string responseBody = await response.Content.ReadAsStringAsync();

                    // Check the response status
                    if (response.IsSuccessStatusCode)
                    {
                        Console.WriteLine("Response: " + responseBody);
                        LogHelper.Info(typeof(DeliveryCreated), $"Successfully called Seeburger BIS: {responseBody}");
                    }
                    else
                    {
                        LogHelper.Error(typeof(DeliveryCreated), $"Error on Seeburger BIS call: {responseBody}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception occurred: " + ex.Message);
                throw ex;
            }
        }

        private static int GetRatioFromFileName(String fileName)
        {
            var fileParts = fileName.Split(new char[] { '-', '_' });
            if (fileParts.Contains("03"))
                return 3;
            else if (fileParts.Contains("04"))
                return 4;
            else
                return -1;
        }

        private class ItemToHandle
        {
            public List<String> AllBinFiles { get; set; }
            public List<String> ValidBinFiles { get; set; }
            public Customer Customer { get; set; }
            public Customer.Batch Item { get; set; }
            public BinFilesInformation BinInformation { get; internal set; }
        }

        private static List<string> GetBinFilesWithRatio(Customer.Batch batch, List<string> allBinFiles)
        {
            var ratios = new List<String>();
            if (batch.MATERIAL_NUMBER_Q_TEXT.Replace(" ", "").StartsWith("KEWGBBMD2U"))
            {
                ratios.Add("04");
            }
            else
            {
                var sapMaterialParts = batch.MATERIAL_NUMBER_Q_TEXT.Split(new char[] { '-', '_' });
                ratios = sapMaterialParts.Where(p => p == "03" || p == "04").ToList();
            }
            if (ratios.Count != 1)
                throw new Exception($"different amount of ratios ({String.Join(",", ratios)}) were found for q material {batch.MATERIAL_NUMBER_Q_TEXT}");

            var orderRatio = ratios.First();

            var resultList = new List<string>();
            foreach (var fileName in allBinFiles)
            {
                var fileParts = fileName.Split(new char[] { '-', '_' });
                ratios = fileParts.Where(p => p == "03" || p == "04").ToList();
                if (ratios.Count == 1 && ratios[0] == orderRatio)
                    resultList.Add(fileName);
            }
            return resultList;
        }
    }

    public class SeeburgerResult
    {
        public string CUSTOMER_GROUP { get; set; }
        public string CUSTOMER_GROUP_NAME { get; set; }
        public string CUSTOMER_ID { get; set; }
        public string CUSTOMER_NAME { get; set; }
        public string CUSTOMER_PO_NUMBER { get; set; }
        public string IDOC { get; set; }
        public string STATUS { get; set; }
        public string DELIVERY_NUMBER { get; set; }
        public SeeburgerResultItems[] ITEMS { get; set; }

        public class SeeburgerResultItems
        {
            public string BATCH_NUMBER { get; set; }
        }
    }

    public class Customer
    {
        public string CUSTOMER_ID { get; set; }
        public string CUSTOMER_NAME { get; set; }
        public string CUSTOMER_GROUP { get; set; }
        public string CUSTOMER_GROUP_NAME { get; set; }
        public string PLANT { get; set; }
        public string CUSTOMER_PO_NUMBER { get; set; }
        public string SALES_ORDER_NUMBER { get; set; }
        public string IDOC { get; set; }
        public Batch[] ITEMS { get; set; }

        public class Batch
        {
            public string SALES_ORDER_ITEM { get; set; }
            public string ORDER_DATE { get; set; }
            public string ORDERED_QUANTITY { get; set; }
            public string MATERIAL_NUMBER_11 { get; set; }
            public string MATERIAL_NUMBER_11_TEXT { get; set; }
            public string MATERIAL_NUMBER_Q { get; set; }
            public string MATERIAL_NUMBER_Q_TEXT { get; set; }
            public string DELIVERY_NUMBER { get; set; }
            public string DELIVERY_ITEM { get; set; }
            public string SHIPMENT_DATE { get; set; }
            [JsonIgnore]
            public DateTime ShipmentDate { get { return String.IsNullOrEmpty(SHIPMENT_DATE) ? DateTime.MinValue : DateTime.ParseExact(SHIPMENT_DATE, "yyyyMMdd", CultureInfo.InvariantCulture); } }
            public string BATCH_NUMBER { get; set; }
            public string LOT_ID { get; set; }
            public string AMOUNT_IN_BATCH { get; set; }
            public double AmountInBatch { get { return double.Parse(AMOUNT_IN_BATCH, CultureInfo.InvariantCulture); } }
            public string BOX_ID { get; set; }
            public string SALES_UNIT { get; set; }
            public string CUSTOMER_MATERIAL_ENTERED { get; set; }
        }
    }
}