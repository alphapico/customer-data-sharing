using CustomerDataSharingLogic.ExternalConnections.ADLS;
using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using CustomerDataSharingLogic.ExternalConnections.Snowflake;
using CustomerDataSharingLogic.Helpers;
using osram.OSAS.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace CustomerDataSharingLogic.Eviyos
{
    public class EviyosBusinessLogic : BaseBusinessLogic
    {

        public static List<String> materialsQStart = new List<String>
        {
            "KEW GBBMD2U", //new material 2.05 -> contains both ratios (3/1 and 4/1) and needs to be treated different 
            "KEW GBBMD1U",
            "KEW GBCLD1U"
        };

        private static Dictionary<String, String> materialOverwrites = new Dictionary<String, String>()
        {
            //customer group, material -> set fix value as there is a dummy material for both product types
            //{"7000001", "KEW GBCLD1U" }, //Marelli
            //{"7000687", "KEW GBCLD1U" }  //Hasco -> switched to 4:1
        };

        private const string logFile = @"filesHandled.txt";
        private static string configurationDirectory = MyApplicationSettings.GetSetting("ConfigurationDirectory");
        private static List<Log> logs;
        private static List<String> errorPixelMailReceiver;

        //private static void DeleteEntries()
        //{
        //    var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c"); // where NOT IS_DEFINED(c.customer_group)
        //    cosmosData.Wait();
        //    Console.WriteLine($"{cosmosData.Result.Count} items found in cosmos db");
        //    List<EviyosBox> data = cosmosData.Result;
        //    foreach (var e in data)
        //    {
        //        var delTask = CosmosConnection.Default.Delete<EviyosBox>(e.id, e.customer_id);
        //        delTask.Wait();
        //    }
        //}

        public class Log
        {
            public long CusotmerGroupID { get; set; }
            public long DeliveryNumber { get; set; }
            public long BatchNumber { get; set; }
            public DateTime TimeOfTransfer { get; set; }
        }

        public class ItemToHandle
        {
            public delegate List<String> GetRelevantFilesDelegate(String material, VW_DELIVERY_DATA dataEntry);

            public String Dir { get; set; }
            public bool DeleteDir { get; set; } = true;
            public List<string> AllBinFiles { get; set; }
            public GetRelevantFilesDelegate GetRelevantBinFiles { get; set; }
            public List<VW_DELIVERY_DATA> SapDataList { get; set; }
            public long BatchNumber { get; set; }
            public String LotId { get; set; }
        }

        public static void Execute()
        {
            //var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.customer_group = \"7000001\""); // where NOT IS_DEFINED(c.customer_group)
            //cosmosData.Wait();
            //Console.WriteLine($"{cosmosData.Result.Count} items found in cosmos db");
            //List<EviyosBox> data = cosmosData.Result;

            //return;
            LogHelper.Info(typeof(EviyosBusinessLogic), "starting application");
            errorPixelMailReceiver = MyApplicationSettings.GetSetting("ErrorPixelMailReceiver").Split(';', ',').ToList();

            SnowflakeDBConnection.EstablishConnection();
            //DtsDBConnection.EstablishConnection();

            logs = new List<Log>();
            if (File.Exists(logFile))
            {
                var fileContent = File.ReadAllText(logFile);
                logs = JsonSerializer.Deserialize<List<Log>>(fileContent);
            }

            var directoriesToCheck = Directory.GetDirectories(configurationDirectory).ToList();
            directoriesToCheck.RemoveAll(d => !d.StartsWith("5")); //logic is only valid for deliveryNumber_batchNumber -> ignore all other directories

            var hrDirectoryList = directoriesToCheck
                .Select(d => new FileInfo(d))
                .Where(d => d.Name.StartsWith("HR"))
                .Select(d => d.Name.Replace(".", ""))
                .ToList();
            var sapData = SnowflakeDBConnection.GetData($"select distinct * exclude customer_material_entered from \"DW\".\"EVIYOS\".\"VW_DELIVERY_DATA\" where " +
                $"lot_id in ('{String.Join("','", hrDirectoryList)}') and " +
                $"batch_number is not null and " +
                $"amount_in_batch > 0 and " +
                $"shipment_date is not null and " +
                $"customer_group is not null " +
                $"order by delivery_number, batch_number");
            var sapDataList = VW_DELIVERY_DATA.GetList(sapData);

            ////fake some data for testing
            //sapDataList[0].AMOUNT_IN_BATCH = 56 * 5;
            //sapDataList[0].MATERIAL_NUMBER_11 = "11137768";
            //sapDataList[0].MATERIAL_NUMBER_11_TEXT = "KEWGBBMD1UELES3E04E0____1.97_T05_";
            //sapDataList[0].MATERIAL_NUMBER_Q = "Q65113A9074";
            //sapDataList[0].MATERIAL_NUMBER_Q_TEXT = "KEWGBBMD1U-EMEQ-3E0-0-T05-HV";


            var preCachedDbValues = new Dictionary<string, List<VW_DELIVERY_DATA>>();
            foreach(var hrDirectory in hrDirectoryList)
                preCachedDbValues[hrDirectory] = sapDataList.Where(s => s.LOT_ID == hrDirectory).ToList();

            List<ItemToHandle> itemsToHandle = new List<ItemToHandle>();

            foreach (var dir in directoriesToCheck) // prod: \\nas-pen1001-01\PIXLOG_2217$\BIN
            {
                try
                {
                    //if (!dir.Contains("51607400_1015718689"))
                    //    continue;

                    LogHelper.Info(typeof(EviyosBusinessLogic), $"checking directory {dir}");
                    //sub dir name: <deliveryNum>_batchNum (e.g. 51577252_1014588077)

                    //var batchNumber = -1;
                    //var lotId = (string)null;

                    var dirToHandle = new DirectoryInfo(dir);
                    if (dirToHandle.Name.StartsWith("_"))
                    {
                        LogHelper.Info(typeof(EviyosBusinessLogic), $"  skip (starts with _ or does not contain _)");
                        continue;
                    }
                    else if (dirToHandle.Name.Contains("_") && (dirToHandle.Name.StartsWith("51") || dirToHandle.Name.StartsWith("50") || dirToHandle.Name.StartsWith("54"))) //DeliveryNumber_BatchNumber -> should be carved out as it is a manual process
                    {
                        var allBinFiles = Directory.GetFiles(dir).Where(f => f.EndsWith(".bin")).ToList();
                        var directoryValues = dirToHandle.Name.Split('_');
                        if (directoryValues.Length != 2)
                        {
                            LogHelper.Error(typeof(EviyosBusinessLogic), "  skip -> directory name doesn't match expectation: " + dir);
                            continue;
                        }
                        string deliveryStr = directoryValues[0];
                        string batchStr = directoryValues[1];

                        if (!Regex.IsMatch(deliveryStr, "^51\\d{6}$") && !Regex.IsMatch(deliveryStr, "^5(0|4)\\d{8}$"))
                        {
                            LogHelper.Error(typeof(EviyosBusinessLogic), "  delivery ID doesn't match expectation (start with 51 and continue with 6 additional digits or start with 50/54 with 8 additional digits): " + deliveryStr);
                            continue;
                        }

                        if (!Regex.IsMatch(batchStr, "^10\\d{8}$"))
                        {
                            LogHelper.Error(typeof(EviyosBusinessLogic), "batch ID doesn't match expectation (start with 10 and continue with 8 additional digits): " + batchStr);
                            continue;
                        }
                        //var deliveryNumber = Int64.Parse(deliveryStr);
                        List<string> relevantBinFilesDelegate(string materialStart, VW_DELIVERY_DATA dataEntry)
                        {
                            if (materialStart == "KEWGBBMD2U")
                                return GetBinFilesWithRatio(dataEntry, allBinFiles);
                            return allBinFiles.Where(f => f.ToUpper().StartsWith(dir.ToUpper() + "\\" + materialStart)).ToList();
                        }

                        sapData = SnowflakeDBConnection.GetData($"select distinct * exclude customer_material_entered from \"DW\".\"EVIYOS\".\"VW_DELIVERY_DATA\" where " +
                            $"delivery_number = '{deliveryStr}' and " +
                            $"batch_number is not null and " +
                            $"shipment_date is not null and " +
                            $"customer_group is not null and " +
                            $"amount_in_batch > 0 " +
                            $"order by delivery_number, batch_number");
                        sapDataList = VW_DELIVERY_DATA.GetList(sapData);

                        var item = new ItemToHandle()
                        {
                            Dir = dir,
                            AllBinFiles = allBinFiles,
                            SapDataList = sapDataList,
                            GetRelevantBinFiles = relevantBinFilesDelegate,
                            BatchNumber = Int64.Parse(batchStr)
                        };
                        itemsToHandle.Add(item);
                    }
                    else if (dir == configurationDirectory) { }
                    /*else if (false) //old logic, not in use anymore
                    {
                        //get deliveries where shipment date is within the last 7 days
                        //remove from this list the entries that have already been handled (log)
                        //get a list of files from dts
                        //if no files available -> skip
                        //handle files

                        var sapData = SnowflakeDBConnection.GetData($"select * from \"DW\".\"EVIYOS\".\"VW_DELIVERY_DATA\" where " +
                            $"shipment_date > dateadd(days, -10, CURRENT_DATE) and " +
                            $"batch_number is not null and " +
                            $"amount_in_batch > 0 and " +
                            $"material_number_q_text ilike any ({string.Join(",", materialsQStart.Select(m => "'" + m + "%'"))}) " +
                            $"order by delivery_number, batch_number");
                        var sapDataList = VW_DELIVERY_DATA.GetList(sapData);
                        List<String> allBinFiles = null;
                        var lotList = sapDataList.Select(i => i.LOT_ID).Where(l => !String.IsNullOrEmpty(l)).Distinct().ToList();

                        foreach (var lotId in lotList)
                        {
                            LogHelper.Debug(typeof(EviyosBusinessLogic), $"handle lot id {lotId}");

                            if (lotId.Length != 10)
                            {
                                LogHelper.Info(typeof(EviyosBusinessLogic), $"lot id {lotId} has a different length than 10");
                                continue;
                            }

                            var sapDataEntries = sapDataList.Where(s => s.LOT_ID == lotId).ToList();
                            var batchNumbers = sapDataEntries.Where(s => !String.IsNullOrEmpty(s.BATCH_NUMBER)).Select(s => s.BATCH_NUMBER).Distinct().ToList();
                            if (batchNumbers.Count != 1)
                            {
                                LogHelper.Error(typeof(EviyosBusinessLogic), $"lot id {lotId} contains a wrong amount of batch numbers from SAP. 1 expected, {batchNumbers.Count} found: " + String.Join(",", batchNumbers));
                                continue;
                            }
                            var batchNumber = batchNumbers.First();

                            if (logs.Any(l => l.BatchNumber.ToString() == batchNumber))
                            {
                                LogHelper.Info(typeof(EviyosBusinessLogic), $"batch number {batchNumber} was already processed");
                                continue;
                            }

                            var dtsData = DtsDBConnection.GetData($"select * from dts_admin.V_EVIYOS_WEBSITE where WLD_LOT_NAME='{lotId.Substring(0, 8)}.{lotId.Substring(8)}'");
                            var list = BaseDBHelper.GetList<V_EVIYOS_WEBSITE>(dtsData);
                            if (list.Count == 0)
                            {
                                LogHelper.Info(typeof(EviyosBusinessLogic), "no files in dts found for lot id " + lotId);
                                continue;
                            }

                            if (allBinFiles == null)
                                allBinFiles = Directory.GetFiles(configurationDirectory).Where(f => f.EndsWith(".bin")).ToList();

                            var allBinFilesOfBatch = allBinFiles.Where(f => list.Any(l => f.Contains($"_{l.WD_DEVICE_ID}.bin"))).ToList();
                            List<string> relevantBinFilesDelegate(string materialStart, VW_DELIVERY_DATA dataEntry)
                            {
                                return allBinFilesOfBatch.Where(f => f.ToUpper().StartsWith(configurationDirectory.ToUpper() + "\\" + materialStart)).ToList();
                            }

                            var item = new ItemToHandle()
                            {
                                Dir = configurationDirectory,
                                AllBinFiles = allBinFilesOfBatch,
                                SapDataList = sapDataList,
                                GetRelevantBinFiles = relevantBinFilesDelegate,
                                BatchNumber = Int64.Parse(batchNumber)
                            };
                            itemsToHandle.Add(item);
                        }
                    }
                    */
                    //lot handled different now (serverless & other logic)
                    /*else if (dir.Contains("\\HR")) //lot id directory
                    {
                        var allBinFiles = Directory.GetFiles(dir).Where(f => f.EndsWith(".bin")).ToList();
                        List<string> relevantBinFilesDelegate(string materialStart, VW_DELIVERY_DATA dataEntry)
                        {
                            if (materialStart == "KEWGBBMD2U")
                                return GetBinFilesWithRatio(dataEntry, allBinFiles);
                            return allBinFiles.Where(f => f.ToUpper().StartsWith(dir.ToUpper() + "\\" + materialStart)).ToList();
                        }

                        var lotDirName = dirToHandle.Name.Replace(".", "");

                        if (preCachedDbValues.ContainsKey(lotDirName))
                        {
                            sapDataList = preCachedDbValues[lotDirName];
                        }
                        else
                        {
                            sapData = SnowflakeDBConnection.GetData($"select distinct * exclude customer_material_entered from \"DW\".\"EVIYOS\".\"VW_DELIVERY_DATA\" where " +
                                $"lot_id = '{lotDirName}' and " +
                                $"batch_number is not null and " +
                                $"shipment_date is not null and " +
                                $"customer_group is not null and " +
                                $"amount_in_batch > 0 " +
                                $"order by delivery_number, batch_number");
                            sapDataList = VW_DELIVERY_DATA.GetList(sapData);
                        }

                        var item = new ItemToHandle()
                        {
                            Dir = dir,
                            AllBinFiles = allBinFiles,
                            SapDataList = sapDataList,
                            GetRelevantBinFiles = relevantBinFilesDelegate,
                            LotId = lotDirName
                        };
                        itemsToHandle.Add(item);
                    }*/
                    else
                    {
                        LogHelper.Info(typeof(EviyosBusinessLogic), $"not sure how to handle directory {dir} - skip");
                    }

                }
                catch (Exception ex)
                {
                    LogHelper.Error(typeof(EviyosBusinessLogic), $"Error happened: {ex.Message}\r\n{ex.StackTrace}");
                }
            }

            LogHelper.Info(typeof(EviyosBusinessLogic), $"{itemsToHandle} directories to handle");


            //          FAKE data for validation!!!
            {
                //itemsToHandle = new List<ItemToHandle>() { itemsToHandle.First() };

                //var sapData2 = SnowflakeDBConnection.GetData($"select * from \"DW\".\"EVIYOS\".\"VW_DELIVERY_DATA\" where " +
                //    $"lot_id = 'HRF1700586' and " +
                //    $"batch_number is not null and " +
                //    $"amount_in_batch > 0 " +
                //    $"order by delivery_number, batch_number");
                //var item = VW_DELIVERY_DATA.GetList(sapData2).First();
                //itemsToHandle.First().SapDataList = new List<VW_DELIVERY_DATA>() { item };
                //itemsToHandle.First().LotId = item.LOT_ID;
                //itemsToHandle.First().BatchNumber = Int64.Parse(item.BATCH_NUMBER);
                //item.MATERIAL_NUMBER_11_TEXT = item.MATERIAL_NUMBER_11_TEXT.Replace("KEWGBBMD1U", "KEWGBBMD2U");
                //item.MATERIAL_NUMBER_Q_TEXT = item.MATERIAL_NUMBER_Q_TEXT.Replace("KEWGBBMD1U", "KEWGBBMD2U");
            }
            //          FAKE data for validation done

            foreach (var itemToHandle in itemsToHandle)
                HandleItem(itemToHandle);

            LogHelper.Info(typeof(EviyosBusinessLogic), "Duplicate check");
            //handle duplicate check on DMC level for the items
            var cosmosEntriesWithDuplicates = EviyosDuplicateValidationCheck.EviyosGetValuesWithDuplicates();
            if (cosmosEntriesWithDuplicates.Count > 0)
            {
                foreach (var itemToHandle in itemsToHandle)
                {
                    var cosmosEntryWithDuplicates = cosmosEntriesWithDuplicates.Where(e => e.lot_id == itemToHandle.LotId).FirstOrDefault();
                    if (cosmosEntryWithDuplicates != null)
                        continue;

                    var entriesWithDuplicates = cosmosEntriesWithDuplicates.Where(e => e.products.Any(p => cosmosEntryWithDuplicates.products.Select(p2 => p2.ic_code).Contains(p.ic_code))).ToList();

                    LogHelper.Error(typeof(EviyosBusinessLogic), "Duplicated products found (multiple batch/lot entries with same IC_IDs):" + Environment.NewLine +
                        $"new imported: lot {itemToHandle.LotId}, batch {itemToHandle.BatchNumber}" + Environment.NewLine +
                        $"existing:" + Environment.NewLine +
                        String.Join(Environment.NewLine, entriesWithDuplicates.Select(e => $"lot {e.lot_id}, batch {e.batch_number}, sample products {String.Join(", ", e.products.Take(3).Select(p => p.ic_code + "/" + p.dmc))}")));
                }
            }
            
            LogHelper.Info(typeof(EviyosBusinessLogic), "Final upload of all files with AzCopy");
            try
            {
                UploadFilesToStorage();
            }
            catch (Exception ex)
            {
                throw new Exception("Files were not uploaded to storage! please check with high priority!", ex);
            }
        }

        private static List<string> GetBinFilesWithRatio(VW_DELIVERY_DATA dataEntry, List<string> allBinFiles)
        {
            var ratios = new List<String>();
            if (dataEntry.MATERIAL_NUMBER_Q_TEXT.StartsWith("KEWGBBMD2U"))
            {
                ratios.Add("04");
            }
            else
            {
                var sapMaterialParts = dataEntry.MATERIAL_NUMBER_Q_TEXT.Split(new char[] { '-', '_' });
                ratios = sapMaterialParts.Where(p => p == "03" || p == "04").ToList();
            }
            if (ratios.Count != 1)
                throw new Exception($"different amount of ratios ({String.Join(",", ratios)}) were found for q material {dataEntry.MATERIAL_NUMBER_Q_TEXT}");

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

        private static void HandleItem(ItemToHandle item)
        {
            LogHelper.Info(typeof(EviyosBusinessLogic), "handle directory " + item.Dir);

            var sapEntries = item.SapDataList.Where(s => (item.BatchNumber != -1 && s.BATCH_NUMBER == item.BatchNumber.ToString()) ||
                                                (!string.IsNullOrEmpty(item.LotId) && s.LOT_ID == item.LotId))
                                            .ToList();

            if (sapEntries.Count == 0)
            {
                LogHelper.Info(typeof(EviyosBusinessLogic), "  no rows were found in database for " + item.Dir);
                return;
            }
            else if (sapEntries.Count > 1)
            {
                LogHelper.Error(typeof(EviyosBusinessLogic), "  multiple rows were found in database for " + item.Dir);
                return;
            }
            if (sapEntries[0].SHIPMENT_DATE < new DateTime(2000, 1, 1)) //sometimes shipment date is only maintained later
            {
                LogHelper.Info(typeof(EviyosBusinessLogic), $"  shipment date is too old ({sapEntries[0].SHIPMENT_DATE}) -> indication that it was not maintained in SAP yet. Skip for now and try again later ({item.Dir})");
                return;
            }

            var sapEntry = sapEntries.First();
            var materialStart = sapEntry.MATERIAL_NUMBER_Q_TEXT?.Substring(0, 10); //will be found in the bin file name
                                                                                   //overwrite material for certain customers (e.g. Marelli receives a 3:1 product but in SAP there is no possibility to differentiate till now
            if (sapEntry.CUSTOMER_GROUP != null && materialOverwrites.ContainsKey(sapEntry.CUSTOMER_GROUP) && sapEntry.MATERIAL_NUMBER_Q == "Q65113A0090")
            {
                LogHelper.Info(typeof(EviyosBusinessLogic), $"  change material to {materialOverwrites[sapEntry.CUSTOMER_GROUP]}");
                materialStart = materialOverwrites[sapEntry.CUSTOMER_GROUP].Replace(" ", "");
                sapEntry.MATERIAL_NUMBER_Q_TEXT = materialOverwrites[sapEntry.CUSTOMER_GROUP];
            }
            var relevantBinFiles = item.GetRelevantBinFiles(materialStart, sapEntry);
            if (relevantBinFiles.Count == 0)
            {
                LogHelper.Info(typeof(EviyosBusinessLogic), "  no bin files were found in directory " + item.Dir + " with starting material " + materialStart);
                return;
            }

            if (sapEntry.AMOUNT_IN_BATCH != relevantBinFiles.Count)
            {
                LogHelper.Error(typeof(EviyosBusinessLogic), $"  invalid amount of bin files compared to the plan (SAP amount is {sapEntry.AMOUNT_IN_BATCH} but {relevantBinFiles.Count} files would be processed, batch {sapEntry.BATCH_NUMBER}, lot {sapEntry.LOT_ID})");
                return;
            }

            var customerGroupID = Int64.Parse(sapEntry.CUSTOMER_GROUP);

            ////check if folder is already handled completely
            var existingLog = logs.FirstOrDefault(l => l.CusotmerGroupID == customerGroupID && l.DeliveryNumber.ToString() == sapEntry.DELIVERY_NUMBER && (l.BatchNumber == 0 || l.BatchNumber.ToString() == sapEntry.BATCH_NUMBER));
            var maxCreationDate = relevantBinFiles.Max(f => File.GetCreationTimeUtc(f));
            //if (existingLog != null && maxCreationDate <= existingLog.TimeOfTransfer)
            //{
            //    LogHelper.Info(typeof(EviyosBusinessLogic), $"files are already uploaded (max file creation date: {maxCreationDate.ToString("yyyy-MM-dd HH:mm:ss")}, " +
            //        $"last upload date: {existingLog.TimeOfTransfer.ToString("yyyy-MM-dd HH:mm:ss")} -> skip {dir}");
            //    continue;
            //}

            EviyosBox box = StandardizedUploadHelper.FillEntryFromSAP<EviyosBox>(sapEntry);
            box.purchase_order_number = box.purchase_order_number.Replace(" CHARGEABL", "");
            box.material_text = box.material_text.Replace("-Device", "");
            box.batch_quantity = item.SapDataList.Select(e => e.BATCH_NUMBER).Distinct().Count(); //amount of different batches
            box.quantity_in_batch = relevantBinFiles.Count;
            box.products = new EviyosProduct[relevantBinFiles.Count];

            var filesToUpload = new List<StandardizedUploadHelper.FilesToUpload>();

            String errorList = String.Empty;
            int productCounter = 0;
            int counter = 1;
            foreach (var binFile in item.AllBinFiles)
            {
                LogHelper.Info(typeof(EviyosBusinessLogic), $"  handle file {binFile} ({counter++} of {item.AllBinFiles.Count})");

                var fileInfo = new FileInfo(binFile);
                var fileParts = fileInfo.Name.Split(new char[] { '_', '.' }, StringSplitOptions.RemoveEmptyEntries);
                string material;
                int waferLot;
                int waferNumber;
                int posX;
                int posY;

                if (fileParts.Length == 7 &&
                        fileParts[0].StartsWith("K") &&
                        Int32.TryParse(fileParts[1], out waferLot) &&
                        Int32.TryParse(fileParts[2], out waferNumber) &&
                        Int32.TryParse(fileParts[3], out posX) &&
                        Int32.TryParse(fileParts[4], out posY) &&
                        fileParts[6]?.ToLower() == "bin")
                { }
                else
                {
                    errorList += $"Error parsing the file name {fileInfo.Name}\r\n";
                    continue;
                }
                string dmc = fileParts[5];

                string icc_id = $"{waferLot}_{waferNumber}_{posX}_{posY}";

                var fileContent = File.ReadAllBytes(fileInfo.FullName);
                var lines = Encoding.UTF8.GetString(fileContent).Split(new string[] { Environment.NewLine, "\n" }, StringSplitOptions.RemoveEmptyEntries);

                //only upload the files where the product is matching the ordered one
                if (relevantBinFiles.Contains(binFile))
                {
                    var version = lines[0].Replace("Format Version:", "").Trim();
                    box.products[productCounter++] = new EviyosProduct()
                    {
                        ic_code = icc_id,
                        dmc = dmc,
                        version = version
                    };

                    LogHelper.Info(typeof(EviyosBusinessLogic), $"    upload to storage");

                    //var fileUploadTask = StorageConnection.Default.UploadFileContent($"{waferLot}/{waferNumber}/", $"{icc_id}.bin", fileContent, StorageConnection.WhenFileAlreadyExists.RenameOld);
                    //fileUploadTask.Wait();
                    var targetUploadPath = Path.Combine(UploadDirectory, waferLot.ToString(), waferNumber.ToString(), $"{icc_id}.bin");
                    filesToUpload.Add(new StandardizedUploadHelper.FilesToUploadSourceFile()
                    {
                        Content = fileContent,
                        SourcePath = fileInfo.FullName,
                        DeleteAfterUpload = true,
                        TargetPath = targetUploadPath
                    });
                    //WriteContentToFile(targetUploadPath, fileContent);

                    //check for invalid pixel amount and send error mail
                    var pixelOffLines = lines.Take(14).ToList();
                    PixelOffCheck(pixelOffLines, fileInfo.FullName, sapEntry);
                }
                LogHelper.Info(typeof(EviyosBusinessLogic), $"    upload to archive storage");

                if (fileParts[0].StartsWith("KEWGBBMD2U"))
                {
                    var ratio = GetRatioFromFileName(fileInfo.Name);
                    if (ratio != -1)
                        fileParts[0] = fileParts[0].Substring(0, 10) + "-" + ratio.ToString("00");
                }
                var targetUploadArchivePath = Path.Combine(UploadArchiveDirectory, fileParts[0], waferLot.ToString(), waferNumber.ToString(), fileInfo.Name);
                filesToUpload.Add(new StandardizedUploadHelper.FilesToUploadSourceFile()
                {
                    Content = fileContent,
                    SourcePath = fileInfo.FullName,
                    DeleteAfterUpload = true,
                    TargetPath = targetUploadArchivePath
                });
                //WriteContentToFile(targetUploadArchivePath, fileContent);
                //fileInfo.Delete();
                LogHelper.Info(typeof(EviyosBusinessLogic), $"    file processing finished");
            }
            if (errorList.Length > 0)
            {
                LogHelper.Error(typeof(EviyosBusinessLogic), "  error while uploading files to archive: \r\n" + errorList);
            }

            //if only new files need to be uploaded -> skip the new cosmos db entry
            if (existingLog == null)
            {
                StandardizedUploadHelper.Upload(box, filesToUpload);

                logs.Add(new Log()
                {
                    CusotmerGroupID = customerGroupID,
                    DeliveryNumber = Int64.Parse(sapEntry.DELIVERY_NUMBER),
                    BatchNumber = Int64.Parse(sapEntry.BATCH_NUMBER),
                    TimeOfTransfer = maxCreationDate
                });
                LogHelper.Info(typeof(EviyosBusinessLogic), "  cosmos entry created");
            }
            else
            {
                if(filesToUpload.Count > 0)
                    StandardizedUploadHelper.Upload((EviyosBox)null, filesToUpload);
                LogHelper.Info(typeof(EviyosBusinessLogic), "  skip cosmos entry creation");
            }

            var allFilesOfDir = Directory.GetFiles(item.Dir);
            if (allFilesOfDir.Length == 0 && item.DeleteDir)
            {
                LogHelper.Info(typeof(EviyosBusinessLogic), "  delete directory: " + item.Dir);
                Directory.Delete(item.Dir);
                LogHelper.Info(typeof(EviyosBusinessLogic), "  directory deleted");
            }

            File.WriteAllText(logFile, JsonSerializer.Serialize<List<Log>>(logs));

            UploadFilesToStorage();
        }

        //private static void PixelOffCheck(string binFile, VW_DELIVERY_DATA sapEntry)
        private static void PixelOffCheck(List<string> lines, string binFile, VW_DELIVERY_DATA sapEntry)
        {
            if (sapEntry == null)
            {
                LogHelper.Info(typeof(EviyosBusinessLogic), $"  PermanentOffPixels check could not been processed due to missing sap entry");
                return;
            }
            if (String.IsNullOrEmpty(sapEntry.LOT_ID))
            {
                LogHelper.Info(typeof(EviyosBusinessLogic), $"  PermanentOffPixels check could not been processed due to missing lot ID in SAP entry");
                return;
            }

            try
            {
                //read first 14 lines of file to find the attribute PermanentOffPixels
                //var lines = File.ReadLines(binFile).Take(14).ToList();
                if (lines.Count > 13 && lines[13].StartsWith("PermanentOffPixels")) //skip in case of an issue
                {
                    //read pixel off amount
                    var pixelOffAmountStr = lines[13].Replace("PermanentOffPixels:", "").Trim();
                    int pixelOffAmount = -1;
                    if (int.TryParse(pixelOffAmountStr, out pixelOffAmount))
                    {
                        //if lot ends with 9* values -> sample lot where higher defect pixels are allowed
                        var lotEnding = sapEntry.LOT_ID.Substring(sapEntry.LOT_ID.Length - 2);
                        var isSampleLot = lotEnding.StartsWith("9") || lotEnding.StartsWith("8");

                        //KEWGBCLD1U -> 3:1 product should have max. 19 error pixels
                        //KEWGBBMD1U -> 4:1 product should have max. 25 error pixels
                        int maxAllowedPixel = -1;
                        //if (!isSampleLot && sapEntry.MATERIAL_NUMBER_11_TEXT.StartsWith("KEWGBCLD1U"))
                        //    maxAllowedPixel = 19;
                        //else if (!isSampleLot && sapEntry.MATERIAL_NUMBER_11_TEXT.StartsWith("KEWGBBMD1U"))
                        //    maxAllowedPixel = 25;
                        //else if (isSampleLot && sapEntry.MATERIAL_NUMBER_11_TEXT.StartsWith("KEWGBCLD1U"))
                        //    maxAllowedPixel = 125;
                        //else if (isSampleLot && sapEntry.MATERIAL_NUMBER_11_TEXT.StartsWith("KEWGBBMD1U"))
                        //    maxAllowedPixel = 125;
                        //else
                        //{
                        //    LogHelper.Error(typeof(EviyosBusinessLogic), $"  PermanentOffPixels check failed -> material {sapEntry.MATERIAL_NUMBER_11_TEXT} is not known");
                        //}
                        //-> changed for all material to 100
                        maxAllowedPixel = 125;

                        if (maxAllowedPixel != -1 && maxAllowedPixel < pixelOffAmount)
                        {
                            var errorMailSubject = $"Eviyos web portal - invalid Permanent-OFF pixel count identified";
                            var errorMailText = $"For an Eviyos bin file the maximum allowed amount of Permanent-OFF pixel was detected higher than allowed:" +
                                $"Detected: {pixelOffAmount}" + Environment.NewLine +
                                $"Allowed: {maxAllowedPixel}" + Environment.NewLine +
                                $"File: {binFile}" + Environment.NewLine +
                                $"Customer: {sapEntry.CUSTOMER_NAME}" + Environment.NewLine +
                                $"Delivery: {sapEntry.DELIVERY_NUMBER}" + Environment.NewLine +
                                $"Batch: {sapEntry.BATCH_NUMBER}" + Environment.NewLine +
                                $"Lot: {sapEntry.LOT_ID}" + Environment.NewLine +
                                Environment.NewLine +
                                "Please note - this is a warning / sanity check.  The upload of this file took place already and it is available for download by customer";
                            MailHelper.SendMail(errorPixelMailReceiver, errorMailSubject, errorMailText);
                        }
                        else
                            LogHelper.Info(typeof(EviyosBusinessLogic), $"    PermanentOffPixels check successful: {maxAllowedPixel} pixel max. allowed, {pixelOffAmount} found");
                    }
                    else
                    {
                        LogHelper.Error(typeof(EviyosBusinessLogic), $"  parsing of pixel amount failed: {pixelOffAmount} could not be parsed as number) -> check bin file");
                    }
                }
            }
            catch (Exception ex)
            {
                LogHelper.Error(typeof(EviyosBusinessLogic), $"  unhandled error for the pixel off check: " + ex.Message + Environment.NewLine + ex.StackTrace);
            }
        }

        //was required as wrong files were uploaded to ADLS -> this function was used to replace them
        private static void ReplaceExistingFiles()
        {
            var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.customer_group = \"7000001\""); // where NOT IS_DEFINED(c.customer_group)
            cosmosData.Wait();
            Console.WriteLine($"{cosmosData.Result.Count} items found in cosmos db");
            List<EviyosBox> data = cosmosData.Result;
            foreach (var e in data)
            {
                //if already handled -> continue;
                if (e.material_text == "KEW GBCLD1U")
                    continue;

                foreach (var product in e.products)
                {
                    var storageDir = product.ic_code.Substring(0, product.ic_code.IndexOf("_", 6)).Replace("_", "/");

                    List<String> availableFiles = null;
                    try
                    {
                        var filesAvailableTask = StorageConnection.Backup.ListFilesInDirectory($"KEWGBCLD1U/{storageDir}/");
                        filesAvailableTask.Wait();
                        availableFiles = filesAvailableTask.Result;
                        //sample result file: KEWGBCLD1U/71741/7/KEWGBCLD1U_71741_7_10_5_0N6TF60402.bin
                    }
                    catch (Exception ex)
                    {
                        break;
                        //goto OuterLoopFinish;
                    }

                    var newFile = availableFiles.FirstOrDefault(f => f.StartsWith($"KEWGBCLD1U/{storageDir}/KEWGBCLD1U_{product.ic_code}_"));
                    if (newFile == null)
                        continue;

                    var newFileContentTask = StorageConnection.Backup.DownloadFileContent(newFile.Substring(0, newFile.LastIndexOf("/") + 1), newFile.Substring(newFile.LastIndexOf("/") + 1));
                    newFileContentTask.Wait();
                    var newFileContent = newFileContentTask.Result;

                    var oldFileContentTask = StorageConnection.Default.DownloadFileContent($"{storageDir}/", $"{product.ic_code}.bin");
                    oldFileContentTask.Wait();
                    var oldFileContent = oldFileContentTask.Result;

                    if (oldFileContent.SequenceEqual(newFileContent))
                        continue;

                    var fileUploadTask = StorageConnection.Default.UploadFileContent($"{storageDir}/", $"{product.ic_code}.bin", newFileContent, StorageConnection.WhenFileAlreadyExists.RenameOld);
                    fileUploadTask.Wait();
                }

                var delTask = CosmosConnection.Default.Delete<EviyosBox>(e.id, e.customer_id);
                delTask.Wait();

                e.material_text = "KEW GBCLD1U";

                var insertTask = CosmosConnection.Default.Create<EviyosBox>(e, e.customer_id);
                insertTask.Wait();
            OuterLoopFinish:
                continue;
            }
        }

        //check files in share and move the files that have been uploaded in the past to an archive directory
        private static void BinFromShareToArchive()
        {
            string share = @"\\nas-pen1001-01\PIXLOG_2217$\BIN";
            String archive = Path.Combine(share, "_Archive");
            foreach (var bin in Directory.GetFiles(share))
            {
                var fileInfo = new FileInfo(bin);
                var fileParts = fileInfo.Name.Split(new char[] { '_', '.' }, StringSplitOptions.RemoveEmptyEntries);
                string material;
                int waferLot;
                int waferNumber;
                int posX;
                int posY;

                if (fileParts.Length == 7 &&
                        fileParts[0].StartsWith("K") &&
                        Int32.TryParse(fileParts[1], out waferLot) &&
                        Int32.TryParse(fileParts[2], out waferNumber) &&
                        Int32.TryParse(fileParts[3], out posX) &&
                        Int32.TryParse(fileParts[4], out posY) &&
                        fileParts[6]?.ToLower() == "bin")
                { }
                else
                {
                    Console.WriteLine($"file {fileInfo.Name} could not be parsed -> skip");
                    continue;
                }
                string dmc = fileParts[5];

                try
                {
                    var archiveDirectory = $"{fileParts[0]}/{waferLot}/{waferNumber}/";
                    var filePropertiesTask = StorageConnection.Backup.GetFileProperties(archiveDirectory, fileInfo.Name);
                    filePropertiesTask.Wait();
                    if (filePropertiesTask.Result.ContentLength == fileInfo.Length) //file does exist and is uploaded -> delete original
                    {
                        string target = Path.Combine(archive, fileInfo.Name);
                        fileInfo.MoveTo(target);
                        Console.WriteLine($"File {fileInfo.Name} moved to {target}");
                    }
                }
                catch (Exception ex)
                {
                    if (ex.InnerException.Message.Contains("ErrorCode: BlobNotFound"))
                        Console.WriteLine($"File {fileInfo.Name} was not found in storage -> skip");
                    else
                        Console.WriteLine(ex.ToString());
                }
            }
        }
    }
}
