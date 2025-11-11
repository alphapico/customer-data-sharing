using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using CustomerDataSharingLogic.ExternalConnections.IMO;
using CustomerDataSharingLogic.ExternalConnections.Snowflake;
using CustomerDataSharingLogic.ExternalConnections.TMDB;
using CustomerDataSharingLogic.Helpers;
using osram.OSAS.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace CustomerDataSharingLogic.IMSE5515
{
    public class IMSE5515BusinessLogic : BaseBusinessLogic
    {
        private const string deliveryIdLogFile = @"IMSE5515FilesHandled.txt";
        private static string tempOutput = String.Empty;

        public static void Execute()
        {
            LogHelper.Info(typeof(IMSE5515BusinessLogic), $"start application");
            SnowflakeDBConnection.EstablishConnection();

            //optional filters (mainly for debugging)
            //List<String> deliverNumberFilter = null;// new List<string>() { "5002329273" };
            List<String> deliverNumberFilter =  new List<string>() { "51638412" };
            List<String> batchNumberFilter = null;// new List<string>() { "1015907337" };
            //DateTime? minShipmentDate = new DateTime(2025, 03, 09);
//            DateTime? minShipmentDate = new DateTime(2025, 03, 15);
            DateTime? minShipmentDate = new DateTime(2025, 04, 01);

            //get delivery information
            var deliveryData = VW_DELIVERY_DATA_INCL_IMO.GetDeliveries(deliverNumberFilter, batchNumberFilter, minShipmentDate);

            //only for following customers:
            //511158, EBV Elektronik GmbH = Webasto
            //512341, APAG Elektronik s.r.o.
            deliveryData.RemoveAll(d => d.CUSTOMER_ID != "511158" && d.CUSTOMER_ID != "512341");

            //var deliveryNumbers = deliveryData.Select(d => d.DELIVERY_NUMBER).Distinct().ToList();

            ////remove delivery data if needed for debugging
            //deliveryData.RemoveAll(d => d.BATCH_NUMBER != "1015838613");
            //deliveryData.RemoveAll(d => d.CUSTOMER_ID != "511158"); //webasto
            //deliveryData.RemoveAll(d => d.CUSTOMER_ID != "512341"); //apac
            ////remove all delivery data for now except one
            //for (int i = deliveryData.Count - 1; i > 0; i--)
            //    deliveryData.RemoveAt(i);

            //delivery recalled
            deliveryData.RemoveAll(d => d.DELIVERY_NUMBER == "5002270605");
            //test delivery
            deliveryData.RemoveAll(d => d.DELIVERY_NUMBER == "51638412");

            //load log to validate against already handled items
            var logs = new List<IMSE5515DeliveryLog>();
            if (File.Exists(deliveryIdLogFile))
            {
                var fileContent = File.ReadAllText(deliveryIdLogFile);
                logs = JsonSerializer.Deserialize<List<IMSE5515DeliveryLog>>(fileContent);
            }

            //loop through deliveries and check if they have already been handled (if yes -> remove)
            for (int i = deliveryData.Count - 1; i >= 0; i--)
            {
                var delivery = deliveryData[i];
                var deliveryLog = logs.FirstOrDefault(l =>
                                    l.DeliveryNumber == delivery.DELIVERY_NUMBER &&
                                    l.MaterialNumber == delivery.MATERIAL_NUMBER_11 &&
                                    l.BatchNumber == delivery.BATCH_NUMBER);
                if (deliveryLog != null) //already handled
                {
                    deliveryData.RemoveAt(i);
                    continue;
                }
            }

            if (deliveryData.Count == 0)//no data to handle
            {
                LogHelper.Info(typeof(IMSE5515BusinessLogic), $"nothing to handle");
                return;
            }

            String statusOutput = "";

            //loop deliveries
            var deliveryIds = deliveryData.Select(d => d.DELIVERY_NUMBER).Distinct().ToList();
            LogHelper.Info(typeof(IMSE5515BusinessLogic), $"{deliveryIds.Count} delivery IDs found to handle");
            foreach (var deliveryId in deliveryIds)
            {
                LogHelper.Info(typeof(IMSE5515BusinessLogic), $"handle {deliveryId}");
                try
                {
                    var deliveryItems = deliveryData.Where(d => d.DELIVERY_NUMBER == deliveryId).ToList();
                    bool validationSuccessful = true;
                    var successfulValidatedEntries = new List<SuccessfulValidatedEntry>();

                    //loop material
                    var materialList = deliveryItems.Select(m => m.MATERIAL_NUMBER_11).Distinct().ToList();
                    foreach (var material in materialList)
                    {
                        var deliveryItemsWithMaterial = deliveryItems.Where(d => d.MATERIAL_NUMBER_11 == material);
                        //loop batch numbers
                        var batchNumbers = deliveryItemsWithMaterial.Select(d => d.BATCH_NUMBER).Distinct().ToList();
                        var test = "'" + String.Join("', '", batchNumbers) + "'";
                        foreach (var batchNumber in batchNumbers)
                        {
                            LogHelper.Info(typeof(IMSE5515BusinessLogic), $"  batch number {batchNumber}");
                            var checkInCodes = deliveryItemsWithMaterial
                                                    .Where(d => d.BATCH_NUMBER == batchNumber)
                                                    .Select(d => d.HANDLING_UNIT_CONTENT_IMO_CHECK_CODE)
                                                    .Distinct()
                                                    .ToList();

                            var imoData = REP_T_VD_W_IMO.GetIMOData(material, batchNumber, checkInCodes);
                            if (imoData.Count == 0)
                            {
                                LogHelper.Info(typeof(IMSE5515BusinessLogic), $"  no IMO data found, skip");
                                continue;
                            }

                            foreach (var checkInCode in checkInCodes)
                            {
                                var deliveryItem = deliveryItemsWithMaterial
                                                    .FirstOrDefault(d => d.BATCH_NUMBER == batchNumber && d.HANDLING_UNIT_CONTENT_IMO_CHECK_CODE == checkInCode);

                                var imoItem = imoData.FirstOrDefault(d => d.WI_OUT_CHECK_CODE == checkInCode || d.WI_MID_CHECK_CODE == checkInCode);
                                if (imoItem == null)
                                {
                                    LogHelper.Error(typeof(IMSE5515BusinessLogic), $"IMO entry with WI_OUT_CHECK_CODE or WI_MID_CHECK_CODE equal {checkInCode} could not be found");
                                    validationSuccessful = false;
                                    continue;
                                }

                                //string reelId = deliveryItem.BATCH_NUMBER + imoItem.WI_OUT_CHECK_CODE;
                                string reelId = deliveryItem.BATCH_NUMBER + imoItem.WI_IN_CHECK_CODE;
                                LogHelper.Info(typeof(IMSE5515BusinessLogic), $"  handle reel id {reelId}");

                                if(reelId.IsContainedIn("1015995272001", "1015998741003", "1015998750001", "1016005598001"))
                                {
                                    LogHelper.Info(typeof(IMSE5515BusinessLogic), $"  ignore these reels as they have been recalled from customer");
                                    continue;
                                }

                                var allMeasurementData = REP_D_VJ_REEL_DATALOG.GetTmdbMeasurements(reelId);
                                if (reelId == "1016282896012") //reel 13 from production was renamed to 12 later. Take values of 13 from TMDB
                                    allMeasurementData = REP_D_VJ_REEL_DATALOG.GetTmdbMeasurements("1016282896013");
                                var measurementData = allMeasurementData.Where(m => m.OPERATION_NUMBER == 8444).ToList();

                                if (measurementData.Count == 0)
                                    measurementData = allMeasurementData;

                                if (measurementData.Count == 0)
                                {
                                    statusOutput += deliveryId + "\t" + reelId + "\tno measurement data found" + Environment.NewLine;
                                    LogHelper.Info(typeof(IMSE5515BusinessLogic), "  no measurement data found for reel " + reelId);
                                    validationSuccessful = false;
                                    continue;
                                }

                                var reelData = new REEL_DATA
                                {
                                    CalendarWeek = measurementData.First().LOT_CALWEEK,
                                    Delivery_ID = deliveryItem.DELIVERY_NUMBER,
                                    Handling_Unit = deliveryItem.HANDLING_UNIT_ID,
                                    Reel_Label = deliveryItem.BATCH_NUMBER + imoItem.WI_IN_CHECK_CODE,
                                    Dry_Pack_Label = deliveryItem.BATCH_NUMBER + imoItem.WI_MID_CHECK_CODE,
                                    Product_Box_Label = deliveryItem.BATCH_NUMBER + (imoItem.WI_OUT_CHECK_CODE ?? imoItem.WI_MID_CHECK_CODE)
                                };

                                //skip for now as there is an invalid number of DMC on the dummy reel
                                if (!IMSE5515DataValidation.ValidateData(measurementData, allMeasurementData, out List<string> errors))
                                {
                                    if(errors.Count > 20)
                                    {
                                        int amount = errors.Count;
                                        errors = errors.GetRange(0, 20);
                                        errors.Add($"more ... ({amount} in total)");
                                    }
                                    statusOutput += deliveryId + "\t" + reelId + "\t" + String.Join(";", errors) + Environment.NewLine;
                                    validationSuccessful = false;

                                    LogHelper.Info(typeof(IMSE5515BusinessLogic), $"  validation failed: {String.Join(";", errors)}");
                                    continue;
                                }
                                statusOutput += deliveryId + "\t" + reelId + "\tok" + Environment.NewLine;

                                string csvContent = null;
                                string fileName = null;
                                if (deliveryItems.First().CUSTOMER_ID == "511158") //webasto - new approach
                                {
                                    csvContent = IMSE5515CsvHelper.CreateCsvFile(reelData, measurementData);
                                    fileName = $"{deliveryItem.MATERIAL_NUMBER_11}_{reelData.CalendarWeek}_{imoItem.WI_BATCH_NUM}_{imoItem.WI_IN_CHECK_CODE}_{imoItem.WI_OUT_CHECK_CODE ?? imoItem.WI_MID_CHECK_CODE}_v2.CSV";
                                }
                                else
                                {
                                    csvContent = IMSE5515CsvHelper.CreateOldCsvFile(reelData, measurementData);
                                    fileName = $"{deliveryItem.MATERIAL_NUMBER_11}_{reelData.CalendarWeek}_{imoItem.WI_BATCH_NUM}_{imoItem.WI_IN_CHECK_CODE}_{imoItem.WI_OUT_CHECK_CODE ?? imoItem.WI_MID_CHECK_CODE}.CSV";
                                }
                                //update content to ADLS
                                //var fileUploadTask = StorageConnection.Default.UploadFileContent($"reel/{deliveryItem.BATCH_NUMBER.Substring(0, 5)}/{deliveryItem.BATCH_NUMBER}/", $"{reelId}.csv", csvBytes, StorageConnection.WhenFileAlreadyExists.RenameOld);
                                //fileUploadTask.Wait();
                                var box = StandardizedUploadHelper.FillEntryFromSAP<IMSE5515Box>(deliveryItem);
                                box.products = measurementData.Select(m => new IMSE5515Product() { dmc = m.DMC }).ToArray();
                                box.customer_group = "-" + box.customer_group;

                                successfulValidatedEntries.Add(new SuccessfulValidatedEntry()
                                {
                                    CsvContent = csvContent,
                                    Box = box,
                                    DeliveryItem = deliveryItem,
                                    ImoItem = imoItem,
                                    FileName = fileName
                                });
                                tempOutput += batchNumber + "," + Environment.NewLine;
                            }
                        }
                    }

                    if (validationSuccessful)
                    {
                        //String outputDir = @"C:\Temp\5515\_extendedFilesWebasto\5002359814\";
                        //foreach (var successfulValidatedEntry in successfulValidatedEntries)
                        //{
                        //    string outputFileName = Path.Combine(outputDir, successfulValidatedEntry.FileName);
                        //    File.WriteAllBytes(outputFileName, Encoding.UTF8.GetBytes(successfulValidatedEntry.CsvContent));
                        //    LogHelper.Info(typeof(IMSE5515BusinessLogic), $"  File {outputFileName} created");
                        //}

                        LogHelper.Info(typeof(IMSE5515BusinessLogic), $"  everything successful -> send mails");
                        int reelAmount = statusOutput.Split(new string[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries).Count();
                        var mh = new MailHelper()
                        {
                            Subject = "IMSE5515: data found for delivery " + deliveryId,
                            Body = $"<html>There have been new data found for delivery {deliveryId} (total {reelAmount} reels).<br />" +
                                statusOutput.Replace(Environment.NewLine, "<br />") +
                                $"</html>",
                            From = "serviceinfo@osram.com",
                            ReceiverStr = "V.Schmidts@osram.com",
                            IsBodyHtml = true
                        };
                        mh.Send();

                        foreach (var successfulValidatedEntry in successfulValidatedEntries)
                        {
                            LogHelper.Info(typeof(IMSE5515BusinessLogic), $"  send mail {(successfulValidatedEntries.IndexOf(successfulValidatedEntry) + 1)} out of {successfulValidatedEntries.Count}");
                            var targetUploadPath = Path.Combine(UploadDirectory, "IMSE5515", successfulValidatedEntry.FileName);
                            var csvBytes = Encoding.UTF8.GetBytes(successfulValidatedEntry.CsvContent);
                            var filesToUpload = new List<StandardizedUploadHelper.FilesToUpload>()
                                {
                                    new StandardizedUploadHelper.FilesToUploadBytes()
                                    {
                                        Content = csvBytes,
                                        TargetPath = targetUploadPath
                                    }
                                };

                            StandardizedUploadHelper.Upload(successfulValidatedEntry.Box, filesToUpload);

                            //send mail to customer (for now only to Volker)
                            IMSE5515EmailCreation.CreateMail(successfulValidatedEntry.CsvContent, successfulValidatedEntry.DeliveryItem, successfulValidatedEntry.ImoItem, successfulValidatedEntry.FileName);

                            //add the reel to the log file that it will not be handled again
                            var log = new IMSE5515DeliveryLog()
                            {
                                CusotmerID = successfulValidatedEntry.DeliveryItem.CUSTOMER_ID,
                                DeliveryNumber = successfulValidatedEntry.DeliveryItem.DELIVERY_NUMBER,
                                MaterialNumber = successfulValidatedEntry.DeliveryItem.MATERIAL_NUMBER_11,
                                BatchNumber = successfulValidatedEntry.DeliveryItem.BATCH_NUMBER,
                                ImoCheckinCode = successfulValidatedEntry.DeliveryItem.HANDLING_UNIT_CONTENT_IMO_CHECK_CODE,
                                TimeOfEmailSent = DateTime.Now
                            };
                            logs.Add(log);

                            File.WriteAllText(deliveryIdLogFile, JsonSerializer.Serialize<List<IMSE5515DeliveryLog>>(logs));
                        }
                    }
                    else
                    {
                        LogHelper.Error(typeof(IMSE5515BusinessLogic), "issue with delivery " + deliveryId + Environment.NewLine + statusOutput);

                        string htmlOutput = $"<table><tr><td>{statusOutput.Replace(Environment.NewLine, "</td></tr><tr><td>").Replace("\t", "</td><td>")}</td></tr></table>";

                        var mh = new MailHelper()
                        {
                            Subject = "IMSE5515: data issues found for delivery " + deliveryId,
                            Body = $"<html>There have been data issues found for delivery {deliveryId}. Please check with high priority as the products have already been shipped to the customer." +
                                htmlOutput +
                                $"</html>",
                            From = "serviceinfo@osram.com",
                            //ReceiverStr = "DL-OSWUXSCMVKL@osram-os.com;DL-OS SCM SP ME WUX2@osram-os.com;DL-OOSWUXQMPQE@osram-os.com;Yu.Jin@ams-osram.com;JinLong.Yu@ams-osram.com;DongXu.Qin@ams-osram.com;QingQing.Wu@ams-osram.com;XiaoDong.Wang@ams-osram.com",
                            //BccStr = "V.Schmidts@osram.com"  ReceiverStr = "DL-OSWUXSCMVKL@osram-os.com;DL-OS SCM SP ME WUX2@osram-os.com;DL-OOSWUXQMPQE@osram-os.com;Yu.Jin@ams-osram.com;JinLong.Yu@ams-osram.com;DongXu.Qin@ams-osram.com;QingQing.Wu@ams-osram.com;XiaoDong.Wang@ams-osram.com",
                            ReceiverStr = "V.Schmidts@osram.com",
                            IsBodyHtml = true,
                        };
                        mh.Send();
                    }
                    statusOutput = String.Empty;

                    LogHelper.Info(typeof(IMSE5515BusinessLogic), $"  upload to storage");
                    UploadFilesToStorage();
                }
                catch (Exception ex)
                {
                    LogHelper.Error(typeof(IMSE5515BusinessLogic), $"Error handling delivery {deliveryId}: {ex.Message}", ex);
                }
            }

            Console.WriteLine(statusOutput);


            //var handlingUnitsStatement = $"select * from \"DW\".\"EVIYOS\".\"VW_HANDLING_UNIT\" " +
            //    $"where SALES_DOCUMENT_ID_PACKING_OBJECT in ({string.Join(",", deliveryData.Select(d => $"'{d.DELIVERY_NUMBER}%'"))})";
            //var handlingUnitsTable = SnowflakeDBConnection.GetData(handlingUnitsStatement);
            //var handlingUnits = VW_HANDLING_UNIT.GetList(handlingUnitsTable);




            //get customer email list from Snowflake
            //loop through deliveries
            //  get reel ID and additional reel data from IMO
            //  loop through reels
            //      get dmc numbers from TMDB by reel
            //      get measurement data from TMDB by dmc list
            //      create csv file with measurement data
            //      store csv file to adls
            //      store meta data to cosmos db
            //      send csv file to customer
            //      add delivery incl. IMO mapping to logs

            ////GetIMOReelData();
            //var content = GetTmdbMeasurements("TJF4603P", reelData);
            ////CreateMail(content, deliveryData2);
        }

        internal class SuccessfulValidatedEntry
        {
            public string CsvContent { get; set; }
            public IMSE5515Box Box { get; set; }
            //public IMSE5515DeliveryLog Log { get; set; }
            public VW_DELIVERY_DATA_INCL_IMO DeliveryItem { get; set; }
            public REP_T_VD_W_IMO ImoItem { get; set; }
            public string FileName { get; set; }
        }
    }
}