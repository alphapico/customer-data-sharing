using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using CustomerDataSharingLogic.ExternalConnections.Snowflake;
using CustomerDataSharingLogic.ExternalConnections.TMDB;
using CustomerDataSharingLogic.Helpers;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace CustomerDataSharingLogic.Gaudi
{
    public class GaudiBusinessLogic : BaseBusinessLogic
    {
        private const string logFile = @"GaudiFilesHandled.txt";

        public static void Execute()
        {
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

            //pre-configure databases
            SnowflakeDBConnection.EstablishConnection();
            TmdbDBConnection.Location = "PEN";

            SnowflakeDBConnection.Execute("ALTER EXTERNAL TABLE DW.EVIYOS.OPULENT_DATA_EXT REFRESH;");

            var logs = new List<Log>();
            if (File.Exists(logFile))
            {
                var fileContent = File.ReadAllText(logFile);
                logs = JsonSerializer.Deserialize<List<Log>>(fileContent);
            }

            var opulentFiles = VW_OPULENT_DATA.GetFiles();
            //var maxFileDate = opulentFiles.Max(f => f.METADATA_FILENAME_DATE);
            //var relevantFiles = opulentFiles.Where(o => o.METADATA_FILENAME_DATE == maxFileDate).Select(f => f.METADATA_FILENAME).ToList();
            var relevantFiles = opulentFiles.Select(f => f.METADATA_FILENAME).Where(f => !logs.Any(l => l.FileName == f)).ToList();
            relevantFiles = relevantFiles.Where(r => r.Contains("20241227")).ToList();

            var itemsToHandle = VW_OPULENT_DATA.GetData(relevantFiles);
            foreach (var opulentFileName in relevantFiles)
            {
                var products = itemsToHandle.Where(i => i.METADATA_FILENAME == opulentFileName).ToList();
                var dmcList = products.Select(p => p.U_BARCODE).Distinct().ToList();
                var measurementData = GetMeasurementData(dmcList);
                measurementData.ForEach(m => m.SERIAL_NUMBER = itemsToHandle.FirstOrDefault(i => i.U_BARCODE == m.DEVICE_DMC).SERIAL_NUMBER);
                var fileContent = GaudiExportHelper.CreateCsvFileContent(opulentFileName, measurementData, out string newFileName);


                var targetUploadPath = Path.Combine(UploadDirectory, "Gaudi", newFileName + ".csv");
                var files = new List<StandardizedUploadHelper.FilesToUpload>()
                {
                    new StandardizedUploadHelper.FilesToUploadBytes()
                    {
                        TargetPath = targetUploadPath,
                        Content = fileContent
                    }
                };

                StandardizedUploadHelper.Upload<DefaultCosmosEntry>(null, files);

                logs.Add(new Log()
                {
                    FileName = opulentFileName,
                    TimeOfTransfer = DateTime.Now
                });
                File.WriteAllText(logFile, JsonSerializer.Serialize<List<Log>>(logs));
            }

            UploadFilesToStorage();

            if(relevantFiles.Count > 0)
            {

#if DEBUG
                var mailReceiver = "V.Schmidts@osram.com";
#else
                var mailReceiver = MyApplicationSettings.GetSetting("GaudiFileCreatedMailReceiver");
#endif

                var mailSubject = $"Gaudi - new files created";
                var mailContent = $"<html><body>There are new files created and will be available here soon:<br />" +
                    "<a href=\"https://osram.sharepoint.com/sites/GaudiQ/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FGaudiQ%2FShared%20Documents%2F05%20Logistics%2F04%20Customer%20Test%20Datasheet%2F03%20Customer%20Datasheet%2F02%20DV&viewid=735e4640%2D3118%2D4727%2D8354%2D5d6cfaa995d2\">GaudiQ Sharepoint</a>" +
                    "</body></html>";
                var mh = new MailHelper()
                {
                    ReceiverStr = mailReceiver,
                    Subject = mailSubject,
                    Body = mailContent,
                    From = "CustomerDataSharing@osram.com",
                    IsBodyHtml = true
                };
                mh.Send();
            }
        }

        private static List<V_TMDB_FT_OSBE18_TEST> GetMeasurementData(List<string> dmcList)
        {
            //steps:
            //check if dmc already contained in TMDB_DATA.RPA_FT_OSBE18_DMC
            var alreadyInDMCTable = RPA_FT_OSBE18_DMC.GetExistingDMC(dmcList);
            //if not -> add them
            RPA_FT_OSBE18_DMC.CreateDmcNotExistingYet(alreadyInDMCTable, dmcList);
            //query the measurement data from TMDB_DATA.V_TMDB_FT_OSBE18_TEST
            var result = V_TMDB_FT_OSBE18_TEST.GetMeasurements(dmcList);
            //remove the added dmcs from TMDB_DATA.RPA_FT_OSBE18_DMC
            RPA_FT_OSBE18_DMC.DeleteDMC(alreadyInDMCTable, dmcList);
            return result;
        }

        public class Log
        {
            public String FileName { get; set; }
            public DateTime TimeOfTransfer { get; set; }
        }
    }
}
