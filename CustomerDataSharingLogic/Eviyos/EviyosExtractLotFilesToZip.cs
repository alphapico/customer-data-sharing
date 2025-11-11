using CustomerDataSharingLogic.ExternalConnections.ADLS;
using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.IO;
using System.Linq;

namespace CustomerDataSharingLogic.Eviyos
{
    public class EviyosExtractLotFilesToZip
    {
        private static string lotList = @"HRH0403P21
HRH0500K25
HRH0500X25
HRH0501129
HRH0400409
HRH0700310";
        private static string outputDirectory = @"C:\Temp\Eviyos";

        public static void Extract()
        {
            var lots = lotList.Split(new string[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries).ToList();
            lots.ForEach(l => ExportLot(l));
        }

        private static void ExportLot(string lotId)
        {
            Console.WriteLine($"handle lot {lotId}");

            var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.lot_id = '{lotId}'");
            cosmosData.Wait();
            Console.WriteLine($"  {cosmosData.Result.Count} items found in cosmos db");
            List<EviyosBox> data = cosmosData.Result;

            var zipFile = Path.Combine(outputDirectory, lotId + ".zip");
            if(File.Exists(zipFile)) 
                File.Delete(zipFile);

            using (FileStream zipToOpen = new FileStream(zipFile, FileMode.CreateNew))
            {
                using (ZipArchive archive = new ZipArchive(zipToOpen, ZipArchiveMode.Create))
                {

                    foreach (var e in data)
                    {
                        foreach (var product in e.products)
                        {
                            var icCode = product.ic_code;
                            var icCodeParts = icCode.Split('_');
                            var path = $"{icCodeParts[0]}/{icCodeParts[1]}/{icCode}.bin";

                            Console.WriteLine($"  handle file {path} -> download");
                            var storageDir = product.ic_code.Substring(0, product.ic_code.IndexOf("_", 6)).Replace("_", "/");
                            var fileContentTask = StorageConnection.Default.DownloadFileContent($"{storageDir}/", $"{product.ic_code}.bin");
                            fileContentTask.Wait();
                            var content = fileContentTask.Result;

                            string prefix = "KEWGBCLD1U";
                            if (e.delivery_number == "5400781830")
                                prefix = "KEWGBCLD1U";
                            if (e.delivery_number == "5002371983")
                                prefix = "KEWGBBMD1U";
                            if (e.delivery_number == "51633556")
                                prefix = "KEWGBBMD2U";
                            if (e.delivery_number == "51633421")
                                prefix = "KEWGBBMD2U";
                            if (e.delivery_number == "5400782293")
                                prefix = "KEWGBCLD1U";
                            if (e.delivery_number == "5400781831")
                                prefix = "KEWGBBMD1U";
                            if (e.delivery_number == "5400781674")
                                prefix = "KEWGBBMD1U";
                            ZipArchiveEntry binEntry = archive.CreateEntry($"{prefix}_{product.ic_code}_{product.dmc}.bin");
                            using (var binEntryStream = binEntry.Open())
                            {
                                var memoryStream = new MemoryStream(content);
                                memoryStream.CopyTo(binEntryStream);
                            }

                            Console.WriteLine($"  handle file {path} finished");
                        }
                    }
                }
            }
        }
    }
}