using DocumentFormat.OpenXml.Office2010.ExcelAc;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace CustomerDataSharingLogic.Eviyos
{
    public class UploadLotDir
    {
        private const string tempZipFilePath = @"C:\Temp\CustomerDataSharingMessages\ZipToUpload\";
        private const string binShare = @"\\nas-pen1001-01\PIXLOG_2217$\BIN";
        //private static List<string> lotsWithHighPrio = "HRH1002Y08,HRH1202006,HRH1102514,HRH1202212,HRH1202210,HRH1202R16,HRH1202R14,HRH1202407".Split(',').ToList();

        public static void Upload()
        {
            var directories = Directory.GetDirectories(binShare);
            var directoriesToUpload = directories.Select(d => new DirectoryInfo(d)).Where(d => d.Name.StartsWith("HRH") && d.Name.Length == 11 && d.CreationTime > new DateTime(2025, 03, 01)).Count();
            foreach (var directory in directories)
            {
                var dirInfo = new DirectoryInfo(directory);
                if (dirInfo.Name.StartsWith("HRH") && dirInfo.Name.Length == 11 && dirInfo.CreationTime > new DateTime(2025, 03, 01))
                {
                    Console.WriteLine("Handle " + directory);
                    if (!Directory.Exists(tempZipFilePath))
                        Directory.CreateDirectory(tempZipFilePath);

                    var lotId = dirInfo.Name.Replace(".", "");
                    var zipFilePath = Path.Combine(tempZipFilePath, lotId + ".zip");

                    //if (!lotsWithHighPrio.Contains(lotId))
                    //    continue;

                    if (File.Exists(zipFilePath))
                    {
                        Console.WriteLine("  already exist -> skip");
                        continue;
                    }

                    //copy the bin directory to local to have a better zip performance
                    var zipBufferDirectory = Path.Combine(tempZipFilePath, lotId);
                    
                    Directory.CreateDirectory(zipBufferDirectory);
                    int i = 0;
                    var allFiles = dirInfo.GetFiles();
                    foreach (var sourceFile in allFiles)
                    {
                        i++;
                        Console.WriteLine($"  file {i} out of {allFiles.Length}");

                        var targetFilePath = Path.Combine(zipBufferDirectory, sourceFile.Name);
                        sourceFile.CopyTo(targetFilePath);
                    }

                    ZipFile.CreateFromDirectory(zipBufferDirectory, zipFilePath);

                    //var inboxFileClient = InboxContainerClient.GetBlobClient($"{ProductDirectoryName}/{lotId}.zip");
                    //using (FileStream fileStream = File.OpenRead(zipFilePath))
                    //    inboxFileClient.Upload(fileStream, false);
                    //File.Delete(zipFilePath);

                    Directory.Delete(zipBufferDirectory, true);
                    Directory.Delete(directory, true);
                }
            }
        }
    }
}