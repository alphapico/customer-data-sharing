using CustomerDataSharingLogic.Helpers;
using osram.OSAS.Logging;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace CustomerDataSharingLogic.ExternalConnections.ADLS
{
    public class AzCopyHelper
    {
        private static string azCopyPath = MyApplicationSettings.GetSetting("AzCopyDirectory");

        public delegate void FileSkippedDelegate(string filePath);

        private const String uploadParametersBlob = "copy \"{localFilePath}\" " +
            "\"https://{storageAccount}.blob.core.windows.net/{target}{sas}\" " +
            "--from-to=LocalBlob --overwrite=false --follow-symlinks --put-md5 --follow-symlinks --preserve-smb-info=true --disable-auto-decoding=false --recursive --log-level=INFO";


        //e.g.  C:\test,        spockuploadfileshare/OQC-AOI-Images_Backup/testProduct/
        //or    C:\myDirectory, mycontainer/myBlobDirectory
        public static bool UploadDirectoryToBlob(string sourceDirectory, string storageAccount, string target, FileSkippedDelegate skipped)
        {
            if (!Directory.Exists(sourceDirectory))
            {
                LogHelper.Info(typeof(AzCopyHelper), $"directory {sourceDirectory} doesn't exist anymore");
                return false;
            }

            LogHelper.Info(typeof(AzCopyHelper), $"upload directory {sourceDirectory}");

            string sas = null;
            if (storageAccount == MyApplicationSettings.GetSetting("StorageConnection:AccountName"))
                sas = MyApplicationSettings.GetSetting("StorageConnection:SAS", true);
            else if (storageAccount == MyApplicationSettings.GetSetting("StorageBackupConnection:AccountName"))
                sas = MyApplicationSettings.GetSetting("StorageBackupConnection:SAS", true);
            else
                throw new Exception($"No SAS token available for storage account {storageAccount}");

            var fileParameters = uploadParametersBlob
                .Replace("{storageAccount}", storageAccount)
                .Replace("{localFilePath}", Path.Combine(sourceDirectory, "*"))
                .Replace("{target}", target)
                .Replace("{sas}", sas);

            LogHelper.Info(typeof(AzCopyHelper), $"call azcopy.exe " + fileParameters);
            string result = ExecuteProcess(azCopyPath, "azcopy.exe", fileParameters);

            /* sample output:
                    Job 362cb74f-8200-704c-62e4-18e5d3e172fd has started
                    Log file is located at: C:\Users\v.schmidts\.azcopy\362cb74f-8200-704c-62e4-18e5d3e172fd.log

                    100.0 %, 0 Done, 0 Failed, 0 Pending, 2 Skipped, 2 Total,

                    Job 362cb74f-8200-704c-62e4-18e5d3e172fd summary
                    Elapsed Time (Minutes): 0.0335
                    Number of File Transfers: 2
                    Number of Folder Property Transfers: 0
                    Total Number of Transfers: 2
                    Number of Transfers Completed: 0
                    Number of Transfers Failed: 0
                    Number of Transfers Skipped: 2
                    TotalBytesTransferred: 0
                    Final Job Status: CompletedWithSkipped
             * */

            string totalNumberOfTransfers = result.TextBetween("Total Number of Transfers: ", "\n").Trim();
            string totalNumberOfTransfersCompleted = result.TextBetween("Number of Transfers Completed: ", "\n").Trim();
            string totalNumberOfTransfersFailed = result.TextBetween("Number of Transfers Failed: ", "\n").Trim();
            string totalNumberOfTransfersSkipped = result.TextBetween("Number of Transfers Skipped: ", "\n").Trim();

            if (totalNumberOfTransfers == totalNumberOfTransfersCompleted && totalNumberOfTransfersFailed == "0") { }
            else if (totalNumberOfTransfersSkipped != "0" && totalNumberOfTransfersFailed == "0")
            {
                var logFile = result.TextBetween("Log file is located at:", "\n").Trim();
                if (File.Exists(logFile))
                {
                    var logLines = File.ReadAllLines(logFile);
                    var skippedLines = logLines.Where(l => l.Contains("File already exists, so will be skipped")).ToList();
                    var skippedFiles = skippedLines.Select(l => l.TextBetween("\\\\?\\", "File already exists").Trim()).ToList();
                    skippedFiles.ForEach(f => skipped(f));
                    //Expect all files are uploaded / replaced now
                }
                else
                {
                    throw new Exception("Files have been skipped but no log file could be found. " + Environment.NewLine + result);
                }
            }
            else
            {
                throw new Exception($"error uploading directory {sourceDirectory}. Log:" + Environment.NewLine + result);
            }

            LogHelper.Info(typeof(AzCopyHelper), $"directory upload of {sourceDirectory} successful -> delete sub directories");
            foreach (var subDir in Directory.GetDirectories(sourceDirectory))
                Directory.Delete(subDir, true);
            LogHelper.Info(typeof(AzCopyHelper), $"deletion successful");
            return true;
        }

        private static String ExecuteProcess(String serinDirectory, String file, String arguments)
        {
            Process p = new Process();
            p.StartInfo.WorkingDirectory = serinDirectory; // System.Reflection.Assembly.GetExecutingAssembly().Location;
            p.StartInfo.FileName = Path.Combine(p.StartInfo.WorkingDirectory, file);
            p.StartInfo.Arguments = arguments;
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.CreateNoWindow = true;
            p.StartInfo.WindowStyle = ProcessWindowStyle.Minimized;
            p.StartInfo.RedirectStandardError = true;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.RedirectStandardInput = true;

            p.Start();

            // wait for the child process to exit before
            // reading to the end of its redirected error stream.
            //p.WaitForExit();
            // Read the result
            p.StandardInput.Write("\n");

            String result = p.StandardOutput.ReadToEnd();

            return result;
        }
    }
}
