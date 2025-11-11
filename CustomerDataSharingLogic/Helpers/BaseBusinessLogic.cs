using CustomerDataSharingLogic.Eviyos;
using CustomerDataSharingLogic.ExternalConnections.ADLS;
using osram.OSAS.Logging;
using System;
using System.IO;

namespace CustomerDataSharingLogic.Helpers
{
    public abstract class BaseBusinessLogic
    {
        public static string UploadDirectory { get { return MyApplicationSettings.GetSetting("UploadDirectory"); } }
        public static string UploadArchiveDirectory { get { return MyApplicationSettings.GetSetting("UploadArchiveDirectory"); } }

        private static StorageConnection storage;
        protected static void UploadFilesToStorage()
        {
            LogHelper.Info(typeof(BaseBusinessLogic), "Upload all files with AzCopy");
            try
            {
                if (Directory.GetDirectories(UploadDirectory).Length > 0)
                {
                    storage = StorageConnection.Default;
                    AzCopyHelper.UploadDirectoryToBlob(UploadDirectory, MyApplicationSettings.GetSetting("StorageConnection:AccountName"), "measurements/", FileSkipped);
                }

                if (Directory.GetDirectories(UploadArchiveDirectory).Length > 0)
                {
                    storage = StorageConnection.Backup;
                    AzCopyHelper.UploadDirectoryToBlob(UploadArchiveDirectory, MyApplicationSettings.GetSetting("StorageBackupConnection:AccountName"), "measurements/", FileSkipped);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Files were not uploaded to storage! please check withi high priority!", ex);
            }
            LogHelper.Info(typeof(BaseBusinessLogic), "Upload successful");
        }

        protected static void FileSkipped(string filePath)
        {
            var fileContent = File.ReadAllBytes(filePath);
            var fileInfo = new FileInfo(filePath);

            //archive the file
            // $"{fileParts[0]}/{waferLot}/{waferNumber}/";
            //var archiveDirectory = uploadTarget.Substring(uploadTarget.IndexOf("/") + 1) + fileInfo.Directory.FullName.Substring(UploadDirectory.LastIndexOf("\\") + 1).Replace("\\", "/") + "/";
            var archiveDirectory = fileInfo.Directory.FullName.Substring(UploadDirectory.Length).Replace("\\", "/") + "/";
            var archiveUploadTask = storage.UploadFileContent(archiveDirectory, fileInfo.Name, fileContent, StorageConnection.WhenFileAlreadyExists.RenameOld);
            archiveUploadTask.Wait();

            var filePropertiesTask = storage.GetFileProperties(archiveDirectory, fileInfo.Name);
            filePropertiesTask.Wait();
            if (filePropertiesTask.Result.ContentLength == fileContent.Length) //file does exist and is uploaded -> delete original
            {
                LogHelper.Info(typeof(EviyosBusinessLogic), $"    file size fits -> delete local");
                fileInfo.Delete();
            }
        }
    }
}
