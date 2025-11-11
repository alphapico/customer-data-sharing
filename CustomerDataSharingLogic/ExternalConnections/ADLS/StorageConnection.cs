using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using CustomerDataSharingLogic.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace CustomerDataSharingLogic.ExternalConnections.ADLS
{
    public class StorageConnection
    {
        private static StorageConnection defaultConnection;
        private static StorageConnection backupConnection;

        public static StorageConnection Default
        {
            get
            {
                if (defaultConnection == null)
                {
                    defaultConnection = new StorageConnection()
                    {
                        TenantID = MyApplicationSettings.GetSetting("StorageConnection:TenantID"),
                        ClientID = MyApplicationSettings.GetSetting("StorageConnection:ClientID"),
                        ClientSecret = MyApplicationSettings.GetSetting("StorageConnection:ClientSecret", true),
                        AccountKey = MyApplicationSettings.GetSetting("StorageConnection:AccountKey", true),
                        AccountName = MyApplicationSettings.GetSetting("StorageConnection:AccountName"),
                        FileSystemName = MyApplicationSettings.GetSetting("StorageConnection:FileSystemName")
                    };
                    defaultConnection.ClientSecret = "xxx";
                    defaultConnection.Initialize();
                }
                return defaultConnection;
            }
            set
            {
                defaultConnection = value;
            }
        }

        public static StorageConnection Backup
        {
            get
            {
                if (backupConnection == null)
                {
                    backupConnection = new StorageConnection()
                    {
                        TenantID = MyApplicationSettings.GetSetting("StorageBackupConnection:TenantID"),
                        ClientID = MyApplicationSettings.GetSetting("StorageBackupConnection:ClientID"),
                        ClientSecret = MyApplicationSettings.GetSetting("StorageBackupConnection:ClientSecret", true),
                        AccountKey = MyApplicationSettings.GetSetting("StorageBackupConnection:AccountKey", true),
                        AccountName = MyApplicationSettings.GetSetting("StorageBackupConnection:AccountName"),
                        FileSystemName = MyApplicationSettings.GetSetting("StorageBackupConnection:FileSystemName")
                    };
                    backupConnection.Initialize();
                }
                return backupConnection;
            }
        }


        public static StorageConnection GetNewStorageConnection(string tenantID, string accountKey, string accountName, string fileSystemName)
        {
            var newConnection = new StorageConnection()
            {
                TenantID = tenantID,
                ClientID = null,
                ClientSecret = null,
                AccountKey = accountKey,
                AccountName = accountName,
                FileSystemName = fileSystemName
            };
            newConnection.Initialize();
            return newConnection;
        }

        public string TenantID { get; private set; }
        public string ClientID { get; private set; }
        protected string ClientSecret { get; private set; }
        public string AccountKey { get; private set; }
        public string AccountName { get; private set; }
        public string FileSystemName { get; private set; }

        private DataLakeServiceClient serviceClient;
        private DataLakeFileSystemClient fileSystemClient;

        public StorageConnection()
        {
        }

        public void Initialize()
        {
            var dfsUri = new Uri("https://" + AccountName + ".dfs.core.windows.net");
            if(!String.IsNullOrEmpty(TenantID) && !String.IsNullOrEmpty(ClientSecret))
            {
                TokenCredential credential = new ClientSecretCredential(
                    TenantID, ClientID, ClientSecret, new TokenCredentialOptions());
                serviceClient = new DataLakeServiceClient(dfsUri, credential);
            }
            else if (!String.IsNullOrEmpty(AccountKey))
            {
                StorageSharedKeyCredential credential = new StorageSharedKeyCredential(AccountName, AccountKey);
                serviceClient = new DataLakeServiceClient(dfsUri, credential);
            }
            else
            {
                throw new Exception("Missing credentials to create an ADLS connection");
            }
            fileSystemClient = serviceClient.GetFileSystemClient(FileSystemName);
        }

        public Uri GetUri(string directory, string fileName)
        {
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directory);
            DataLakeFileClient fileClient = directoryClient.GetFileClient(fileName);
            return fileClient.Uri;
        }

        public async Task<byte[]> DownloadFileContent(string directory, string fileName)
        {
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directory);
            DataLakeFileClient fileClient = directoryClient.GetFileClient(fileName);
            Response<FileDownloadInfo> downloadResponse = await fileClient.ReadAsync();

            using (BinaryReader reader = new BinaryReader(downloadResponse.Value.Content))
            {
                return reader.ReadBytes((int)downloadResponse.Value.ContentLength);
            }
        }

        public async Task<List<String>> ListFilesInDirectory(string directory)
        {
            var enumerator = fileSystemClient.GetPathsAsync(directory).GetAsyncEnumerator();

            await enumerator.MoveNextAsync();

            PathItem item = enumerator.Current;

            List<String> resultList = new List<string>();
            while (item != null)
            {
                resultList.Add(item.Name);

                if (!await enumerator.MoveNextAsync())
                {
                    break;
                }

                item = enumerator.Current;
            }
            return resultList;
        }

        public async Task UploadFileContent(string directory, string fileName, byte[] newZipContent, WhenFileAlreadyExists whenFileAlreadyExists = WhenFileAlreadyExists.Skip)
        {
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directory);
            DataLakeFileClient fileClient = directoryClient.GetFileClient(fileName);

            var exists = fileClient.Exists();
            if (exists.Value && whenFileAlreadyExists == WhenFileAlreadyExists.Skip)
                return;
            else if (exists.Value && whenFileAlreadyExists == WhenFileAlreadyExists.Rename)
            {
                int index = 1;
                int dotPosition = fileName.LastIndexOf(".");
                while (exists.Value)
                {
                    string newFileName = fileName.Substring(0, dotPosition) + "_" + (index++) + fileName.Substring(dotPosition);
                    fileClient = directoryClient.GetFileClient(newFileName);
                    exists = fileClient.Exists();
                }
            }
            else if (exists.Value && whenFileAlreadyExists == WhenFileAlreadyExists.RenameOld)
            {
                int index = 1;
                int dotPosition = fileName.LastIndexOf(".");

                DataLakeFileClient fileClient2 = null;
                string newFileName = null;
                while (exists.Value)
                {
                    newFileName = fileName.Substring(0, dotPosition) + "_" + (index++) + fileName.Substring(dotPosition);
                    fileClient2 = directoryClient.GetFileClient(newFileName);
                    exists = fileClient2.Exists();
                }
                //rename the old file
                fileClient.Rename(fileClient2.Path);
            }

            using (var mem = new MemoryStream(newZipContent))
            {
                await fileClient.UploadAsync(mem, true);
            }
        }

        public Stream GetStream(string directory, string fileName)
        {
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directory);
            DataLakeFileClient fileClient = directoryClient.GetFileClient(fileName);
            return fileClient.OpenRead();
        }

        public async Task MoveFileContent(string directory, string fileName, string destinationPath)
        {
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directory);
            DataLakeFileClient fileClient = directoryClient.GetFileClient(fileName);
            await fileClient.RenameAsync(destinationPath);
        }

        public async Task CreateDirectory(string directory)
        {
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directory);
            await directoryClient.CreateAsync();
        }

        public async Task<PathProperties> GetFileProperties(string directory, string fileName)
        {
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directory);
            DataLakeFileClient fileClient = directoryClient.GetFileClient(fileName);
            return fileClient.GetProperties();
        }

        public enum WhenFileAlreadyExists
        {
            Skip,
            Rename,
            RenameOld,
            Overwrite
        }
    }
}
