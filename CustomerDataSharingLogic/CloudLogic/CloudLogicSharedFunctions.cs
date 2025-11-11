using Azure.Storage;
using Azure.Storage.Files.DataLake;
using CustomerDataSharingLogic.Helpers;
using System;
using System.IO;
using System.Text;

namespace CustomerDataSharingLogic.CloudLogic
{
    internal class CloudLogicSharedFunctions
    {
        private static DataLakeServiceClient serviceClient;
        internal static string storageAccountName = MyApplicationSettings.GetSetting("StorageConnection:AccountName");
        internal static string storageAccountKey = MyApplicationSettings.GetSetting("StorageConnection:AccountKey", true);

        private const string inputContainerName = "inbox";
        private const string outputContainerName = "output";

        private static DataLakeFileSystemClient inputFileSystemClient;
        internal static DataLakeFileSystemClient InputFileSystemClient
        {
            get
            {
                if (inputFileSystemClient == null)
                    ConnectAzureStorage();
                return inputFileSystemClient;
            }
            private set
            {
                inputFileSystemClient = value;
            }
        }
        private static DataLakeFileSystemClient outputFileSystemClient;
        internal static DataLakeFileSystemClient OutputFileSystemClient
        {
            get
            {
                if (outputFileSystemClient == null)
                    ConnectAzureStorage();
                return outputFileSystemClient;
            }
            private set
            {
                outputFileSystemClient = value;
            }
        }

        internal static string ReadFromFileClient(DataLakeFileClient client)
        {
            MemoryStream stream = new MemoryStream();
            client.ReadTo(stream);
            stream.Position = 0;
            StreamReader sr = new StreamReader(stream, Encoding.UTF8);
            var result = sr.ReadToEnd();
            return result;
        }


        private static void ConnectAzureStorage()
        {
            if (serviceClient != null)
                return;

            var dfsUri = new Uri("https://" + storageAccountName + ".dfs.core.windows.net");
            StorageSharedKeyCredential credential = new StorageSharedKeyCredential(storageAccountName, storageAccountKey);
            serviceClient = new DataLakeServiceClient(dfsUri, credential);
            inputFileSystemClient = serviceClient.GetFileSystemClient(inputContainerName);
            outputFileSystemClient = serviceClient.GetFileSystemClient(outputContainerName);
        }
    }
}
