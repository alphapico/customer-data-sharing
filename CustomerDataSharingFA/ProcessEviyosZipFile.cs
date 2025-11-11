using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Files.DataLake;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.IO.Compression;
using System.Text;
using System.Text.Json;

namespace CustomerDataSharingFA
{
    /// <summary>
    /// this serverless function reacts on a new zip in the input container and 'Eviyos' subdirectory. It then
    /// - unzips all files in the output container with a new directory with the name of the zip file
    ///     in case the directory does already exist it will be moved to output/Eviyos/Replaced/... with the current UTC timestamp
    /// - while unzipping it reads the PermanentOffPixels and versions and creates an additional file with this information
    /// - moves the file to an archive (with current UTC timestamp)
    /// 
    /// after the file binInformation.txt is created in the storage an event on the storage account will be triggered and fill an entry to a queue -> then the next loacal application will react on
    /// </summary>
    public class ProcessEviyosZipFile
    {
        private readonly BlobServiceClient _blobServiceClient;
        private DataLakeServiceClient serviceClient;
        private DataLakeFileSystemClient inputFileSystemClient;
        private DataLakeFileSystemClient outputFileSystemClient;
        private const string inputContainerName = "inbox";
        private const string outputContainerName = "output";
        private const string ProductDirectoryName = "Eviyos";


        public ProcessEviyosZipFile(BlobServiceClient blobServiceClient)
        {
            _blobServiceClient = blobServiceClient;
        }

        [Function($"Process{ProductDirectoryName}ZipFile")]
        public async Task Run(
            [BlobTrigger(inputContainerName + "/"+ ProductDirectoryName + "/{name}", Connection = "InboxStorage")] Stream zipBlob,
            string name,
            ILogger log)
        {
            log?.LogInformation($"Triggered for blob: {name}");

            if (name.StartsWith("Archive") || !name.ToLower().EndsWith(".zip"))
            {
                log?.LogInformation($" -> ignore");
                return;
            }

            //if (!name.Contains("HRH17014.07"))
            //    return;

            // Blob clients
            var inputContainerClient = _blobServiceClient.GetBlobContainerClient(inputContainerName);
            var outputContainerClient = _blobServiceClient.GetBlobContainerClient(outputContainerName);

            try
            {
                // Ensure output container exist
                await outputContainerClient.CreateIfNotExistsAsync(PublicAccessType.None);

                var targetDir = name;
                if (targetDir.Contains("/"))
                    targetDir = targetDir.Substring(targetDir.IndexOf("/") + 1);
                targetDir = targetDir.Replace(".zip", "").Replace(".", "");

                //if target already exists -> move existing dir to 'Replaced' and create a new one
                var targetDirOutputBlobClient = outputContainerClient.GetBlobClient(ProductDirectoryName + "/" + targetDir);
                if(targetDirOutputBlobClient.Exists())
                {
                    string sourceDirectory = ProductDirectoryName + "/" + targetDir;
                    string destinationDirectory = ProductDirectoryName + "/Replaced/" + targetDir + DateTime.UtcNow.ToString("_yyyy-MM-dd_HH-mm-ss");
                    log?.LogInformation($"target output of '{sourceDirectory}' already exist -> rename to " + destinationDirectory);
                    ConnectAzureStorage();

                    // Get the source directory client
                    DataLakeDirectoryClient sourceDirectoryClient = outputFileSystemClient.GetDirectoryClient(sourceDirectory);
                    // Rename (Move) the directory
                    DataLakeDirectoryClient destinationDirectoryClient = await sourceDirectoryClient.RenameAsync(destinationDirectory);

                    log?.LogInformation($"Directory moved from '{sourceDirectory}' to '{destinationDirectory}' successfully.");
                }

                var binInformation = new BinFilesInformation();

                // Process the zip file
                using (var archive = new ZipArchive(zipBlob))
                {
                    foreach (var entry in archive.Entries)
                    {
                        if (!string.IsNullOrEmpty(entry.Name)) // Skip directories
                        {
                            var outputBlobClient2 = outputContainerClient.GetBlobClient($"{ProductDirectoryName}/{targetDir}/{entry.Name}");

                            log?.LogInformation($"Extracting file: {entry.FullName} to output container.");

                            using var entryStream = entry.Open();

                            using var outputStream = new MemoryStream();
                            await entryStream.CopyToAsync(outputStream);
                            outputStream.Position = 0;

                            StreamReader reader = new StreamReader(outputStream);
                            string text = reader.ReadLine();

                            string version = null;
                            if (String.IsNullOrEmpty(version) && text.Contains("Format Version:"))
                                version = text.Replace("Format Version:", "").Trim();

                            while (text == null || !text.StartsWith("PermanentOffPixels")) {
                                text = reader.ReadLine();
                            }
                            if (text != null && text.StartsWith("PermanentOffPixels"))
                            {
                                var pixelOffAmountStr = text.Replace("PermanentOffPixels:", "").Trim();
                                int pixelOffAmount = -1;
                                if (int.TryParse(pixelOffAmountStr, out pixelOffAmount))
                                {
                                    binInformation.BinFiles.Add(new BinFilesInformation.BinFileInformation()
                                    {
                                        BinFileName = entry.Name,
                                        PermanentOffPixels = pixelOffAmount,
                                        BinFileVersion = version
                                    });
                                }
                            }
                            outputStream.Position = 0;
                            // Upload the extracted file
                            await outputBlobClient2.UploadAsync(outputStream, overwrite: true);
                        }
                    }
                }
                var binInformationBlobClient = outputContainerClient.GetBlobClient($"{ProductDirectoryName}/{targetDir}/binInformation.txt");
                var binInformationSerialized = JsonSerializer.Serialize(binInformation);
                await binInformationBlobClient.UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes(binInformationSerialized)), overwrite: true);

                // Move the original zip to the archive container
                {
                    ConnectAzureStorage();

                    // Get the source directory client
                    DataLakeDirectoryClient sourceDirectoryClient = inputFileSystemClient.GetDirectoryClient(ProductDirectoryName);
                    DataLakeFileClient sourceFileClient = sourceDirectoryClient.GetFileClient($"{name}");

                    DataLakeDirectoryClient targetDirectoryClient = inputFileSystemClient.GetDirectoryClient(ProductDirectoryName + "/Archive");
                    DataLakeFileClient targetFileClient = targetDirectoryClient.GetFileClient(name.Replace(".zip", "_" + DateTime.UtcNow.ToString("yyyy-MM-dd_HH-mm-ss") + ".zip"));

                    log?.LogInformation($"move '{sourceFileClient.Path}' to " + targetFileClient.Path);

                    // Rename (Move) the directory
                    await sourceFileClient.RenameAsync(targetFileClient.Path);

                    log?.LogInformation($"Directory moved from '{ProductDirectoryName}/{name}' to '{targetFileClient.Path}' successfully.");
                }

                log?.LogInformation("Zip processing completed successfully.");
            }
            catch (Exception ex)
            {
                log?.LogError($"Error processing zip file: {ex.Message}");
                throw;
            }
        }

        private void ConnectAzureStorage()
        {
            if (serviceClient != null)
                return;

            var config = new ConfigurationBuilder()
                .AddUserSecrets<Program>() // Loads secrets
                .Build();

            // Retrieve secrets
            string storageConnection = config["InboxStorage"];
            if(String.IsNullOrEmpty(storageConnection))
                storageConnection = Environment.GetEnvironmentVariable("InboxStorage");
            Dictionary<String, String> storageConnectionEntries = storageConnection.Split(';').ToDictionary(e => e.Substring(0, e.IndexOf("=")), e2 => e2.Substring(e2.IndexOf("=") + 1));
            string accountName = storageConnectionEntries["AccountName"];
            string accountKey = storageConnectionEntries["AccountKey"];

            var dfsUri = new Uri("https://" +accountName + ".dfs.core.windows.net");
            StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
            serviceClient = new DataLakeServiceClient(dfsUri, credential);
            inputFileSystemClient = serviceClient.GetFileSystemClient(inputContainerName);
            outputFileSystemClient = serviceClient.GetFileSystemClient(outputContainerName);
        }

        public class BinFilesInformation
        {
            public List<BinFileInformation> BinFiles { get; set; } = new List<BinFileInformation>();

            public class BinFileInformation
            {
                public string BinFileName { get; set; }
                public int PermanentOffPixels { get; set; }
                public string BinFileVersion { get; set; }
            }
        }
    }
}