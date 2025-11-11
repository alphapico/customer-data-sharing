using Azure.Storage.Files.DataLake;
using Azure.Storage.Queues;
using CustomerDataSharingLogic.Helpers;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Linq;
using System.Text;

namespace CustomerDataSharingLogic.CloudLogic
{
    public class FilesArrivedInCloudLogic
    {
        private static string messageArrivedDirectory = @"C:\Temp\CustomerDataSharingMessages\Arrived";
        private static string messageHandledDirectory = @"C:\Temp\CustomerDataSharingMessages\Handled";

        private const string queueName = "data-arrived";

        public static void Execute()
        {
            string connectionString = $"DefaultEndpointsProtocol=https;AccountName={CloudLogicSharedFunctions.storageAccountName};AccountKey={CloudLogicSharedFunctions.storageAccountKey};QueueEndpoint=https://{CloudLogicSharedFunctions.storageAccountName}.queue.core.windows.net/";
            // Instantiate a QueueClient to create and interact with the queue
            QueueClient queueClient = new QueueClient(connectionString, queueName);

            //read from storage queue if there are new entries and store in local json file
            var messageTask = queueClient.ReceiveMessagesAsync(maxMessages: 32);
            messageTask.Wait();
            foreach (var message in messageTask.Result.Value)
            {
                var messageContent = Encoding.UTF8.GetString(Convert.FromBase64String(message.Body.ToString()));
                File.WriteAllText(Path.Combine(messageArrivedDirectory, message.MessageId + ".json"), messageContent);
                
                queueClient.DeleteMessage(message.MessageId, message.PopReceipt);
            }

            //loop local json files and try to handle
            foreach(var file in Directory.GetFiles(messageArrivedDirectory))
            {
                var content = File.ReadAllText(file);
                dynamic dynamicResult = JsonConvert.DeserializeObject(content);

                //try to get the url
                try
                {
                    string url = dynamicResult.data.url;
                    //check for Eviyos
                    if(url.StartsWith($"https://{CloudLogicSharedFunctions.storageAccountName}.blob.core.windows.net/output/Eviyos/"))
                    {
                        var lot = url.Replace($"https://{CloudLogicSharedFunctions.storageAccountName}.blob.core.windows.net/output/Eviyos/", "").Replace("/binInformation.txt", "");
                        DataLakeDirectoryClient targetDirectoryClient = CloudLogicSharedFunctions.OutputFileSystemClient.GetDirectoryClient($"Eviyos/{lot}");
                        DataLakeFileClient targetFileClient = targetDirectoryClient.GetFileClient("binInformation.txt");

                        var binFilesInformationSerialized = CloudLogicSharedFunctions.ReadFromFileClient(targetFileClient);
                        var binFilesInformation = System.Text.Json.JsonSerializer.Deserialize<BinFilesInformation>(binFilesInformationSerialized);

                        ////125 is the max allowed error pixel amount -> if there are more entries send a mail
                        ////outdated -> moved to DeliveryCreated logic
                        //var permanentOffPixelErrors = binFilesInformation.BinFiles
                        //                                .Where(p => p.PermanentOffPixels > 125)
                        //                                .Select(p => $"Detected: {p.PermanentOffPixels}, Allowed: 125, File: {p.BinFileName}")
                        //                                .ToList();
                        //if (permanentOffPixelErrors.Count > 0)
                        //{
                        //    var errorMailSubject = $"Eviyos web portal - invalid Permanent-OFF pixel count identified";
                        //    var errorMailText = $"For an Eviyos bin file the maximum allowed amount of Permanent-OFF pixel was detected higher than allowed:" + Environment.NewLine +
                        //        String.Join(Environment.NewLine, permanentOffPixelErrors) + Environment.NewLine +
                        //        Environment.NewLine +
                        //        "Please note - this is a warning / sanity check.  The upload of this file took place already and it might be available for download by customer";

                        //    var errorPixelMailReceiver = MyApplicationSettings.GetSetting("ErrorPixelMailReceiver").Split(';', ',').ToList();
                        //    MailHelper.SendMail(errorPixelMailReceiver, errorMailSubject, errorMailText);
                        //}

                        //version check - all bin files must have the same version
                        var versions = binFilesInformation.BinFiles
                                                        .Select(f => f.BinFileVersion)
                                                        .Distinct()
                                                        .ToList();
                        if (versions.Count > 1)
                        {
                            var versionsWithAmount = versions.Select(v => $"version {v}, amount {binFilesInformation.BinFiles.Count(b => b.BinFileVersion == v)}").ToList();

                            var errorMailSubject = $"Eviyos web portal - multiple bin file versions identified";
                            var errorMailText = $"for every lot only one bin file version is allowed but multiple were found:" + Environment.NewLine +
                                String.Join(Environment.NewLine, versionsWithAmount) + Environment.NewLine +
                                Environment.NewLine +
                                "Please note - this is a warning / sanity check.  The upload of this file took place already and it might be available for download by customer";

                            var versionMailReceiver = MyApplicationSettings.GetSetting("DifferentBinVersionMailReceiverCC").Split(';', ',').ToList();
                            MailHelper.SendMail(versionMailReceiver, errorMailSubject, errorMailText);
                        }

                        ////check amount of files with expected value (56 or 280)
                        ////removed again as it happened too frequent (amount different)
                        //var allFilesInLotDir = targetDirectoryClient.GetPaths(true);
                        //var amountOfBinFiles = allFilesInLotDir.Where(f => f.Name.EndsWith(".bin")).Count();
                        //if(amountOfBinFiles != (56*2) && amountOfBinFiles != (280*2))
                        //{
                        //    var errorMailSubject = $"Eviyos web portal - invalid amount of files identified";
                        //    var errorMailText = $"The directory {targetDirectoryClient.Name} contains {amountOfBinFiles} files which looks invalid. Please check." +
                        //        String.Join(Environment.NewLine, amountOfBinFiles) + Environment.NewLine +
                        //        Environment.NewLine +
                        //        "Please note - this is a warning / sanity check.  The upload of this file took place already and it might be available for download by customer";

                        //    var wrongAmountMailReceiver = new List<string>() { "volker.schmidts@ams-osram.com", "Muhamad.AwangWahab@ams-osram.com" };
                        //    MailHelper.SendMail(wrongAmountMailReceiver, errorMailSubject, errorMailText);
                        //}

                        //everything fine -> nothing else to be done, so file can be archived
                        var fileInfo = new FileInfo(file);
                        var handledFile = Path.Combine(messageHandledDirectory, fileInfo.Name.Replace(".json", $"_{DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss")}.json"));
                        File.Move(file, handledFile);
                    }

                    //check for IMSE5515 (Aster)
                    //  -> load IMO / TMDB data
                    //          data not available yet -> add to local file list
                    //      validate data
                    //          validation failed -> send mail to colleagues
                    //      create file and upload

                    //Gaudi
                }
                catch (Exception ex) { }
            }
        }
    }
}