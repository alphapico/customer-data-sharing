using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text;

namespace CustomerDataSharingFA
{
    public class DeliveryCreated
    {
        private readonly ILogger<DeliveryCreated> _logger;
        private readonly BlobServiceClient _blobServiceClient;

        public DeliveryCreated(ILogger<DeliveryCreated> logger, BlobServiceClient blobServiceClient)
        {
            _logger = logger;
            _blobServiceClient = blobServiceClient;
        }

        [Function("DeliveryCreated")]
        public IActionResult Run([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            string data = GetRequestBody(req);
            _logger.LogInformation("Content read from post: " + data);

            string outputContainerName = "inbox";
            var outputContainerClient = _blobServiceClient.GetBlobContainerClient(outputContainerName);

            var outputFileName = "ApiRequests/" + DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss_fff") + "__" + Guid.NewGuid() + ".json";
            var outputBlobClient = outputContainerClient.GetBlobClient(outputFileName);
            _logger.LogInformation("write content to " + outputFileName);

            using var outputStream = new MemoryStream(Encoding.UTF8.GetBytes(data ?? ""));

            // Upload the extracted file
            outputBlobClient.Upload(outputStream, overwrite: true);

            return new OkObjectResult("finished!");
        }

        private static string GetRequestBody(HttpRequest req)
        {
            var bodyStream = new StreamReader(req.Body);
            //bodyStream.BaseStream.Seek(0, SeekOrigin.Begin);
            var bodyTextTask = bodyStream.ReadToEndAsync();
            bodyTextTask.Wait();
            return bodyTextTask.Result;
        }
    }
}

/*
steps:
    new message arrived -> store json to storage account
    then try to handle:
        check if files are there
        yes: 
            move files to corresponding download blob
            create cosmos entry
        no:
            stop processing

additional on prem app checking for files

 */

