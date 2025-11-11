using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

//var builder = FunctionsApplication.CreateBuilder(args);

//builder.Services
//    .AddAzureClients(clientBuilder =>
//    {
//        clientBuilder.AddBlobServiceClient(builder.Configuration.GetSection("InboxStorage"))
//            .WithName("copierOutputBlob");
//    });

//builder.Build().Run();


IConfigurationBuilder configBuilder = null;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureAppConfiguration((hostContext, config) =>
    {
        if (hostContext.HostingEnvironment.IsDevelopment())
        {
            config.AddJsonFile("local.settings.json");
            config.AddUserSecrets<Program>();
            configBuilder = config;
        }
    })
    .ConfigureServices(services =>
    {
        //services.AddApplicationInsightsTelemetryWorkerService();
        //services.ConfigureFunctionsApplicationInsights();
    })
    .ConfigureServices(services =>
    {
        string inboxStorageSettings;
        if (configBuilder == null)
        {
            inboxStorageSettings = Environment.GetEnvironmentVariable("InboxStorage");
        }
        else
        {
            var config = configBuilder.Build();
            inboxStorageSettings = config["InboxStorage"];
        }
        services.AddScoped(serviceProvider => new BlobServiceClient(inboxStorageSettings));
    })
    .Build();

host.Run();



//var builder = FunctionsApplication.CreateBuilder(args)
//    .ConfigureFunctionsWebApplication();

//IConfigurationBuilder configBuilder = null;

//builder.ConfigureFunctionsWebApplication();
////if (builder.Environment.IsDevelopment()) {
//builder.Configuration.AddJsonFile("local.settings.json");
//configBuilder = builder.Configuration.AddUserSecrets<Program>();
////}
//builder.Configuration.AddEnvironmentVariables();


////builder.Services
////    .AddAzureClients(clientBuilder =>
////    {
////        clientBuilder.AddBlobServiceClient(builder.Configuration.GetSection("InboxStorage"))
////            .WithName("processZipBlob");
////    });

//// Application Insights isn't enabled by default. See https://aka.ms/AAt8mw4.
//// builder.Services
////     .AddApplicationInsightsTelemetryWorkerService()
////     .ConfigureFunctionsApplicationInsights();

////var config = configBuilder.Build();
//var host = builder.Build();
////var inboxStorageSettings = host.["InboxStorage"];
////builder.Services.AddScoped(serviceProvider => new BlobServiceClient(inboxStorageSettings));

//host.Run();
