using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using osram.OSAS.Logging;
using System;
using System.IO;
using System.Reflection;

namespace CustomerDataSharingLogic.Helpers
{
    public class MyApplicationSettings
    {
        private const string encryptionKey = @"IZ2tTf2QkFqtJnjzaC2He/IhMud9X0PxUZu2Lx401/c=";
        private const string encryptionVector = @"6LHzYzU3m0UO8OxAqHKHLw==";

        private static HostApplicationBuilder builder;
        public static string GetSetting(string key, bool isEncrypted = false)
        {
            if (builder == null)
                InitiateBuilder();

            LogHelper.Info(typeof(MyApplicationSettings), $"loading setting {key} from config (encrypted: {isEncrypted})");

            var value = builder.Configuration.GetSection(key).Value;
            if (isEncrypted && !String.IsNullOrEmpty(value))
                value = EncryptionHelper.DecryptText(value, encryptionKey, encryptionVector);

            return value;
        }

        public static string DecryptSetting(string value)
        {
            return EncryptionHelper.DecryptText(value, encryptionKey, encryptionVector);
        }

        private static void InitiateBuilder()
        {
            builder = Host.CreateApplicationBuilder();

            builder.Configuration.Sources.Clear();

            var assemblyConfigurationAttribute = Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyConfigurationAttribute>();
            var buildConfigurationName = assemblyConfigurationAttribute?.Configuration;

            var location = AppContext.BaseDirectory;
            LogHelper.Info(typeof(MyApplicationSettings), $"starting with configuration {buildConfigurationName}, location {location}");

            builder.Configuration
                .AddJsonFile(Path.Combine(location, "appsettings.json"), optional: true, reloadOnChange: true)
                        .AddJsonFile($"appsettings.{buildConfigurationName}.json", true, true);
        }
    }
}