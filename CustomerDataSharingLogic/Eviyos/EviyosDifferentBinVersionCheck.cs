using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using CustomerDataSharingLogic.ExternalConnections.Snowflake;
using CustomerDataSharingLogic.Helpers;
using osram.OSAS.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;

namespace CustomerDataSharingLogic.Eviyos
{
    public class EviyosDifferentBinVersionCheck
    {
        public static void Check()
        {
            LogHelper.Info(typeof(EviyosDifferentBinVersionCheck), $"Application started");
            SnowflakeDBConnection.EstablishConnection();

            var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.shipment_date > '{DateTime.Now.AddDays(-10).ToString("yyyy-MM-dd")}'");
            cosmosData.Wait();

            LogHelper.Info(typeof(EviyosDifferentBinVersionCheck), $"{cosmosData.Result} entries found in cosmos db");

            foreach (var data in cosmosData.Result)
            {
                LogHelper.Info(typeof(EviyosDifferentBinVersionCheck), $"handle batch number {data.batch_number}");
                var batchVersions = data.products.Select(p => p.version)
                                            .Where(v => !String.IsNullOrEmpty(v))
                                            .Distinct()
                                            .ToList();

                if (batchVersions.Count > 1)
                {
                    LogHelper.Info(typeof(EviyosDifferentBinVersionCheck), $"  {batchVersions.Count} versions found -> create mail");
                    var maxVersion = batchVersions.Max();

                    int fileCountWithCorrectVersion = data.products.Count(p => p.version == maxVersion);

                    var lines = new List<String>();
                    foreach (var product in data.products)
                    {
                        if (product.version == maxVersion)
                            continue;

                        lines.Add($"<td>{data.customer_name}</td>" +
                                    $"<td>{data.shipment_date}</td>" +
                                    $"<td>{data.delivery_number}</td>" +
                                    $"<td>{data.batch_number}</td>" +
                                    $"<td>{data.lot_id}</td>" +
                                    $"<td>{product.ic_code}</td>" +
                                    $"<td>{product.dmc}</td>" +
                                    $"<td>{product.version}</td>");
                    }

                    lines.Insert(0, "<td>Ccstomer</td><td>shipment date</td><td>delivery</td><td>batch</td><td>lot id</td><td>IC Code</td><td>DMC</td><td>version</td>");

                    var errorMailSubject = $"Eviyos - batch with multiple file versions found";
                    var errorMailText = $"<html><body>Please check the following bin files because not all files are in the latest version {maxVersion} (files with max version: {fileCountWithCorrectVersion}, skipped in this list):<br />" +
                        "<table border=\"1\">" + String.Join(Environment.NewLine, lines.Select(l => "<tr>" + l + "</tr>")) + "</table><br /> <br />" +
                        "Boundaries for this check: We are considering the last 10 days of shipments" +
                        "</body></html>";
                    var mailReceiver = MyApplicationSettings.GetSetting("DifferentBinVersionMailReceiverCC");

                    //create a new instance of the Mail-class
                    MailHelper mail = new MailHelper();

                    foreach (var receiver in mailReceiver.Split(',', ';'))
                        mail.Receiver.Add(new MailAddress(receiver));

                    //mail.Cc = mailReceiverCC.Split(',', ';').ToArray();

                    mail.Subject = errorMailSubject;
                    mail.Body = errorMailText;
                    mail.IsBodyHtml = true;

                    //send the mail
                    mail.Send();
                }
                else if(batchVersions.Count == 1 && data.material_number_11 != null && data.material_number_11.IsContainedIn("11137766", "11123357") && batchVersions[0].StartsWith("0.") && data.shipment_date > new DateTime(2024, 07, 24))
                {
                    var errorMailSubject = $"Eviyos - old file version found";
                    var errorMailText = $"<html><body>Please check the bin files of the batch {data.batch_number} because they contain an old version {batchVersions[0]}.<br /> <br />" +
                        "Boundaries for this check: We are considering the last 10 days of shipments" +
                        "</body></html>";
                    var mailReceiver = MyApplicationSettings.GetSetting("DifferentBinVersionMailReceiverCC");

                    //create a new instance of the Mail-class
                    MailHelper mail = new MailHelper();

                    foreach (var receiver in mailReceiver.Split(',', ';'))
                        mail.Receiver.Add(new MailAddress(receiver));

                    //mail.Cc = mailReceiverCC.Split(',', ';').ToArray();

                    mail.Subject = errorMailSubject;
                    mail.Body = errorMailText;
                    mail.IsBodyHtml = true;

                    //send the mail
                    mail.Send();
                }
                else
                {
                    LogHelper.Info(typeof(EviyosDifferentBinVersionCheck), $"  only one version found -> skip");
                }
            }
        }
    }
}
