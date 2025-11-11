using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using CustomerDataSharingLogic.ExternalConnections.Snowflake;
using CustomerDataSharingLogic.Helpers;
using osram.OSAS.Logging;
using System;
using System.Linq;
using System.Net.Mail;

namespace CustomerDataSharingLogic.Eviyos
{
    public static class EviyosMissingFileCheck
    {
        public static void Check()
        {
            SnowflakeDBConnection.EstablishConnection();

            var sqlStatement = $"select * from \"DW\".\"EVIYOS\".\"VW_DELIVERY_DATA\" where " +
                $"shipment_date between dateadd(days, -20, CURRENT_DATE) and dateadd(days, -1, CURRENT_DATE) and " +
                $"batch_number is not null and " +
                $"material_number_q_text ilike any ({string.Join(",", EviyosBusinessLogic.materialsQStart.Select(m => $"'{m.Substring(0, 3)}{m.Substring(3)}%'"))}) and " +
                $"delivery_number like any ('51%', '50%', '54%') and " +
                $"customer_group is not null " +
                $"order by delivery_number, batch_number";
            var sapDataTable = SnowflakeDBConnection.GetData(sqlStatement);
            var sapDataList = VW_DELIVERY_DATA.GetList(sapDataTable);
            //material 11123584 does not need bin file -> skip
            sapDataList.RemoveAll(d => d.MATERIAL_NUMBER_11 == "11123584" || d.MATERIAL_NUMBER_11 == "11123583");
            LogHelper.Info(typeof(EviyosMissingFileCheck), $"{sapDataList.Count} sap entries found");

            var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.shipment_date > '{DateTime.Now.AddDays(-30).ToString("yyyy-MM-dd")}'");
            cosmosData.Wait();
            LogHelper.Info(typeof(EviyosMissingFileCheck), $"{cosmosData.Result.Count} cosmos entries found");

            for (int i = sapDataList.Count - 1; i >= 0; i--)
            {
                var sapData = sapDataList[i];
                var entryUploaded = cosmosData.Result.Any(c =>
                    c.customer_group == sapData.CUSTOMER_GROUP &&
                    c.delivery_number == sapData.DELIVERY_NUMBER &&
                    c.batch_number == sapData.BATCH_NUMBER);

                if (entryUploaded)
                    sapDataList.RemoveAt(i);
            }

            LogHelper.Info(typeof(EviyosMissingFileCheck), $"{sapDataList.Count} missing entries found");

            if (sapDataList.Count == 0) //great - no entries found -> stop
                return;

            var lines = sapDataList
                            .Select(s => $"<td>{s.CUSTOMER_GROUP}</td>" +
                                        $"<td>{s.CUSTOMER_NAME}</td>" +
                                        $"<td>{s.PLANT}</td>" +
                                        $"<td>{s.DELIVERY_NUMBER}</td>" +
                                        $"<td>{s.MATERIAL_NUMBER_11}</td>" +
                                        $"<td>{s.MATERIAL_NUMBER_11_TEXT}</td>" +
                                        $"<td>{s.MATERIAL_NUMBER_Q}</td>" +
                                        $"<td>{s.MATERIAL_NUMBER_Q_TEXT}</td>" +
                                        $"<td>{s.BATCH_NUMBER}</td>" +
                                        $"<td>{s.LOT_ID}</td>" +
                                        $"<td>{s.AMOUNT_IN_BATCH}</td>" +
                                        $"<td>{s.SHIPMENT_DATE.ToString("yyyy-MM-dd")}</td>")
                            .ToList();

            lines.Insert(0, "<td>Customer group</td><td>Customer name</td><td>Plant</td><td>Delivery number</td><td>11 Material number</td><td>11 Material number text</td><td>Q Material number</td><td>Q Material number text</td><td>Batch number</td><td>Lot ID</td><td>Amount</td><td>Shipment date</td>");

            var errorMailSubject = $"Eviyos - shipments without data found";
            var errorMailText = $"<html><body>Please check the following SAP deliveries which have not been uploaded to the portal yet:<br />" +
                "<table border=\"1\">" + String.Join(Environment.NewLine, lines.Select(l => "<tr>" + l + "</tr>")) + "</table>" +
                "</body></html>";
            var mailReceiver = MyApplicationSettings.GetSetting("MissingBinFilesMailReceiver");
            var mailReceiverCC = MyApplicationSettings.GetSetting("MissingBinFilesMailReceiverCC");

            //create a new instance of the Mail-class
            MailHelper mail = new MailHelper();

            foreach (var receiver in mailReceiver.Split(',', ';'))
                mail.Receiver.Add(new MailAddress(receiver));

            mail.Cc = mailReceiverCC.Split(',', ';').ToArray();

            mail.Subject = errorMailSubject;
            mail.Body = errorMailText;
            mail.IsBodyHtml = true;

            //send the mail
            mail.Send();
        }
    }
}