using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using CustomerDataSharingLogic.ExternalConnections.Snowflake;
using osram.OSAS.Excel;
using System.Collections.Generic;
using System.Linq;
using System;

namespace CustomerDataSharingLogic.Eviyos
{
    public static class EviyosDataExportFromCosmos
    {
        private const string excelFile = "EviyosData.xlsx";

        public static void Export()
        {
            var values = ExportData();
            ExcelSheet excelSheet = new ExcelSheet()
            {
                Title = "Export",
                Values = values
            };

            ExcelCreator.OpenFileAfterCreation = false;
            ExcelCreator.CreateExcelFile(excelFile, excelSheet);
        }

        private static object[][] ExportData()
        {
            List<object[]> result = new List<object[]>();

            result.Add(new object[]
            {
                "DMC",
                "IC ID",
                "Customer Name",
                "Customer Number",
                "Customer Group",
                "Delivery Number",
                "Batch Number",
                "SAP Lot Number",
                "DTS Lot Number",
                "Shipment Date",
                //"11 Material number",
                //"11 Material number text",
                "Q Material number",
                "Q Material number text"
            });

            SnowflakeDBConnection.EstablishConnection();
            //get snowflake deliveries
            var sapData = SnowflakeDBConnection.GetData($"select * from \"DW\".\"EVIYOS\".\"VW_DELIVERY_DATA\" where " +
                $"shipment_date is not null and " +
                $"batch_number is not null and " +
                $"batch_number is not null and " +
                $"material_number_q_text ilike any ({string.Join(",", EviyosBusinessLogic.materialsQStart.Select(m => "'" + m + "%'"))})");
            var sapDataList = VW_DELIVERY_DATA.GetList(sapData);

            var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.product <> 'IMSE5515' and c.customer_id <> '512345'"); // where NOT IS_DEFINED(c.customer_group)
            cosmosData.Wait();
            Console.WriteLine($"{cosmosData.Result.Count} items found in cosmos db");
            List<EviyosBox> data = cosmosData.Result;
            foreach (var e in data)
            {
                var lotNumbers = sapDataList.Where(s =>
                                        s.CUSTOMER_ID == e.customer_id &&
                                        s.DELIVERY_NUMBER == e.delivery_number &&
                                        s.BATCH_NUMBER == e.batch_number)
                                    .Select(s => s.LOT_ID)
                                    .Distinct()
                                    .ToList();
                var lotNumber = String.Join(",", lotNumbers);

                foreach (var product in e.products)
                {
                    result.Add(new object[]
                    {
                        product.dmc,
                        product.ic_code,
                        e.customer_name,
                        e.customer_id,
                        e.customer_group,
                        e.delivery_number,
                        e.batch_number,
                        lotNumber,
                        (String.IsNullOrEmpty(lotNumber)) ? "" : lotNumber.Substring(0,8),//+"."+lotNumber.Substring(8),
                        e.shipment_date,
                        e.material_number,
                        e.material_text
                    });
                }
            }

            return result.ToArray();
        }
    }
}