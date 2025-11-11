using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using osram.OSAS.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CustomerDataSharingLogic.Eviyos
{
    public class EviyosDuplicateValidationCheck
    {
        public static List<EviyosBox> EviyosGetValuesWithDuplicates(DateTime? minShipmentDate = null, DateTime? maxShipmentDate = null)
        {
            if (minShipmentDate == null)
                minShipmentDate = DateTime.Now.Date.AddDays(-180);
            if (maxShipmentDate == null)
                maxShipmentDate = DateTime.Now.Date.AddDays(2);

            var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.product <> 'IMSE5515' and c.customer_id <> '512345' and " +
                    $"c.shipment_date >= '{((DateTime)minShipmentDate).ToString("yyyy-MM-dd")}' and c.shipment_date <= '{((DateTime)maxShipmentDate).ToString("yyyy-MM-dd")}'");
            cosmosData.Wait();

            LogHelper.Info(typeof(EviyosDuplicateValidationCheck), $"{cosmosData.Result.Count} items found in cosmos db");
            List<EviyosBox> data = cosmosData.Result;
            var allIcId = data.SelectMany(d => d.products.Select(p => p.ic_code)).ToList();
            var duplicatedIcId = allIcId.GroupBy(i => i)
                                .Where(i => i.Count() > 1)
                                .Select(i => i.Key)
                                .ToList();

            var relevantCosmosEntries = data.Where(d => d.products.Any(p => duplicatedIcId.Contains(p.ic_code))).ToList();
            if(relevantCosmosEntries.Count > 0)
            {
                var errors = relevantCosmosEntries.Select(e => $"customer: {e.customer_name}, delivery: {e.delivery_number}, batch: {e.batch_number}, lot: {e.lot_id}").ToList();
                String errorMessage = String.Join(Environment.NewLine, errors);
                LogHelper.Error(typeof(EviyosDuplicateValidationCheck), $"{cosmosData.Result.Count} items found in cosmos db with duplicate DMC: " + Environment.NewLine + errorMessage + Environment.NewLine + "please check!");
            }

            return relevantCosmosEntries;
        }
    }
}