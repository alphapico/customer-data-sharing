using osram.OSAS.Database.Framework;
using System;
using System.Collections.Generic;
using System.Data;

namespace CustomerDataSharingLogic.ExternalConnections.Snowflake
{
    public class VW_DELIVERY_DATA : BaseBusinessObject
    {
        public string CUSTOMER_ID { get; set; }
        public string CUSTOMER_NAME { get; set; }
        public string CUSTOMER_GROUP { get; set; }
        public string CUSTOMER_GROUP_NAME { get; set; }
        public string COMPANY_CODE { get; set; }
        public string SALES_ORGANIZATION { get; set; }
        public string DISTRIBUTION_CHANNEL { get; set; }
        public string PLANT { get; set; }
        public string CUSTOMER_PO_NUMBER { get; set; }
        public string SALES_ORDER_NUMBER { get; set; }
        public string SALES_ORDER_ITEM { get; set; }
        public DateTime ORDER_DATE { get; set; }
        public double ORDERED_QUANTITY { get; set; }
        public string MATERIAL_NUMBER_11 { get; set; }
        public string MATERIAL_NUMBER_11_TEXT { get; set; }
        public string MATERIAL_NUMBER_Q { get; set; }
        public string MATERIAL_NUMBER_Q_TEXT { get; set; }
        public string DELIVERY_NUMBER { get; set; }
        public string DELIVERY_ITEM { get; set; }
        public DateTime SHIPMENT_DATE { get; set; }
        public string BATCH_NUMBER { get; set; }
        public double BATCH_QUANTITY { get; set; }
        public double QUANTITY_IN_BATCH { get; set; }
        public string BOX_ID { get; set; }
        public string SALES_UNIT { get; set; }
        public string LOT_ID { get; set; }
        public int AMOUNT_IN_BATCH { get; set; }

        public static List<VW_DELIVERY_DATA> GetList(DataTable sapData)
        {
            var sapDataList = new List<VW_DELIVERY_DATA>();
            foreach (DataRow row in sapData.Rows)
            {
                var deliveryData = new VW_DELIVERY_DATA();
                deliveryData.FillObjectByDataRow(row);
                deliveryData.MATERIAL_NUMBER_Q_TEXT = deliveryData.MATERIAL_NUMBER_Q_TEXT.Replace("KEW GB", "KEWGB");
                sapDataList.Add(deliveryData);
            }
            return sapDataList;
        }
    }

    public class VW_DELIVERY_DATA_INCL_IMO : VW_DELIVERY_DATA
    {
        public static String[] validMaterial = new String[] { "11144518" };

        //public string HANDLING_UNIT_CONTENT { get; set; }
        //public string HANDLING_UNIT_CONTENT_MATERIAL_ID { get; set; }
        //public string HANDLING_UNIT_CONTENT_BATCH_ID { get; set; }
        public string HANDLING_UNIT_ID { get; set; }
        public string HANDLING_UNIT_CONTENT_IMO_CHECK_CODE { get; set; }

        public static List<VW_DELIVERY_DATA_INCL_IMO> GetList(DataTable sapData)
        {
            var sapDataList = new List<VW_DELIVERY_DATA_INCL_IMO>();
            foreach (DataRow row in sapData.Rows)
            {
                var deliveryData = new VW_DELIVERY_DATA_INCL_IMO();
                deliveryData.FillObjectByDataRow(row);
                sapDataList.Add(deliveryData);
            }
            return sapDataList;
        }

        public static List<VW_DELIVERY_DATA_INCL_IMO> GetDeliveries(List<String> deliverNumberFilter = null, List<String> batchNumberFilter = null, DateTime? minShipmentDate = null)
        {
            var deliveryFilter = String.Empty;
            if (deliverNumberFilter != null && deliverNumberFilter.Count == 1)
                deliveryFilter = $"and delivery_number = '{deliverNumberFilter[0]}' ";
            else if (deliverNumberFilter != null && deliverNumberFilter.Count > 0)
                deliveryFilter = $"and delivery_number in ('{String.Join("', '", deliverNumberFilter)}') ";

            var batchFilter = String.Empty;
            if (batchNumberFilter != null && batchNumberFilter.Count == 1)
                batchFilter = $"and batch_number = '{batchNumberFilter[0]}' ";
            else if (batchNumberFilter != null && batchNumberFilter.Count > 0)
                batchFilter = $"and batch_number in ('{String.Join("', '", batchNumberFilter)}') ";

            var dateFilter = String.Empty;
            if (minShipmentDate != null)
                dateFilter = $"and del.shipment_date > '{((DateTime)minShipmentDate).ToString("yyyy-MM-dd HH:mm:ss")}' ";

            //read deliveries of last 30 days from snowflake
            var sqlStatement = $@"select 
del.*, 
handling.HANDLING_UNIT_ID,
handling.HANDLING_UNIT_CONTENT_IMO_CHECK_CODE
from
""DW"".""EVIYOS"".""VW_DELIVERY_DATA"" del
left join
""DW"".""EVIYOS"".""VW_HANDLING_UNIT"" handling
on 
    del.DELIVERY_NUMBER = handling.SALES_DOCUMENT_ID_PACKING_OBJECT AND
    del.MATERIAL_NUMBER_11 = handling.HANDLING_UNIT_CONTENT_MATERIAL_ID AND
    del.BATCH_NUMBER = handling.HANDLING_UNIT_CONTENT_BATCH_ID
where
shipment_date between dateadd(days, -270, CURRENT_DATE) and CURRENT_DATE and
MATERIAL_NUMBER_11 in ('{String.Join("', '", validMaterial)}') and
amount_in_batch > 0
{deliveryFilter}
{batchFilter}
{dateFilter}
order by delivery_number, batch_number";

            var sapDataTable = SnowflakeDBConnection.GetData(sqlStatement);

            var sapDataList = new List<VW_DELIVERY_DATA_INCL_IMO>();
            foreach (DataRow row in sapDataTable.Rows)
            {
                var deliveryData = new VW_DELIVERY_DATA_INCL_IMO();
                deliveryData.FillObjectByDataRow(row);
                sapDataList.Add(deliveryData);
            }
            return sapDataList;
        }
    }
}
