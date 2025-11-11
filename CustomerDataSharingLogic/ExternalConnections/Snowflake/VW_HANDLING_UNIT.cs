using CustomerDataSharingLogic.Helpers;
using System.Collections.Generic;
using System.Data;

namespace CustomerDataSharingLogic.ExternalConnections.Snowflake
{
    public class VW_HANDLING_UNIT : BaseDBClass
    {
        public string HANDLING_UNIT_CONTENT { get; set; }
        public string SALES_DOCUMENT_ID_PACKING_OBJECT { get; set; } //link with DELIVERY_NUMBER

        public static List<VW_HANDLING_UNIT> GetList(DataTable sapData)
        {
            var sapDataList = new List<VW_HANDLING_UNIT>();
            foreach (DataRow row in sapData.Rows)
            {
                var handlingUnit = new VW_HANDLING_UNIT();
                FillObjectByDataRow(row, handlingUnit);
                sapDataList.Add(handlingUnit);
            }
            return sapDataList;
        }
    }
}
