using CustomerDataSharingLogic.Helpers;
using System;
using System.Collections.Generic;
using System.Data;

namespace CustomerDataSharingLogic.ExternalConnections.Snowflake
{
    public class Capella_DTS : BaseDBClass
    {
        public string LOT_NAME { get; set; }
        public string TARGET_SUBSTRATE_ID { get; set; }
        public string DEVICE_ID { get; set; }
        public DateTime RECEIVE_TIME { get; set; }

        public static List<Capella_DTS> GetData()
        {
            var dataTable = SnowflakeDBConnection.GetData(
                $@"SELECT 
                  t.lot_name,
                  f.value:target_substrate_id AS target_substrate_id,
                  f.value:device_id AS device_id,
                  f.value:result AS result,
                FROM edl.raw_pdp.dts t,
                LATERAL FLATTEN(input => t.devices) f
                 where product = '11152696' and operation = '7409' and equipment = '1ATF0102' and receive_time > '2025-03-01' and result = 'PASS' order by device_id");
            return GetList(dataTable);
        }

        private static List<Capella_DTS> GetList(DataTable data)
        {
            var result = new List<Capella_DTS>();
            foreach (DataRow row in data.Rows)
            {
                var dts = new Capella_DTS();
                FillObjectByDataRow(row, dts);
                result.Add(dts);
            }
            return result;
        }
    }
}