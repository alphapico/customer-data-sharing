using CustomerDataSharingLogic.Helpers;
using System;
using System.Collections.Generic;
using System.Data;

namespace CustomerDataSharingLogic.ExternalConnections.Snowflake
{
    public class Capella_MC300 : BaseDBClass
    {
        public string LOT_NAME { get; set; }
        public DateTime MEASUREMENT_DATE { get; set; }
        public string DEVICE_DMC { get; set; }
        public double FT1_P_30 { get; set; }
        public double FT1_WL_DOM { get; set; }
        public double FT1_VF_30 { get; set; }
        public double FT1_TILT_MAG_AVG { get; set; }

        public static List<Capella_MC300> GetData(string lot)
        {
            var dataTable = SnowflakeDBConnection.GetData(
                @"select
                    lot_name,
                    measurement_date,
                    device_dmc,
                    measurements:FT1_P_30 as FT1_P_30,
                    measurements:FT1_WL_dom as FT1_WL_DOM,
                    measurements:FT1_Vf_30 as FT1_VF_30,
                    measurements:FT1_Tilt_mag_avg as FT1_TILT_MAG_AVG
                from
                edl.raw_pdp.mc300_v2 where lot_name = '" + lot + "' and product = '11152696'");
            return GetList(dataTable);
        }

        private static List<Capella_MC300> GetList(DataTable data)
        {
            var result = new List<Capella_MC300>();
            foreach (DataRow row in data.Rows)
            {
                var mc300 = new Capella_MC300();
                FillObjectByDataRow(row, mc300);
                result.Add(mc300);
            }
            return result;
        }
    }
}