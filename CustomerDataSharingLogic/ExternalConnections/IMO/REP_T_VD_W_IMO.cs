using osram.OSAS.Database.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace CustomerDataSharingLogic.ExternalConnections.IMO
{
    public class REP_T_VD_W_IMO : BaseBusinessObject
    {
        public String WI_BATCH_NUM { get; set; }
        public String WI_MATERIAL_NUM { get; set; }
        public String WI_IN_CHECK_CODE { get; set; }
        public String WI_MID_CHECK_CODE { get; set; }
        public String WI_OUT_CHECK_CODE { get; set; }
        public String ReelID { get { return WI_BATCH_NUM + WI_IN_CHECK_CODE; } }


        public static List<REP_T_VD_W_IMO> GetList(DataTable sapData)
        {
            var resultList = new List<REP_T_VD_W_IMO>();
            foreach (DataRow row in sapData.Rows)
            {
                var handlingUnit = new REP_T_VD_W_IMO();
                handlingUnit.FillObjectByDataRow(row);
                resultList.Add(handlingUnit);
            }
            return resultList;
        }

        private static bool imoInitialized;
        public static void GetIMOReelData()
        {
            if (!imoInitialized)
            {
                ImoDBConnection.EstablishConnection();
                imoInitialized = true;
            }

            var dmcDataTable = ImoDBConnection.GetData($"select count(*) from rep_t_vd_w_imo where wi_verified_date = '20231206'");
            Console.WriteLine();
        }

        public static List<REP_T_VD_W_IMO> GetIMOData(string material, string batchNumber, List<string> checkInCodes)
        {
            if (!imoInitialized)
            {
                ImoDBConnection.EstablishConnection();
                imoInitialized = true;
            }

            var imoStatement = $"select " +
                $"  WI_BATCH_NUM, WI_MATERIAL_NUM, WI_IN_CHECK_CODE, WI_MID_CHECK_CODE, WI_OUT_CHECK_CODE " +
                $"from " +
                $"  rep_t_vd_w_imo " +
                $"where " +
                $"  WI_MATERIAL_NUM = '{material}' AND " +
                $"  WI_BATCH_NUM = '{batchNumber}' AND " +
                $"  (WI_MID_CHECK_CODE in ({String.Join(",", checkInCodes.Select(i => $"'{i}'"))}) OR WI_OUT_CHECK_CODE in ({String.Join(",", checkInCodes.Select(i => $"'{i}'"))}))";
            //$"  WI_IN_CHECK_CODE in ({String.Join(",", checkInCodes.Select(i => $"'{i}'"))})";
            var imoDataTable = ImoDBConnection.GetData(imoStatement);
            var imoData = REP_T_VD_W_IMO.GetList(imoDataTable);

            return imoData;
        }
    }
}
