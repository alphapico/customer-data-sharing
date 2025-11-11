using osram.OSAS.Database.Framework;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices;

namespace CustomerDataSharingLogic.ExternalConnections.TMDB
{
    public class REP_D_VJ_REEL_DATALOG : BaseBusinessObject
    {
        // Declare the function from the DLL
        [DllImport("Math_Color_64.dll", CallingConvention = CallingConvention.StdCall)]
        public static extern int xy_to_LdomPurity(
            double x_coord, double y_coord,
            ref double Ldom, ref double Purity);

        public static Dictionary<string, string> fixReelOperations = new Dictionary<string, string>()
        {
            {"1015838616001", "7398" },
            {"1015838616002", "7398" },
            {"1015838616003", "7398" },
            {"1015838616004", "7398" },
            {"1015838613001", "7398" },
            {"1015842004001", "7398" },
            {"1015833738001", "7398" },
            {"1015833739001", "7398" },
            {"1016047388006", "8444" },
            {"1016047388014", "8444" },
            {"1016047388004", "8444" },
            {"1016047388017", "8444" },
            {"1016047388003", "8444" },
            {"1016030173001", "8444" },
            {"1016030173006", "8444" },
            {"1016030173005", "8444" },
            {"1016030173002", "8444" },
            {"1016047382003", "8444" },
            {"1016047382001", "8444" },
            {"1016047385003", "8444" },
            {"1016047385004", "8444" },
            {"1016047385001", "8444" },
            {"1016047385002", "8444" },
            {"1016047388018", "8444" },
            {"1016047388020", "8444" },
            {"1016047388015", "8444" }
        };

        public String DMC { get; set; }
        public String LOT_CALWEEK { get; set; }
        public int REEL_SEQ_POS { get; set; }
        public double CX_BLUE_20 { get; set; }
        public double CX_BLUE_50 { get; set; }
        public double CX_GREEN_20 { get; set; }
        public double CX_GREEN_50 { get; set; }
        public double CX_RED_20 { get; set; }
        public double CX_RED_50 { get; set; }
        public double CY_BLUE_20 { get; set; }
        public double CY_BLUE_50 { get; set; }
        public double CY_GREEN_20 { get; set; }
        public double CY_GREEN_50 { get; set; }
        public double CY_RED_20 { get; set; }
        public double CY_RED_50 { get; set; }
        public double IV_BLUE_20 { get; set; }
        public double IV_BLUE_50 { get; set; }
        public double IV_GREEN_20 { get; set; }
        public double IV_GREEN_50 { get; set; }
        public double IV_RED_20 { get; set; }
        public double IV_RED_50 { get; set; }
        public double UF_BLUE_20 { get; set; }
        public double UF_BLUE_50 { get; set; }
        public double UF_GREEN_20 { get; set; }
        public double UF_GREEN_50 { get; set; }
        public double UF_RED_20 { get; set; }
        public double UF_RED_50 { get; set; }
        public object REEL_ID { get; set; }
        public int OPERATION_NUMBER { get; set; }
        public DateTime MEAS_TIME { get; set; }

        public double LdomBlue20 { get { return GetLdomFromXY(CX_BLUE_20, CY_BLUE_20); } }
        public double LdomBlue50 { get { return GetLdomFromXY(CX_BLUE_50, CY_BLUE_50); } }
        public double LdomGreen20 { get { return GetLdomFromXY(CX_GREEN_20, CY_GREEN_20); } }
        public double LdomGreen50 { get { return GetLdomFromXY(CX_GREEN_50, CY_GREEN_50); } }
        public double LdomRed20 { get { return GetLdomFromXY(CX_RED_20, CY_RED_20); } }
        public double LdomRed50 { get { return GetLdomFromXY(CX_RED_50, CY_RED_50); } }

        public double BXBlue20 { get { return IV_BLUE_20 * CX_BLUE_20 / CY_BLUE_20; } }
        public double BXBlue50 { get { return IV_BLUE_50 * CX_BLUE_50 / CY_BLUE_50; } }
        public double BZBlue20 { get { return IV_BLUE_20 * (1 - CX_BLUE_20 - CY_BLUE_20) / CY_BLUE_20; } }
        public double BZBlue50 { get { return IV_BLUE_50 * (1 - CX_BLUE_50 - CY_BLUE_50) / CY_BLUE_50; } }
        public double BXGreen20 { get { return IV_GREEN_20 * CX_GREEN_20 / CY_GREEN_20; } }
        public double BXGreen50 { get { return IV_GREEN_50 * CX_GREEN_50 / CY_GREEN_50; } }
        public double BZGreen20 { get { return IV_GREEN_20 * (1 - CX_GREEN_20 - CY_GREEN_20) / CY_GREEN_20; } }
        public double BZGreen50 { get { return IV_GREEN_50 * (1 - CX_GREEN_50 - CY_GREEN_50) / CY_GREEN_50; } }
        public double BXRed20 { get { return IV_RED_20 * CX_RED_20 / CY_RED_20; } }
        public double BXRed50 { get { return IV_RED_50 * CX_RED_50 / CY_RED_50; } }
        public double BZRed20 { get { return IV_RED_20 * (1 - CX_RED_20 - CY_RED_20) / CY_RED_20; } }
        public double BZRed50 { get { return IV_RED_50 * (1 - CX_RED_50 - CY_RED_50) / CY_RED_50; } }

        public double BXaBlue20 { get { return BXauBlue20 + BXavBlue20 * LdomBlue20 + BXawBlue20 * UF_BLUE_20; } }
        public double BXbBlue20 { get { return BXbuBlue20 + BXbvBlue20 * LdomBlue20 + BXbwBlue20 * UF_BLUE_20; } }
        public double BYaBlue20 { get { return BYauBlue20 + BYavBlue20 * LdomBlue20 + BYawBlue20 * UF_BLUE_20; } }
        public double BYbBlue20 { get { return BYbuBlue20 + BYbvBlue20 * LdomBlue20 + BYbwBlue20 * UF_BLUE_20; } }
        public double BZaBlue20 { get { return BZauBlue20 + BZavBlue20 * LdomBlue20 + BZawBlue20 * UF_BLUE_20; } }
        public double BZbBlue20 { get { return BZbuBlue20 + BZbvBlue20 * LdomBlue20 + BZbwBlue20 * UF_BLUE_20; } }
        public double BTaBlue20 { get { return BTauBlue20 + BTavBlue20 * LdomBlue20 + BTawBlue20 * UF_BLUE_20; } }
        public double BTbBlue20 { get { return BTbuBlue20 + BTbvBlue20 * LdomBlue20 + BTbwBlue20 * UF_BLUE_20; } }

        public double GXaGreen20 { get { return GXauGreen20 + GXavGreen20 * LdomGreen20 + GXawGreen20 * UF_GREEN_20; } }
        public double GXbGreen20 { get { return GXbuGreen20 + GXbvGreen20 * LdomGreen20 + GXbwGreen20 * UF_GREEN_20; } }
        public double GYaGreen20 { get { return GYauGreen20 + GYavGreen20 * LdomGreen20 + GYawGreen20 * UF_GREEN_20; } }
        public double GYbGreen20 { get { return GYbuGreen20 + GYbvGreen20 * LdomGreen20 + GYbwGreen20 * UF_GREEN_20; } }
        public double GZaGreen20 { get { return GZauGreen20 + GZavGreen20 * LdomGreen20 + GZawGreen20 * UF_GREEN_20; } }
        public double GZbGreen20 { get { return GZbuGreen20 + GZbvGreen20 * LdomGreen20 + GZbwGreen20 * UF_GREEN_20; } }
        public double GTaGreen20 { get { return GTauGreen20 + GTavGreen20 * LdomGreen20 + GTawGreen20 * UF_GREEN_20; } }
        public double GTbGreen20 { get { return GTbuGreen20 + GTbvGreen20 * LdomGreen20 + GTbwGreen20 * UF_GREEN_20; } }

        public double RXaRed20 { get { return RXauRed20 + RXavRed20 * LdomRed20 + RXawRed20 * UF_RED_20; } }
        public double RXbRed20 { get { return RXbuRed20 + RXbvRed20 * LdomRed20 + RXbwRed20 * UF_RED_20; } }
        public double RYaRed20 { get { return RYauRed20 + RYavRed20 * LdomRed20 + RYawRed20 * UF_RED_20; } }
        public double RYbRed20 { get { return RYbuRed20 + RYbvRed20 * LdomRed20 + RYbwRed20 * UF_RED_20; } }
        public double RZaRed20 { get { return RZauRed20 + RZavRed20 * LdomRed20 + RZawRed20 * UF_RED_20; } }
        public double RZbRed20 { get { return RZbuRed20 + RZbvRed20 * LdomRed20 + RZbwRed20 * UF_RED_20; } }
        public double RTaRed20 { get { return RTauRed20 + RTavRed20 * LdomRed20 + RTawRed20 * UF_RED_20; } }
        public double RTbRed20 { get { return RTbuRed20 + RTbvRed20 * LdomRed20 + RTbwRed20 * UF_RED_20; } }

        public double BXauBlue20 { get { return -5.391; } }
        public double BXavBlue20 { get { return 0.01117; } }
        public double BXawBlue20 { get { return 0.4103; } }
        public double BXbuBlue20 { get { return -85.35; } }
        public double BXbvBlue20 { get { return 0.076; } }
        public double BXbwBlue20 { get { return 17.06; } }
        public double BYauBlue20 { get { return 2.834; } }
        public double BYavBlue20 { get { return -0.01483; } }
        public double BYawBlue20 { get { return 1.25; } }
        public double BYbuBlue20 { get { return -50.04; } }
        public double BYbvBlue20 { get { return 0.08776; } }
        public double BYbwBlue20 { get { return 3.564; } }
        public double BZauBlue20 { get { return -4.759; } }
        public double BZavBlue20 { get { return 0.009889; } }
        public double BZawBlue20 { get { return 0.3412; } }
        public double BZbuBlue20 { get { return -80.54; } }
        public double BZbvBlue20 { get { return 0.07631; } }
        public double BZbwBlue20 { get { return 15.35; } }
        public double BTauBlue20 { get { return -7766; } }
        public double BTavBlue20 { get { return 6.079; } }
        public double BTawBlue20 { get { return 1592; } }
        public double BTbuBlue20 { get { return 19920; } }
        public double BTbvBlue20 { get { return -24.28; } }
        public double BTbwBlue20 { get { return -2874; } }

        public double GXauGreen20 { get { return -23.54; } }
        public double GXavGreen20 { get { return 0.04131; } }
        public double GXawGreen20 { get { return 0.578; } }
        public double GXbuGreen20 { get { return 35.93; } }
        public double GXbvGreen20 { get { return -0.06252; } }
        public double GXbwGreen20 { get { return -1.001; } }
        public double GYauGreen20 { get { return -2.011; } }
        public double GYavGreen20 { get { return 0.01156; } }
        public double GYawGreen20 { get { return -1.322; } }
        public double GYbuGreen20 { get { return -23.87; } }
        public double GYbvGreen20 { get { return 0.0198; } }
        public double GYbwGreen20 { get { return 4.718; } }
        public double GZauGreen20 { get { return 14.53; } }
        public double GZavGreen20 { get { return -0.01594; } }
        public double GZawGreen20 { get { return -1.986; } }
        public double GZbuGreen20 { get { return -47.77; } }
        public double GZbvGreen20 { get { return 0.06129; } }
        public double GZbwGreen20 { get { return 5.388; } }
        public double GTauGreen20 { get { return -152.5; } }
        public double GTavGreen20 { get { return -3.115; } }
        public double GTawGreen20 { get { return 549.7; } }
        public double GTbuGreen20 { get { return 41680; } }
        public double GTbvGreen20 { get { return -57.79; } }
        public double GTbwGreen20 { get { return -3983; } }

        public double RXauRed20 { get { return 48.01; } }
        public double RXavRed20 { get { return -0.04781; } }
        public double RXawRed20 { get { return -7.142; } }
        public double RXbuRed20 { get { return -36.61; } }
        public double RXbvRed20 { get { return 0.08016; } }
        public double RXbwRed20 { get { return -8.336; } }
        public double RYauRed20 { get { return 40.33; } }
        public double RYavRed20 { get { return -0.04628; } }
        public double RYawRed20 { get { return -3.959; } }
        public double RYbuRed20 { get { return -61.05; } }
        public double RYbvRed20 { get { return 0.08393; } }
        public double RYbwRed20 { get { return 2.126; } }
        public double RZauRed20 { get { return -275.7; } }
        public double RZavRed20 { get { return 0.4662; } }
        public double RZawRed20 { get { return -7.776; } }
        public double RZbuRed20 { get { return 374.8; } }
        public double RZbvRed20 { get { return -0.8863; } }
        public double RZbwRed20 { get { return 82.44; } }
        public double RTauRed20 { get { return -5206; } }
        public double RTavRed20 { get { return 4.97; } }
        public double RTawRed20 { get { return 763.6; } }
        public double RTbuRed20 { get { return 39120; } }
        public double RTbvRed20 { get { return -41.44; } }
        public double RTbwRed20 { get { return -5707; } }

        private static double GetLdomFromXY(double x, double y)
        {
            double ldom = 0;
            double purity = 0;
            int result = xy_to_LdomPurity(x, y, ref ldom, ref purity);
            return Math.Round(ldom, 4);
        }

        public static List<REP_D_VJ_REEL_DATALOG> GetTmdbMeasurementsInRange(String startDMC, String endDMC)
        {
            var dataStatement = $"select distinct dmc, reel_id, REEL_SEQ_POS from (" +
                    $"select dmc, reel_id, REEL_SEQ_POS, RANK() OVER(PARTITION BY dmc ORDER BY meas_time DESC) AS rnk FROM(" +
                        $"select dmc, reel_id, REEL_SEQ_POS, meas_time from REP_D_VJ_REEL_DATALOG where dmc in ('{startDMC}', '{endDMC}') and operation_number in ('7407', '7408', '7409', '7389', '8444', '7390')" +
                    $")" +
                $") where rnk = 1";
            
            var dataTable = TmdbDBConnection.GetData(dataStatement);
            if (dataTable.Rows.Count != 2)
                throw new Exception("database does not contain an entry for the requested DMC, operation 7407 / 7408 / 7409 / 7389 / 8444 / 7390");
            string reelId = null;
            int minPos = 0;
            int maxPos = 0;

            foreach (DataRow row in dataTable.Rows)
            {
                var dmc = row["DMC"]?.ToString();
                var reel = row["REEL_ID"]?.ToString();
                var pos = (int)row["REEL_SEQ_POS"];

                if (reelId == null)
                    reelId = reel;
                else if (reelId != reel)
                    throw new Exception("DMCs are contained in different reels");

                if (dmc == startDMC)
                    minPos = pos;
                else if (dmc == endDMC)
                    maxPos = pos;
            }

            var result = GetTmdbMeasurements(reelId, startPos: minPos, endPos: maxPos);
            //, "7398"
            result.ForEach(r => r.REEL_ID = reelId);
            return result;
        }

        public static List<REP_D_VJ_REEL_DATALOG> GetTmdbMeasurements(string reelId, int startPos = Int32.MinValue, int endPos = Int32.MaxValue)
        {
            //var dmcDataTable = TmdbDBConnection.GetData($"select distinct dmc, reel_seq_pos from REP_D_VJ_REEL_DATALOG where REEL_ID = '{reelId}' and operation_number in ('7407', '7408')");
            //var dmcList = BaseDBHelper.GetList<DMCItem>(dmcDataTable);
            //'{reelId}'
            //var dmcString = "'" + String.Join("','", dmcList.Select(d => d.DMC)) + "'";

            //var defaultOperation = new String[] { "7310", "7398" };
            //if (measurementOperation != null)
            //    defaultOperation = new String[] { measurementOperation };
            //else if (fixReelOperations.ContainsKey(reelId))
            //    defaultOperation = new String[] { fixReelOperations[reelId] };

            var posFilter = String.Empty;
            if (startPos != Int32.MinValue)
                posFilter += " AND REEL_SEQ_POS >= " + startPos;
            if (endPos != Int32.MaxValue)
                posFilter += " AND REEL_SEQ_POS <= " + endPos;

            var dataStatement = $@"SELECT * FROM 
(
    SELECT * FROM (
        SELECT a.dmc, a.REEL_SEQ_POS, DMEAS_VALUE, LOT_CALWEEK, parameter_name, operation_number, meas_time, RANK() OVER(PARTITION BY a.dmc, operation_number, parameter_name ORDER BY meas_time DESC) AS rnk FROM (
        (select distinct dmc, REEL_SEQ_POS from REP_D_VJ_REEL_DATALOG where reel_id = :reelId and operation_number in ('7407', '7408', '7409', '7389', '8444', '7390', '7740')) a
        inner join
        (select dmc, REEL_SEQ_POS, DMEAS_VALUE, LOT_CALWEEK, parameter_name, meas_time, operation_number from REP_D_VJ_REEL_DATALOG where operation_number in ('7310', '7398', '7385', '7388', '8444') and reel_id is null and test_flag = 'P') b
        on a.dmc = b.dmc)
    ) where rnk = 1 {posFilter}
)
PIVOT(
    AVG(DMEAS_VALUE) 
    FOR parameter_name
    IN ( 
        'FT1_PT1.Site1.BS.x' as CX_BLUE_20,
        'FT1_PT1.Site1.BS50.x' as CX_BLUE_50,
        'FT1_PT1.Site1.TS.x' as CX_GREEN_20,
        'FT1_PT1.Site1.TS50.x' as CX_GREEN_50,
        'FT1_PT1.Site1.RS.x' as CX_RED_20,
        'FT1_PT1.Site1.RS50.x' as CX_RED_50,
        'FT1_PT1.Site1.BS.y' as CY_BLUE_20,
        'FT1_PT1.Site1.BS50.y' as CY_BLUE_50,
        'FT1_PT1.Site1.TS.y' as CY_GREEN_20,
        'FT1_PT1.Site1.TS50.y' as CY_GREEN_50,
        'FT1_PT1.Site1.RS.y' as CY_RED_20,
        'FT1_PT1.Site1.RS50.y' as CY_RED_50,
        'FT1_PT1.Site1.BS.Iv' as IV_BLUE_20,
        'FT1_PT1.Site1.BS50.Iv' as IV_BLUE_50,
        'FT1_PT1.Site1.TS.Iv' as IV_GREEN_20,
        'FT1_PT1.Site1.TS50.Iv' as IV_GREEN_50,
        'FT1_PT1.Site1.RS.Iv' as IV_RED_20,
        'FT1_PT1.Site1.RS50.Iv' as IV_RED_50,
        'FT1_PT1.SMU_3.VF_20mA' as UF_BLUE_20,
        'FT1_PT1.SMU_3.VF_50mA' as UF_BLUE_50,
        'FT1_PT1.SMU_2.VF_20mA' as UF_GREEN_20,
        'FT1_PT1.SMU_2.VF_50mA' as UF_GREEN_50,
        'FT1_PT1.SMU_1.VF_20mA' as UF_RED_20,
        'FT1_PT1.SMU_1.VF_50mA' as UF_RED_50
    )
)";

            if(reelId == "1016622395001" || reelId == "1016622395002" || reelId == "1016622395003" || reelId == "1016667270001" || reelId == "1016681055003")
                dataStatement = $@"SELECT * FROM 
(
    SELECT * FROM (
        SELECT a.dmc, a.REEL_SEQ_POS, DMEAS_VALUE, LOT_CALWEEK, parameter_name, operation_number, meas_time, RANK() OVER(PARTITION BY a.dmc, operation_number, parameter_name ORDER BY meas_time DESC) AS rnk FROM (
        (select distinct dmc, REEL_SEQ_POS from (
SELECT REGEXP_REPLACE (l.lot_number, '[.]', '')       ""LOT_LOT_NUMBER"",
           m.FOIL_BARCODE                                 ""REEL_ID"",
           d.device_dmc                                   ""DMC"",
           dm.ft_fc                                       ""DMEAS_FT_FC"",
           dm.p_param_id                                  ""PAR_PARAM_ID"",
           dm.VALUE                                       ""DMEAS_VALUE"",
           dm.chip                                        ""DMEAS_CHIP"",
           mp.param_color                                 ""COL_COLOR"",
           TO_CHAR (l.END_DATE, 'YYYYMMDD')               ""LOT_ENDDATE"",
           LPAD (l.year, 4, 0) || LPAD (l.week, 2, 0)     ""LOT_CALWEEK"",
           MOD (CONCAT (l.year, l.week), 4)               ""MOD_CALWEEK"",
           m.record                                       ""MEAS_RECORD"",
           m.x_pos_frame                                  ""REEL_SEQ_POS"",
           dm.test_seq                                    ""DMEAS_RECNUM"",
           l.material_number                              ""LOT_MAT_NUMBER"",
           mp.param_name                                  ""PARAMETER_NAME"",
           mp.PARAM_GROUP                                 ""PARAM_GROUP"",
           pp.PARAM_BIAS                                  ""PARAM_BIAS"",
           M.operation_number                             ""OPERATION_NUMBER"",
           m.MEAS_DATE                                     ""MEAS_TIME"",
           m.TEST_FLAG										""TEST_FLAG""
      FROM tmdb_data.device  d
           JOIN tmdb_data.lot l ON l.lot_id = d.lot_id
           JOIN tmdb_data.measurement m ON m.device_id = d.device_id
           JOIN tmdb_data.device_measurement dm ON dm.meas_id = m.meas_id
           JOIN tmdb_data.p_parameter pp ON pp.p_param_id = dm.p_param_id
           JOIN TMDB_DATA.MM_PARAMETER mp
               ON UPPER (mp.PARAM_NAME) = UPPER (pp.PARAM_NAME)
     WHERE 
           d.device_dmc <> 'NO ID'
           --  AND l.lot_number in ('TJF4603P','TJF4603P01')
           --   AND l.material_number IN (11131069,
           --                           11118237,
           --                           11137168,
           --                          11137166)
           AND mp.PARAM_COLOR != 'NA'
           AND mp.PARAM_COLOR IS NOT NULL
           AND TO_CHAR (l.rec_date, 'yyyymmdd') >= '20231020'
 ) where reel_id = :reelId and operation_number in ('7407', '7408', '7409', '7389', '8444', '7390', '7740')) a
        inner join
        (select dmc, REEL_SEQ_POS, DMEAS_VALUE, LOT_CALWEEK, parameter_name, meas_time, operation_number from REP_D_VJ_REEL_DATALOG where operation_number in ('7310', '7398', '7385', '7388', '8444') and reel_id is null and test_flag = 'P') b
        on a.dmc = b.dmc)
    ) where rnk = 1 {posFilter}
)
PIVOT(
    AVG(DMEAS_VALUE) 
    FOR parameter_name
    IN ( 
        'FT1_PT1.Site1.BS.x' as CX_BLUE_20,
        'FT1_PT1.Site1.BS50.x' as CX_BLUE_50,
        'FT1_PT1.Site1.TS.x' as CX_GREEN_20,
        'FT1_PT1.Site1.TS50.x' as CX_GREEN_50,
        'FT1_PT1.Site1.RS.x' as CX_RED_20,
        'FT1_PT1.Site1.RS50.x' as CX_RED_50,
        'FT1_PT1.Site1.BS.y' as CY_BLUE_20,
        'FT1_PT1.Site1.BS50.y' as CY_BLUE_50,
        'FT1_PT1.Site1.TS.y' as CY_GREEN_20,
        'FT1_PT1.Site1.TS50.y' as CY_GREEN_50,
        'FT1_PT1.Site1.RS.y' as CY_RED_20,
        'FT1_PT1.Site1.RS50.y' as CY_RED_50,
        'FT1_PT1.Site1.BS.Iv' as IV_BLUE_20,
        'FT1_PT1.Site1.BS50.Iv' as IV_BLUE_50,
        'FT1_PT1.Site1.TS.Iv' as IV_GREEN_20,
        'FT1_PT1.Site1.TS50.Iv' as IV_GREEN_50,
        'FT1_PT1.Site1.RS.Iv' as IV_RED_20,
        'FT1_PT1.Site1.RS50.Iv' as IV_RED_50,
        'FT1_PT1.SMU_3.VF_20mA' as UF_BLUE_20,
        'FT1_PT1.SMU_3.VF_50mA' as UF_BLUE_50,
        'FT1_PT1.SMU_2.VF_20mA' as UF_GREEN_20,
        'FT1_PT1.SMU_2.VF_50mA' as UF_GREEN_50,
        'FT1_PT1.SMU_1.VF_20mA' as UF_RED_20,
        'FT1_PT1.SMU_1.VF_50mA' as UF_RED_50
    )
)";

            var dataTable = TmdbDBConnection.GetData(dataStatement, new Dictionary<string, string>() { { "reelId", reelId } });
            var data = Helpers.BaseDBHelper.GetList<REP_D_VJ_REEL_DATALOG>(dataTable).OrderBy(d => d.REEL_SEQ_POS).ThenBy(d => d.MEAS_TIME).ToList();
            //var dmcReelSeqMapping = dmcList.ToDictionary(d => d.DMC, d => d.REEL_SEQ_POS);
            //data.ForEach(d => d.REEL_SEQ_POS = dmcReelSeqMapping[d.DMC]);
            //data = data.OrderBy(d => d.REEL_SEQ_POS).ToList();

            //Remove all entries without the latest meas_time
            var dataNot8444 = data.Where(d => d.OPERATION_NUMBER != 8444).ToList();
            var reelSeqPos = dataNot8444.Select(d => d.REEL_SEQ_POS).Distinct().OrderBy(d => d).ToList();

            foreach(var reelPos in reelSeqPos)
            {
                var dataOfPos = dataNot8444.Where(d => d.REEL_SEQ_POS == reelPos).ToList();
                if (dataOfPos.Count == 1)
                    continue;
                var distinctDMC = dataOfPos.Select(p => p.DMC).ToList();
                foreach(var dmc in distinctDMC) //should ideally be just one
                {
                    var dataOfPosDMC = dataOfPos.Where(d => d.DMC == dmc).ToList();
                    var latestMeas = dataOfPosDMC.Max(d => d.MEAS_TIME);
                    var entryWithLastMeas = dataOfPosDMC.FirstOrDefault(d => d.MEAS_TIME == latestMeas);
                    dataOfPosDMC.Remove(entryWithLastMeas);
                    data.RemoveAll(d => dataOfPosDMC.Contains(d));

                }
            }

            //for (int i = 1; i < data.Count; i++) //loop dmc and remove entries with same dmc but different measurement times
            //{
            //    if (data[i].OPERATION_NUMBER == 8444)
            //        continue;

                //if (data[i - 1].DMC == data[i].DMC && data[i - 1].OPERATION_NUMBER == data[i].OPERATION_NUMBER)
                //{
                //    if (String.Compare(data[i - 1].LOT_CALWEEK, data[i].LOT_CALWEEK) < 0)
                //        data.RemoveAt(i - 1);
                //    else
                //        data.RemoveAt(i);
                //}
            //}

            return data;
        }

        public static List<REP_D_VJ_REEL_DATALOG> GetTmdbMeasurementsFromList(List<string> dmcList)
        {
            var dataStatement = $@"SELECT * FROM 
(
    SELECT * FROM (
        SELECT dmc, DMEAS_VALUE, LOT_CALWEEK, parameter_name, operation_number, meas_time, RANK() OVER(PARTITION BY dmc, operation_number, parameter_name ORDER BY meas_time DESC) AS rnk FROM (
        select dmc, REEL_SEQ_POS, DMEAS_VALUE, LOT_CALWEEK, parameter_name, meas_time, operation_number from REP_D_VJ_REEL_DATALOG where operation_number in ('7310', '7398', '7385', '7388', '8444') and reel_id is null and test_flag = 'P'
            and dmc in ('{string.Join("','", dmcList)}'))
    ) where rnk = 1
)
PIVOT(
    AVG(DMEAS_VALUE) 
    FOR parameter_name
    IN ( 
        'FT1_PT1.Site1.BS.x' as CX_BLUE_20,
        'FT1_PT1.Site1.BS50.x' as CX_BLUE_50,
        'FT1_PT1.Site1.TS.x' as CX_GREEN_20,
        'FT1_PT1.Site1.TS50.x' as CX_GREEN_50,
        'FT1_PT1.Site1.RS.x' as CX_RED_20,
        'FT1_PT1.Site1.RS50.x' as CX_RED_50,
        'FT1_PT1.Site1.BS.y' as CY_BLUE_20,
        'FT1_PT1.Site1.BS50.y' as CY_BLUE_50,
        'FT1_PT1.Site1.TS.y' as CY_GREEN_20,
        'FT1_PT1.Site1.TS50.y' as CY_GREEN_50,
        'FT1_PT1.Site1.RS.y' as CY_RED_20,
        'FT1_PT1.Site1.RS50.y' as CY_RED_50,
        'FT1_PT1.Site1.BS.Iv' as IV_BLUE_20,
        'FT1_PT1.Site1.BS50.Iv' as IV_BLUE_50,
        'FT1_PT1.Site1.TS.Iv' as IV_GREEN_20,
        'FT1_PT1.Site1.TS50.Iv' as IV_GREEN_50,
        'FT1_PT1.Site1.RS.Iv' as IV_RED_20,
        'FT1_PT1.Site1.RS50.Iv' as IV_RED_50,
        'FT1_PT1.SMU_3.VF_20mA' as UF_BLUE_20,
        'FT1_PT1.SMU_3.VF_50mA' as UF_BLUE_50,
        'FT1_PT1.SMU_2.VF_20mA' as UF_GREEN_20,
        'FT1_PT1.SMU_2.VF_50mA' as UF_GREEN_50,
        'FT1_PT1.SMU_1.VF_20mA' as UF_RED_20,
        'FT1_PT1.SMU_1.VF_50mA' as UF_RED_50
    )
)
            ";

            var dataTable = TmdbDBConnection.GetData(dataStatement);
            var data = Helpers.BaseDBHelper.GetList<REP_D_VJ_REEL_DATALOG>(dataTable).ToList();
            data.ForEach(d => d.REEL_SEQ_POS = dmcList.IndexOf(d.DMC) + 1);
            data = data.OrderBy(d => d.REEL_SEQ_POS).ThenBy(d => d.MEAS_TIME).ToList();

            //Remove all entries without the latest meas_time
            var dataNot8444 = data.Where(d => d.OPERATION_NUMBER != 8444).ToList();
            var reelSeqPos = dataNot8444.Select(d => d.REEL_SEQ_POS).Distinct().OrderBy(d => d).ToList();

            foreach (var reelPos in reelSeqPos)
            {
                var dataOfPos = dataNot8444.Where(d => d.REEL_SEQ_POS == reelPos).ToList();
                if (dataOfPos.Count == 1)
                    continue;
                var distinctDMC = dataOfPos.Select(p => p.DMC).ToList();
                foreach (var dmc in distinctDMC) //should ideally be just one
                {
                    var dataOfPosDMC = dataOfPos.Where(d => d.DMC == dmc).ToList();
                    var latestMeas = dataOfPosDMC.Max(d => d.MEAS_TIME);
                    var entryWithLastMeas = dataOfPosDMC.FirstOrDefault(d => d.MEAS_TIME == latestMeas);
                    dataOfPosDMC.Remove(entryWithLastMeas);
                    data.RemoveAll(d => dataOfPosDMC.Contains(d));
                }
            }

            return data;
        }
    }
}