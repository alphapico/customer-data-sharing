using CustomerDataSharingLogic.Helpers;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Diagnostics;

namespace CustomerDataSharingLogic.ExternalConnections.IMO
{
    public static class ImoDBConnection
    {
        private static OracleConnection imoConnection = null;

        public static DataTable GetData(string sqlString)
        {
            Debug.WriteLine(sqlString);
            DataSet ds = null;
            var da = new OracleDataAdapter(sqlString, imoConnection);

            ds = new DataSet();
            da.Fill(ds);

            return ds?.Tables[0];
        }

        public static bool EstablishConnection()
        {
            try
            {
                string connectionString = MyApplicationSettings.GetSetting("IMOConnectionString");
                string imoPw = MyApplicationSettings.GetSetting("IMOPassword", true);
                connectionString = connectionString.Replace("{password}", imoPw);

                imoConnection = new OracleConnection();
                imoConnection.ConnectionString = connectionString;

                var da = new OracleDataAdapter("select 3 as test from dual", imoConnection);

                var ds = new DataSet();
                da.Fill(ds);
                var result = ds?.Tables[0];
                if (result.Columns.Count == 1 && result.Rows.Count == 1 && result.Rows[0][0]?.ToString() == "3")
                    return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }
    }
}
