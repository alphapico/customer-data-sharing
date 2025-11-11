using CustomerDataSharingLogic.Helpers;
using Snowflake.Data.Client;
using System;
using System.Data;
using System.Diagnostics;

namespace CustomerDataSharingLogic.ExternalConnections.Snowflake
{
    public static class SnowflakeDBConnection
    {
        private static SnowflakeDbConnection snowflakeConnection = null;

        public static DataTable GetData(string sqlString)
        {
            Debug.WriteLine(sqlString);
            DataSet ds = null;
            var da = new SnowflakeDbDataAdapter(sqlString, snowflakeConnection);
            //da.SelectCommand.CommandTimeout = 300;

            ds = new DataSet();
            da.Fill(ds);

            return ds?.Tables[0];
        }

        public static void Execute(string sqlString)
        {
            Debug.WriteLine(sqlString);

            var command = new SnowflakeDbCommand()
            {
                CommandText = sqlString,
                Connection = snowflakeConnection
            };

            command.ExecuteNonQuery();
        }

        public static bool EstablishConnection()
        {
            try
            {
                string connectionString = MyApplicationSettings.GetSetting("SnowflakeConnectionString");
                string snowflakePw = MyApplicationSettings.GetSetting("SnowflakePassword", true);
                connectionString = connectionString.Replace("{password}", snowflakePw);

                snowflakeConnection = new SnowflakeDbConnection();
                snowflakeConnection.ConnectionString = connectionString;
                snowflakeConnection.Open();

                var da = new SnowflakeDbDataAdapter("select 3 as test from dual", snowflakeConnection);

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
