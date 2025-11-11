using CustomerDataSharingLogic.Helpers;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;

namespace CustomerDataSharingLogic.ExternalConnections.TMDB
{
    public static class TmdbDBConnection
    {
        private static OracleConnection tmdbConnection = null;
        public static String Location { get; set; } = "WUX";
        private static bool tmdbInitialized;

        public static DataTable GetData(string sqlString, Dictionary<String, String> parameters = null)
        {
            if (!tmdbInitialized)
            {
                EstablishConnection();
                tmdbInitialized = true;
            }

            try
            {
                tmdbConnection.Open();
            }
            catch (Exception ex) { }
            OracleCommand cmd = new OracleCommand
            {
                Connection = tmdbConnection,
                CommandText = sqlString,
                CommandType = CommandType.Text
            };

            /*IMPORTANT: adding parameters must be in order how they are in order in the SQL statement*/
            if (parameters != null)
                foreach (var parameterKey in parameters.Keys)
                    cmd.Parameters.Add(new OracleParameter(parameterKey, OracleDbType.Varchar2, parameters[parameterKey], ParameterDirection.Input));

            OracleDataReader dr = cmd.ExecuteReader();
            DataTable dt = new DataTable();
            dt.Load(dr);

            return dt;
        }

        internal static void Execute(string dbStatement)
        {
            if (!tmdbInitialized)
            {
                EstablishConnection();
                tmdbInitialized = true;
            }

            DbCommand command = new OracleCommand();
            command.CommandText = dbStatement;
            command.Connection = tmdbConnection;
            command.ExecuteNonQuery();
        }

        public static void InsertMassData<T>(List<T> objects, String tableName)
        {
            if (!tmdbInitialized)
            {
                EstablishConnection();
                tmdbInitialized = true;
            }

            if (objects.Count == 0)
                return;
            Type objType = objects[0].GetType();


            //pass all properties of the class to create a default insert statement
            PropertyInfo[] properties = objType.GetProperties();
            Dictionary<string, object[]> dbParameters = new Dictionary<string, object[]>();
            //List<String> dbParameters = new List<String>();
            foreach (PropertyInfo property in properties)
            {
                dbParameters[property.Name] = objects.Select(o => property.GetValue(o, null)).ToArray();
            }

            string columns = string.Join(", ", dbParameters.Keys);
            string values = string.Join(", ", dbParameters.Keys.Select(k => ":" + k));

            String sqlStatement = "insert into " + tableName + "(" + columns + ") values (" + values + ")";

            //pass all objects and insert them into the databse
            try
            {
                OracleCommand cmd = new OracleCommand
                {
                    Connection = tmdbConnection,
                    CommandText = sqlStatement,
                    CommandType = CommandType.Text
                };
                OracleCommand command = (OracleCommand)tmdbConnection.CreateCommand();
                //command.BindByName = true;
                DbTransaction transaction = tmdbConnection.BeginTransaction();
                command.CommandText = sqlStatement;
                command.ArrayBindCount = objects.Count;

                foreach (String dbParameter in dbParameters.Keys)
                {
                    OracleParameter par = new OracleParameter
                    {
                        ParameterName = dbParameter
                    };
                    if (dbParameters[dbParameter][0].GetType() == typeof(string))
                        par.OracleDbType = OracleDbType.Varchar2;
                    else if (dbParameters[dbParameter][0].GetType() == typeof(DateTime))
                        par.OracleDbType = OracleDbType.Date;
                    else
                        par.OracleDbType = OracleDbType.Double;
                    par.Value = dbParameters[dbParameter];
                    command.Parameters.Add(par);
                }

                command.ExecuteNonQuery();

                transaction.Commit();
                command.Dispose();
            }
            catch (Exception e)
            {
                DbCommand command = new OracleCommand();
                command.CommandText = "rollback";
                command.Connection = tmdbConnection;
                command.ExecuteNonQuery();
                throw;
            }
        }

        public static bool EstablishConnection()
        {
            try
            {
                string connectionString = MyApplicationSettings.GetSetting("TMDBConnectionString" + Location);
                if (String.IsNullOrEmpty(connectionString))
                    connectionString = ConfigurationManager.AppSettings["TMDBConnectionString" + Location];
                string tmdbPw = MyApplicationSettings.GetSetting("TMDBPassword" + Location, true);
                if(String.IsNullOrEmpty(tmdbPw))
                    tmdbPw = MyApplicationSettings.DecryptSetting(ConfigurationManager.AppSettings["TMDBPassword" + Location]);
                connectionString = connectionString.Replace("{password}", tmdbPw);

                tmdbConnection = new OracleConnection();
                tmdbConnection.ConnectionString = connectionString;

                var da = new OracleDataAdapter("select 3 as test from dual", tmdbConnection);

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
