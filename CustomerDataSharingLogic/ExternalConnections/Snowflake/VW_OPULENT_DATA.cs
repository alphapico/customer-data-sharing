using CustomerDataSharingLogic.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text;

namespace CustomerDataSharingLogic.ExternalConnections.Snowflake
{
    public class VW_OPULENT_DATA : BaseDBClass
    {
        public string SERIAL_NUMBER { get; set; }
        public string U_BARCODE { get; set; }
        public string SUB_VARIANT { get; set; }
        public string SPIID { get; set; }
        public DateTime MANUFACTURING_DATE { get; set; }
        public DateTime DELIVERY_DATE { get; set; }
        public string METADATA_FILENAME { get; set; }
        public DateTime METADATA_FILENAME_DATE
        {
            get
            {
                if (String.IsNullOrEmpty(METADATA_FILENAME))
                    return DateTime.MinValue;
                //sample file name result: Opulent/11149258_CE00002198_001_20240520025256_4b53d5a0-1643-11ef-bc08-7dbf0a3e0164.json
                var fileNameParts = METADATA_FILENAME.Split(new char[] { '/', '_' });
                if (fileNameParts.Length < 6)
                    return DateTime.MinValue;

                if (DateTime.TryParseExact(fileNameParts[4], "yyyyMMddHHmmss", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
                { //successful
                }
                return result;
            }
        }

        public static List<VW_OPULENT_DATA> GetFiles()
        {
            var dataTable = SnowflakeDBConnection.GetData("select distinct METADATA_FILENAME from DW.EVIYOS.VW_OPULENT_DATA");
            return GetList(dataTable);
        }

        public static List<VW_OPULENT_DATA> GetData(List<String> fileNames)
        {
            var dataTable = SnowflakeDBConnection.GetData($"select * from DW.EVIYOS.VW_OPULENT_DATA where METADATA_FILENAME in ('{String.Join("', '", fileNames)}')");
            return GetList(dataTable);
        }

        private static List<VW_OPULENT_DATA> GetList(DataTable data)
        {
            var result = new List<VW_OPULENT_DATA>();
            foreach (DataRow row in data.Rows)
            {
                var opulent = new VW_OPULENT_DATA();
                FillObjectByDataRow(row, opulent);
                result.Add(opulent);
            }
            return result;
        }
    }
}
