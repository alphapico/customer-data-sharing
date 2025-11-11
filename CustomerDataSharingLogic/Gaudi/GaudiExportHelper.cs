using CustomerDataSharingLogic.ExternalConnections.TMDB;
using osram.OSAS.Excel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace CustomerDataSharingLogic.Gaudi
{
    public static class GaudiExportHelper
    {
        public static void CreateCsvFile(String opulentFileName, List<V_TMDB_FT_OSBE18_TEST> measurementData)
        {
            var newFileName = GetOutputFileNameWithoutEnding(opulentFileName);
            var outputFileName = newFileName + ".csv";
            var fileContent = GetOutputContent(measurementData, newFileName);

            File.WriteAllLines(outputFileName, fileContent.Select(f => String.Join(",", f)));
        }

        public static byte[] CreateCsvFileContent(String opulentFileName, List<V_TMDB_FT_OSBE18_TEST> measurementData, out string newFileName)
        {
            newFileName = GetOutputFileNameWithoutEnding(opulentFileName);
            var fileContent = GetOutputContent(measurementData, newFileName);

            var result = String.Join(Environment.NewLine, fileContent.Select(f => String.Join(",", f)));
            return Encoding.UTF8.GetBytes(result);
        }

        private static string GetOutputFileNameWithoutEnding(String opulentFileName)
        {
            var fileNameParts = opulentFileName.Split(new char[] { '/', '_' });
            var newFileName = $"DV_{DateTime.Now.ToString("yyyyMMdd")}_{fileNameParts[2]}_{fileNameParts[3]}";
            //var newFileName = $"{fileNameParts[1]}_{fileNameParts[2]}_{fileNameParts[3]}";
            return newFileName;
        }

        private static List<object[]> GetOutputContent(List<V_TMDB_FT_OSBE18_TEST> measurementData, string newFileName)
        {
            List<object[]> fileContent = new List<object[]>();
            //add headers
            fileContent.Add(V_TMDB_FT_OSBE18_TEST.Columns.Select(c => c.Value).ToArray());
            //add content
            foreach (var measurement in measurementData)
            {
                var lineContent = new List<object>();
                foreach (var key in V_TMDB_FT_OSBE18_TEST.Columns.Keys)
                {
                    if (key == "-5") //5.filename
                        lineContent.Add(newFileName);
                    //lineContent.Add("EV2_Datasheet_20240624_0000000000");
                    else if (key == "-7") //7.2d_matrix_on_flex-->_scan
                        lineContent.Add(measurement.SERIAL_NUMBER);
                    else if (key == "-8") //8.2d_matrix_on_lid-->_scan
                        lineContent.Add(measurement.DEVICE_DMC);
                    else if (key == "-11") //11.module_config
                        lineContent.Add("DV");
                    else if (measurement.ColumnValues.ContainsKey(key))
                        lineContent.Add(measurement.ColumnValues[key]);
                    else
                        lineContent.Add(string.Empty);
                }
                fileContent.Add(lineContent.ToArray());
            }
            return fileContent;
        }

        public static void CreateExcelFile(String opulentFileName, List<V_TMDB_FT_OSBE18_TEST> measurementData)
        {
            var newFileName = GetOutputFileNameWithoutEnding(opulentFileName);
            var outputFileName = newFileName + ".xlsx";
            var fileContent = GetOutputContent(measurementData, newFileName);

            ExcelSheet sheet = new ExcelSheet()
            {
                Title = "HiLo",
                Values = fileContent.ToArray(),
                CreateTableFilter = false
            };
            ExcelCreator.CreateExcelFile(outputFileName, sheet);
        }
    }
}
