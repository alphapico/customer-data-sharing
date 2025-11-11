using Microsoft.Office.Interop.Excel;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Windows;

namespace IMSE5515ExcelIntegration
{
    static class ExcelHelper
    {
        internal static Dictionary<string, string> LoadCellValues(Range selectedCells, bool mustContainValue = true)
        {
            var result = new Dictionary<string, string>();

            foreach (Range cell in selectedCells)
            {
                string value = null;

                if (mustContainValue)
                {
                    object valueToSearch = cell.get_Value(Type.Missing);
                    if (valueToSearch == null)
                        continue;

                    value = valueToSearch.ToString();
                    if (String.IsNullOrEmpty(value))
                        continue;

                    value = value.Trim();
                    if (value.Length == 0)
                        continue;
                }

                result[cell.get_Address(Type.Missing)] = value;
            }

            return result;
        }

        internal static void WriteExcelValues(List<IMSE5515VM.CellValue> cellValues, Microsoft.Office.Interop.Excel.Application app)
        {
            try
            {
                app.Range["A1", "Z9999"].Clear();

                foreach (var cellValue in cellValues)
                {
                    string changedCell = cellValue.CellReference; //$A$1

                    int startColumn = 0;

                    var range = app.get_Range(
                        GetCell(changedCell, 0, startColumn),
                        GetCell(changedCell, 0, startColumn + cellValue.Result.Length - 1)
                    );
                    range.Value2 = cellValue.Result;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private static String GetCell(String originalCell, int addRows, int addColumns)
        {
            originalCell = originalCell.Replace("$", "");
            var column = Regex.Replace(originalCell, @"[\d]", String.Empty);
            var row = Regex.Replace(originalCell, @"[^\d]", String.Empty);

            int colId = ExcelColumnNameToNumber(column);
            int rowId = Convert.ToInt32(row);

            int newColId = colId + addColumns;
            int newRowId = rowId + addRows;

            return GetExcelColumnName(newColId) + newRowId;
        }

        private static string GetExcelColumnName(int columnNumber)
        {
            int dividend = columnNumber;
            string columnName = String.Empty;
            int modulo;

            while (dividend > 0)
            {
                modulo = (dividend - 1) % 26;
                columnName = Convert.ToChar(65 + modulo).ToString() + columnName;
                dividend = (int)((dividend - modulo) / 26);
            }

            return columnName;
        }

        private static int ExcelColumnNameToNumber(string columnName)
        {
            if (string.IsNullOrEmpty(columnName)) throw new ArgumentNullException("columnName");
            columnName = columnName.ToUpperInvariant();
            int sum = 0;

            for (int i = 0; i < columnName.Length; i++)
            {
                sum *= 26;
                sum += (columnName[i] - 'A' + 1);
            }

            return sum;
        }
    }
}