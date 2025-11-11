using CustomerDataSharingLogic.ExternalConnections.TMDB;
using CustomerDataSharingLogic.IMSE5515;
using osram.OSAS.Files;
using System.IO;
using System.Security.Policy;
using System.Text;
using System.Windows;
using System.Windows.Input;

namespace IMSE5515DataValidation
{
    internal class VM
    {
        public string Reel { get; set; }
        public ICommand ValidateCommand { get; set; }
        public ICommand CsvExportCommand { get; set; }

        public VM()
        {
            ValidateCommand = new Prism.Commands.DelegateCommand(() => Validate() );
            CsvExportCommand = new Prism.Commands.DelegateCommand(() => CsvExport());
        }

        private void Validate()
        {
            var valid = GetCurrentMeasurementData(out List<REP_D_VJ_REEL_DATALOG>  measurementData, out List<REP_D_VJ_REEL_DATALOG> allMeasurementData, out string status);
            if (!valid)
            {
                MessageBox.Show(status);
                return;
            }
            //var allMeasurementData = REP_D_VJ_REEL_DATALOG.GetTmdbMeasurements(Reel);
            //var measurementData = allMeasurementData.Where(m => m.OPERATION_NUMBER == 8444).ToList();
            //string statusOutput = "";
            //if (measurementData.Count > 0 && !(REP_D_VJ_REEL_DATALOG.fixReelOperations.ContainsKey(Reel) && REP_D_VJ_REEL_DATALOG.fixReelOperations[Reel] == "8444"))
            //{
            //    statusOutput += $"measurements found with operation 8444 - check with Chin Hock Ong" + Environment.NewLine;
            //}
            //if (measurementData.Count == 0)
            //    measurementData = allMeasurementData.Where(m => m.OPERATION_NUMBER == 7385).ToList();
            //if (measurementData.Count == 0)
            //    measurementData = allMeasurementData.Where(m => m.OPERATION_NUMBER == 7398 || m.OPERATION_NUMBER == 7310).ToList();
            //if (measurementData.Count == 0)
            //{
            //    MessageBox.Show("no measurement data found for reel " + Reel);
            //    return;
            //}

            //skip for now as there is an invalid number of DMC on the dummy reel
            if (!CustomerDataSharingLogic.IMSE5515.IMSE5515DataValidation.ValidateData(measurementData, allMeasurementData, out List<string> errors))
            {
                if (errors.Count > 20)
                {
                    int amount = errors.Count;
                    errors = errors.GetRange(0, 20);
                    errors.Add($"more ... ({amount} in total)");
                }
                MessageBox.Show(status + String.Join(Environment.NewLine, errors));
            }
            else
                MessageBox.Show(status + "Validation successful - no errors found");
        }

        private bool GetCurrentMeasurementData(out List<REP_D_VJ_REEL_DATALOG> measurementData, out List<REP_D_VJ_REEL_DATALOG> allMeasurementData, out string status)
        {
            allMeasurementData = REP_D_VJ_REEL_DATALOG.GetTmdbMeasurements(Reel);
            measurementData = allMeasurementData.Where(m => m.OPERATION_NUMBER == 8444).ToList();
            if (measurementData.Count == 0)
                measurementData = allMeasurementData;
            //if (measurementData.Count > 0)
            //{
            //    statusOutput += $"measurements of operation 8444 used" + Environment.NewLine;
            //}
            //if (measurementData.Count == 0)
            //    measurementData = allMeasurementData.Where(m => m.OPERATION_NUMBER == 7385).ToList();
            //if (measurementData.Count == 0)
            //    measurementData = allMeasurementData.Where(m => m.OPERATION_NUMBER == 7398 || m.OPERATION_NUMBER == 7310).ToList();
            if (measurementData.Count == 0)
            {
                status = "no measurement data found";
                return false;
            }
            else
            {
                var operations = measurementData.Select(m => m.OPERATION_NUMBER).Distinct().ToList();
                //var myMeasurementData = measurementData.ToList();
                //var operationText = String.Join(", ", operations.Select(o => $"{o} ({myMeasurementData.Where(m => m.OPERATION_NUMBER == o).Count()})"));
                var myAllMeasurementData = measurementData.ToList();
                var operationUsedText = String.Join(", ", operations.Select(o => $"{o} ({myAllMeasurementData.Where(m => m.OPERATION_NUMBER == o).Count()})"));

                status = $"measurements of operations used: " + operationUsedText + Environment.NewLine;
                return true;
            }
        }

        private void CsvExport()
        {
            var valid = GetCurrentMeasurementData(out List<REP_D_VJ_REEL_DATALOG> measurementData, out List<REP_D_VJ_REEL_DATALOG> allMeasurementData, out string status);
            if (!valid)
            {
                MessageBox.Show(status);
                return;
            }

            var reelData = new REEL_DATA
            {
                CalendarWeek = measurementData.First().LOT_CALWEEK,
                Delivery_ID = "delivery id",
                Handling_Unit = "handling unit",
                Reel_Label = Reel,
                Dry_Pack_Label = "dry pack label",
                Product_Box_Label = "product box label"
            };

            var csvContent = IMSE5515CsvHelper.CreateCsvFile(reelData, measurementData);
            //update content to ADLS
            var csvBytes = Encoding.UTF8.GetBytes(csvContent);
            var outputFile = FileFunctions.GetOutputFile("csv");
            File.WriteAllBytes(outputFile, csvBytes);
            FileFunctions.OpenFile(outputFile);

            //string fileName = $"{deliveryItem.MATERIAL_NUMBER_11}_{reelData.CalendarWeek}_{imoItem.WI_BATCH_NUM}_{imoItem.WI_IN_CHECK_CODE}_{imoItem.WI_OUT_CHECK_CODE ?? imoItem.WI_MID_CHECK_CODE}.CSV";
            //var targetUploadPath = Path.Combine(UploadDirectory, "IMSE5515", fileName);
            ////var fileUploadTask = StorageConnection.Default.UploadFileContent($"reel/{deliveryItem.BATCH_NUMBER.Substring(0, 5)}/{deliveryItem.BATCH_NUMBER}/", $"{reelId}.csv", csvBytes, StorageConnection.WhenFileAlreadyExists.RenameOld);
            ////fileUploadTask.Wait();
            //var box = StandardizedUploadHelper.FillEntryFromSAP<IMSE5515Box>(deliveryItem);
            //box.products = measurementData.Select(m => new IMSE5515Product() { dmc = m.DMC }).ToArray();
            //box.customer_group = "-" + box.customer_group;
        }
    }
}
