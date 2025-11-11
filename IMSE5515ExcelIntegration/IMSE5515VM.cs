using CustomerDataSharingLogic.ExternalConnections.TMDB;
using CustomerDataSharingLogic.IMSE5515;
using osram.OSAS.WPF.MVVM;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Windows;
using System.Windows.Input;

namespace IMSE5515ExcelIntegration
{
    public class IMSE5515VM : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;
        protected void NotifyPropertyChanged<T>(Expression<Func<T>> expression)
        {
            this.NotifyPropertyChanged(PropertyChanged, expression);
        }

        public bool? DialogResult { get; set; }

        public String StartDMC { get; set; }
        public String EndDMC { get; set; }
        public int Amount { get { return measurements.Count; } }
        private List<REP_D_VJ_REEL_DATALOG> measurements = new List<REP_D_VJ_REEL_DATALOG>();

        public ICommand CheckDataCommand { get; set; }
        public ICommand TakeDataCommand { get; set; }

        public static System.Windows.Window MyOwner { get; set; }
        public List<CellValue> CellValues { get; set; } = new List<CellValue>();

        public Action WriteExcelValues { get; set; }

        public IMSE5515VM()
        {
            CheckDataCommand = new ActionCommand(CheckData);
            TakeDataCommand = new ActionCommand(TakeData);
        }

        private void CheckData()
        {
            if (String.IsNullOrEmpty(StartDMC))
            {
                MessageBox.Show("No Start DMC set");
                return;
            }

            if (String.IsNullOrEmpty(EndDMC))
            {
                MessageBox.Show("No End DMC set");
                return;
            }

            try 
            {
                var allMeasurementData = REP_D_VJ_REEL_DATALOG.GetTmdbMeasurementsInRange(StartDMC, EndDMC);
                var measurementData = allMeasurementData.Where(m => m.OPERATION_NUMBER == 8444).ToList();
                if (measurementData.Count == 0)
                    measurementData = allMeasurementData;

                if (measurementData.Count == 0)
                {
                    MessageBox.Show("No measurements found");
                    return;
                }

                //skip for now as there is an invalid number of DMC on the dummy reel
                if (!IMSE5515DataValidation.ValidateData(measurementData, allMeasurementData, out List<string> errors, false))
                {
                    MessageBox.Show("Errors in measurements found:" + Environment.NewLine + String.Join(";" + Environment.NewLine, errors));
                    return;
                }

                measurements = measurementData;
                NotifyPropertyChanged(() => Amount);
            }
            catch(Exception ex)
            {
                MessageBox.Show(ex.Message + Environment.NewLine + ex.StackTrace);
                return;
            }
        }

        private void TakeData()
        {
            try
            {
                FillData();
                WriteExcelValues();

                DialogResult = true;
                NotifyPropertyChanged(() => DialogResult);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
                return;
            }
        }

        private void FillData()
        {
            CellValues.Clear();

            //var rows = new List<object[]>();
            //rows.Add(",,Parameter grouping,CX,CX,CX,CX,CX,CX,CY,CY,CY,CY,CY,CY,IV,IV,IV,IV,IV,IV,UF,UF,UF,UF,UF,UF".Split(',').ToArray());
            //rows.Add(",,Chip color,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED".Split(',').ToArray());
            //rows.Add("Pocket Number,DMC,inner check code|Bias in Amp,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.".Split(',').ToArray());

            //int position = 1;
            //foreach(var measurement in measurements)
            //{
            //    rows.Add(
            //        new object[]
            //        {
            //            position++,
            //            measurement.DMC,
            //            measurement.REEL_ID,
            //            measurement.CX_BLUE_20,
            //            measurement.CX_BLUE_50,
            //            measurement.CX_GREEN_20,
            //            measurement.CX_GREEN_50,
            //            measurement.CX_RED_20,
            //            measurement.CX_RED_50,
            //            measurement.CY_BLUE_20,
            //            measurement.CY_BLUE_50,
            //            measurement.CY_GREEN_20,
            //            measurement.CY_GREEN_50,
            //            measurement.CY_RED_20,
            //            measurement.CY_RED_50,
            //            measurement.IV_BLUE_20,
            //            measurement.IV_BLUE_50,
            //            measurement.IV_GREEN_20,
            //            measurement.IV_GREEN_50,
            //            measurement.IV_RED_20,
            //            measurement.IV_RED_50,
            //            measurement.UF_BLUE_20,
            //            measurement.UF_BLUE_50,
            //            measurement.UF_GREEN_20,
            //            measurement.UF_GREEN_50,
            //            measurement.UF_RED_20,
            //            measurement.UF_RED_50
            //        }
            //    );
            //}

            var rows = new List<object[]>();
            rows.Add(",,Parameter grouping,CX,CX,CX,CX,CX,CX,CY,CY,CY,CY,CY,CY,IV,IV,IV,IV,IV,IV,IV adj,IV adj,IV adj,IV adj,IV adj,IV adj,UF,UF,UF,UF,UF,UF,Ldom,Ldom,Ldom,Ldom,Ldom,Ldom,B.X,B.X,B.Z,B.Z,G.X,G.X,G.Z,G.Z,R.X,R.X,R.Z,R.Z,B.X.a,B.X.b,B.Y.a,B.Y.b,B.Z.a,B.Z.b,B.T.a,B.T.b,G.X.a,G.X.b,G.Y.a,G.Y.b,G.Z.a,G.Z.b,G.T.a,G.T.b,R.X.a,R.X.b,R.Y.a,R.Y.b,R.Z.a,R.Z.b,R.T.a,R.T.b,B.X.a.u,B.X.a.v,B.X.a.w,B.X.b.u,B.X.b.v,B.X.b.w,B.Y.a.u,B.Y.a.v,B.Y.a.w,B.Y.b.u,B.Y.b.v,B.Y.b.w,B.Z.a.u,B.Z.a.v,B.Z.a.w,B.Z.b.u,B.Z.b.v,B.Z.b.w,B.T.a.u,B.T.a.v,B.T.a.w,B.T.b.u,B.T.b.v,B.T.b.w,G.X.a.u,G.X.a.v,G.X.a.w,G.X.b.u,G.X.b.v,G.X.b.w,G.Y.a.u,G.Y.a.v,G.Y.a.w,G.Y.b.u,G.Y.b.v,G.Y.b.w,G.Z.a.u,G.Z.a.v,G.Z.a.w,G.Z.b.u,G.Z.b.v,G.Z.b.w,G.T.a.u,G.T.a.v,G.T.a.w,G.T.b.u,G.T.b.v,G.T.b.w,R.X.a.u,R.X.a.v,R.X.a.w,R.X.b.u,R.X.b.v,R.X.b.w,R.Y.a.u,R.Y.a.v,R.Y.a.w,R.Y.b.u,R.Y.b.v,R.Y.b.w,R.Z.a.u,R.Z.a.v,R.Z.a.w,R.Z.b.u,R.Z.b.v,R.Z.b.w,R.T.a.u,R.T.a.v,R.T.a.w,R.T.b.u,R.T.b.v,R.T.b.w".Split(',').ToArray());
            rows.Add(",,Chip color,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,GREEN,GREEN,RED,RED,BLUE,BLUE,BLUE,BLUE,GREEN,GREEN,GREEN,GREEN,RED,RED,RED,RED,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,RED,RED,RED,RED,RED,RED,RED,RED,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,BLUE,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,GREEN,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED,RED".Split(',').ToArray());
            rows.Add("Pocket Number,DMC,inner check code|Bias in Amp,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20.,50.,20,50,20,50,20,50,20,50,20,50,20,50,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20,20".Split(',').ToArray());
            
            int position = 1;
            foreach(var measurement in measurements)
            {
                rows.Add(
                    new object[]
                    {
                        position++,
                        measurement.DMC,
                        measurement.REEL_ID,
                        measurement.CX_BLUE_20,
                        measurement.CX_BLUE_50,
                        measurement.CX_GREEN_20,
                        measurement.CX_GREEN_50,
                        measurement.CX_RED_20,
                        measurement.CX_RED_50,
                        measurement.CY_BLUE_20,
                        measurement.CY_BLUE_50,
                        measurement.CY_GREEN_20,
                        measurement.CY_GREEN_50,
                        measurement.CY_RED_20,
                        measurement.CY_RED_50,

                        measurement.IV_BLUE_20,
                        measurement.IV_BLUE_50,
                        measurement.IV_GREEN_20,
                        measurement.IV_GREEN_50,
                        measurement.IV_RED_20,
                        measurement.IV_RED_50,
                        //adjusted
                        measurement.IV_BLUE_20 * 0.92,
                        measurement.IV_BLUE_50 * 0.92,
                        measurement.IV_GREEN_20 * 0.98,
                        measurement.IV_GREEN_50 * 0.98,
                        measurement.IV_RED_20 * 0.97,
                        measurement.IV_RED_50 * 0.97,

                        measurement.UF_BLUE_20,
                        measurement.UF_BLUE_50,
                        measurement.UF_GREEN_20,
                        measurement.UF_GREEN_50,
                        measurement.UF_RED_20,
                        measurement.UF_RED_50,


                        measurement.LdomBlue20,
                        measurement.LdomBlue50,
                        measurement.LdomGreen20,
                        measurement.LdomGreen50,
                        measurement.LdomRed20,
                        measurement.LdomRed50,

                        measurement.BXBlue20,
                        measurement.BXBlue50,
                        measurement.BZBlue20,
                        measurement.BZBlue50,
                        measurement.BXGreen20,
                        measurement.BXGreen50,
                        measurement.BZGreen20,
                        measurement.BZGreen50,
                        measurement.BXRed20,
                        measurement.BXRed50,
                        measurement.BZRed20,
                        measurement.BZRed50,

                        measurement.BXaBlue20,
                        measurement.BXbBlue20,
                        measurement.BYaBlue20,
                        measurement.BYbBlue20,
                        measurement.BZaBlue20,
                        measurement.BZbBlue20,
                        measurement.BTaBlue20,
                        measurement.BTbBlue20,
                        measurement.GXaGreen20,
                        measurement.GXbGreen20,
                        measurement.GYaGreen20,
                        measurement.GYbGreen20,
                        measurement.GZaGreen20,
                        measurement.GZbGreen20,
                        measurement.GTaGreen20,
                        measurement.GTbGreen20,
                        measurement.RXaRed20,
                        measurement.RXbRed20,
                        measurement.RYaRed20,
                        measurement.RYbRed20,
                        measurement.RZaRed20,
                        measurement.RZbRed20,
                        measurement.RTaRed20,
                        measurement.RTbRed20,

                        measurement.BXauBlue20,
                        measurement.BXavBlue20,
                        measurement.BXawBlue20,
                        measurement.BXbuBlue20,
                        measurement.BXbvBlue20,
                        measurement.BXbwBlue20,
                        measurement.BYauBlue20,
                        measurement.BYavBlue20,
                        measurement.BYawBlue20,
                        measurement.BYbuBlue20,
                        measurement.BYbvBlue20,
                        measurement.BYbwBlue20,
                        measurement.BZauBlue20,
                        measurement.BZavBlue20,
                        measurement.BZawBlue20,
                        measurement.BZbuBlue20,
                        measurement.BZbvBlue20,
                        measurement.BZbwBlue20,
                        measurement.BTauBlue20,
                        measurement.BTavBlue20,
                        measurement.BTawBlue20,
                        measurement.BTbuBlue20,
                        measurement.BTbvBlue20,
                        measurement.BTbwBlue20,

                        measurement.GXauGreen20,
                        measurement.GXavGreen20,
                        measurement.GXawGreen20,
                        measurement.GXbuGreen20,
                        measurement.GXbvGreen20,
                        measurement.GXbwGreen20,
                        measurement.GYauGreen20,
                        measurement.GYavGreen20,
                        measurement.GYawGreen20,
                        measurement.GYbuGreen20,
                        measurement.GYbvGreen20,
                        measurement.GYbwGreen20,
                        measurement.GZauGreen20,
                        measurement.GZavGreen20,
                        measurement.GZawGreen20,
                        measurement.GZbuGreen20,
                        measurement.GZbvGreen20,
                        measurement.GZbwGreen20,
                        measurement.GTauGreen20,
                        measurement.GTavGreen20,
                        measurement.GTawGreen20,
                        measurement.GTbuGreen20,
                        measurement.GTbvGreen20,
                        measurement.GTbwGreen20,

                        measurement.RXauRed20,
                        measurement.RXavRed20,
                        measurement.RXawRed20,
                        measurement.RXbuRed20,
                        measurement.RXbvRed20,
                        measurement.RXbwRed20,
                        measurement.RYauRed20,
                        measurement.RYavRed20,
                        measurement.RYawRed20,
                        measurement.RYbuRed20,
                        measurement.RYbvRed20,
                        measurement.RYbwRed20,
                        measurement.RZauRed20,
                        measurement.RZavRed20,
                        measurement.RZawRed20,
                        measurement.RZbuRed20,
                        measurement.RZbvRed20,
                        measurement.RZbwRed20,
                        measurement.RTauRed20,
                        measurement.RTavRed20,
                        measurement.RTawRed20,
                        measurement.RTbuRed20,
                        measurement.RTbvRed20,
                        measurement.RTbwRed20
                    }
                );
            }

            for(int i = 0; i < rows.Count; i++)
            {
                CellValues.Add(new CellValue()
                {
                    CellReference = "A" + (i+1),
                    Result = rows[i]
                });
            }
        }

        public class CellValue
        {
            public String CellReference { get; set; }
            public object[] Result { get; set; }
        }
    }
}