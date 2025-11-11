using Microsoft.Win32;
using System;
using System.Threading;
using System.Windows.Forms;
using System.Windows.Interop;
using Office = Microsoft.Office.Core;

namespace IMSE5515ExcelIntegration
{
    public partial class ThisAddIn
    {
        //private Office.CommandBarButton imse5515Button;
        //private Excel.Range selectedCells;
        private IMSE5515Window imse5515Window = null;
        private IMSE5515VM vm;
        public IMSE5515VM VM
        {
            get
            {
                if (vm == null)
                {
                    vm = new IMSE5515VM()
                    {
                        WriteExcelValues = () =>
                        {
                            ExcelHelper.WriteExcelValues(vm.CellValues, Application);
                            if (imse5515Window != null)
                                imse5515Window.Close();
                        }
                    };
                }
                return vm;
            }
        }

        private IMSE5515ListWindow imse5515ListWindow = null;
        private IMSE5515ListVM vmList;
        public IMSE5515ListVM VMList
        {
            get
            {
                if (vmList == null)
                {
                    vmList = new IMSE5515ListVM()
                    {
                        WriteExcelValues = () =>
                        {
                            ExcelHelper.WriteExcelValues(vmList.CellValues, Application);
                            if (imse5515ListWindow != null)
                                imse5515ListWindow.Close();
                        }
                    };
                }
                return vmList;
            }
        }

        private void ThisAddIn_Startup(object sender, System.EventArgs e)
        {
            Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en-US");
        }

        internal void IMSE5515Button_Click(Office.CommandBarButton ctrl, ref bool cancelDefault)
        {
            //if (vm.UserDataControl == null || vm.UserDataControl is UserDetailData)
            //    vm.UserDataControl = new UserListData();

            //selectedCells = Application.Selection;
            //if (selectedCells == null)
            //    selectedCells = Application.Selection as Range;

            //if (selectedCells == null || selectedCells.Count == 0)
            //    MessageBox.Show("No cells selected");

            try
            {
                //VM.CellValues = ExcelHelper.LoadCellValues(selectedCells, vm.UserDataControl is UserDetailData);
                imse5515Window = new IMSE5515Window
                {
                    DataContext = VM
                };
                try //try to set the parent of the popup window
                {
                    WindowInteropHelper interop = new WindowInteropHelper(imse5515Window)
                    {
                        Owner = new IntPtr(Globals.ThisAddIn.Application.Hwnd)
                    };
                }
                catch { }
                Globals.ThisAddIn.Application.Interactive = false;
                imse5515Window.ShowDialog();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            finally
            {
                Globals.ThisAddIn.Application.Interactive = true;
            }
        }

        internal void IMSE5515ButtonList_Click(Office.CommandBarButton ctrl, ref bool cancelDefault)
        {
            //if (vm.UserDataControl == null || vm.UserDataControl is UserDetailData)
            //    vm.UserDataControl = new UserListData();

            //selectedCells = Application.Selection;
            //if (selectedCells == null)
            //    selectedCells = Application.Selection as Range;

            //if (selectedCells == null || selectedCells.Count == 0)
            //    MessageBox.Show("No cells selected");

            try
            {
                //VM.CellValues = ExcelHelper.LoadCellValues(selectedCells, vm.UserDataControl is UserDetailData);
                imse5515ListWindow = new IMSE5515ListWindow
                {
                    DataContext = VMList
                };
                try //try to set the parent of the popup window
                {
                    WindowInteropHelper interop = new WindowInteropHelper(imse5515ListWindow)
                    {
                        Owner = new IntPtr(Globals.ThisAddIn.Application.Hwnd)
                    };
                }
                catch { }
                Globals.ThisAddIn.Application.Interactive = false;
                imse5515ListWindow.ShowDialog();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            finally
            {
                Globals.ThisAddIn.Application.Interactive = true;
            }
        }

        internal void AddAdditionalColumns(Office.CommandBarButton ctrl, ref bool cancelDefault)
        {
            try
            {
                //VM.CellValues = ExcelHelper.LoadCellValues(selectedCells, vm.UserDataControl is UserDetailData);
                imse5515ListWindow = new IMSE5515ListWindow
                {
                    DataContext = VMList
                };
                try //try to set the parent of the popup window
                {
                    WindowInteropHelper interop = new WindowInteropHelper(imse5515ListWindow)
                    {
                        Owner = new IntPtr(Globals.ThisAddIn.Application.Hwnd)
                    };
                }
                catch { }
                Globals.ThisAddIn.Application.Interactive = false;
                imse5515ListWindow.ShowDialog();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
            finally
            {
                Globals.ThisAddIn.Application.Interactive = true;
            }
        }

        protected override Office.IRibbonExtensibility CreateRibbonExtensibilityObject()
        {
            var ribbon = new Ribbon(this);

            //add right click menu buttons only until version 2013 -> afterwards it throws errors
            try
            {
                RegistryKey baseKey = Registry.ClassesRoot;
                RegistryKey subKey = baseKey.OpenSubKey(@"Excel.Application\CurVer");
                var versionStr = subKey.GetValue(string.Empty).ToString().Replace("Excel.Application.", "");
                var version = Convert.ToInt32(versionStr);
                if (version <= 15) //Office 2013
                {
                    this.Startup += new EventHandler(ThisAddIn_Startup);
                    this.Shutdown += new EventHandler(ThisAddIn_Shutdown);
                }
            }
            catch { }

            return ribbon;
        }

        private void ThisAddIn_Shutdown(object sender, System.EventArgs e)
        {
        }

        #region VSTO generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InternalStartup()
        {
            this.Startup += new System.EventHandler(ThisAddIn_Startup);
            this.Shutdown += new System.EventHandler(ThisAddIn_Shutdown);
        }
        
        #endregion
    }
}
