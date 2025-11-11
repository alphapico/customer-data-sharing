using System.Windows;
using System.Windows.Input;
using System.Windows.Markup;

namespace IMSE5515ExcelIntegration
{
    /// <summary>
    /// Interaction logic for SearchWindow.xaml
    /// </summary>
    public partial class IMSE5515Window : System.Windows.Window
    {
        public IMSE5515Window()
        {
            InitializeComponent();
            DataContextChanged += IMSE5515Window_DataContextChanged;
            
        }

        void IMSE5515Window_DataContextChanged(object sender, DependencyPropertyChangedEventArgs e)
        {
            IMSE5515VM vm = e.NewValue as IMSE5515VM;
            if (vm == null)
                return;

            IMSE5515VM.MyOwner = this;
            //ResultFieldTable.BaseDataGrid.ItemsSource = vm.ResultKeys;
        }

        private void TextBox_KeyDown(object sender, KeyEventArgs e)
        {
        }

        private void TextBox_PreviewKeyDown(object sender, KeyEventArgs e)
        {

        }
    }
}
