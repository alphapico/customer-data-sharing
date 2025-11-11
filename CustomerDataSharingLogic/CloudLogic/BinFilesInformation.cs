using System.Collections.Generic;

namespace CustomerDataSharingLogic.CloudLogic
{
    public class BinFilesInformation
    {
        public List<BinFileInformation> BinFiles { get; set; } = new List<BinFileInformation>();

        public class BinFileInformation
        {
            public string BinFileName { get; set; }
            public int PermanentOffPixels { get; set; }
            public string BinFileVersion { get; set; }
        }
    }
}