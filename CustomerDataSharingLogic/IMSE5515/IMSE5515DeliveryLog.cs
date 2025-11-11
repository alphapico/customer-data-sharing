using System;

namespace CustomerDataSharingLogic.IMSE5515
{
    internal class IMSE5515DeliveryLog
    {
        public string CusotmerID { get; set; }
        public string DeliveryNumber { get; set; }
        public string MaterialNumber { get; set; }
        public string BatchNumber { get; set; }
        public string ImoCheckinCode { get; set; }
        public DateTime TimeOfEmailSent { get; set; }
    }
}