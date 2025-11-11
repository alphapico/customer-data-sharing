using System;
using System.Collections.Generic;
using System.Text;

namespace CustomerDataSharingLogic.ExternalConnections.CosmosDB
{
    public class DefaultCosmosEntry
    {
        public string id { get; set; }
        public string customer_id { get; set; }
        public string customer_name { get; set; }
        public string customer_group { get; set; }
        public string purchase_order_number { get; set; }
        public string order_number { get; set; }
        public DateTime order_date { get; set; }
        public string order_pos_no { get; set; }
        public int quantity_ordered { get; set; }
        public string material_number { get; set; }
        public string material_text { get; set; }
        public string material_number_11 { get; set; }
        public string material_text_11 { get; set; }
        public string delivery_number { get; set; }
        public DateTime shipment_date { get; set; }
        public string batch_number { get; set; }
        public int batch_quantity { get; set; }
        public int quantity_in_batch { get; set; } = 280;
        public string box_id { get; set; } //=same as box property
        public string lot_id { get; set; }
        public string product { get; set; }
    }
}
