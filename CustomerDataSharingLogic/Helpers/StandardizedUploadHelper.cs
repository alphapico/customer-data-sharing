using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using CustomerDataSharingLogic.ExternalConnections.Snowflake;
using osram.OSAS.Logging;
using System;
using System.Collections.Generic;
using System.IO;

namespace CustomerDataSharingLogic.Helpers
{
    internal class StandardizedUploadHelper
    {
        public static void Upload<T>(T box, List<FilesToUpload> files) where T: DefaultCosmosEntry
        {
            if(box != null)
            {
                LogHelper.Info(typeof(StandardizedUploadHelper), "  create cosmos entry");
                var fileUploadTask = CosmosConnection.Default.Create<T>(box, box.customer_id);
                fileUploadTask.Wait();
                LogHelper.Info(typeof(StandardizedUploadHelper), "  cosmos entry created");
            }

            if(files != null)
                foreach(var file in files)
                {
                    var fileInfo = new FileInfo(file.TargetPath);
                    if (!fileInfo.Directory.Exists)
                        fileInfo.Directory.Create();

                    if(file is FilesToUploadBytes fileBytes) 
                    {
                        WriteContentToFile(file.TargetPath, fileBytes.Content);
                    }
                    else if(file is FilesToUploadSourceFile fileSourceFile)
                    {
                        if(fileSourceFile.Content == null)
                            fileSourceFile.Content = File.ReadAllBytes(fileSourceFile.SourcePath);
                        WriteContentToFile(file.TargetPath, fileSourceFile.Content);
                        if (fileSourceFile.DeleteAfterUpload)
                            File.Delete(fileSourceFile.SourcePath);
                    }
                }
        }

        private static void WriteContentToFile(string targetPath, byte[] content)
        {
            var fileInfo = new FileInfo(targetPath);
            if (!Directory.Exists(fileInfo.Directory.FullName))
                Directory.CreateDirectory(fileInfo.Directory.FullName);

            File.WriteAllBytes(targetPath, content);
        }

        public static T FillEntryFromSAP<T>(VW_DELIVERY_DATA deliveryData) where T : DefaultCosmosEntry
        {
            T result = (T)Activator.CreateInstance(typeof(T));

            result.id = Guid.NewGuid().ToString();
            result.customer_id = deliveryData.CUSTOMER_ID;
            result.customer_name = deliveryData.CUSTOMER_NAME;
            result.customer_group = deliveryData.CUSTOMER_GROUP;
            result.purchase_order_number = deliveryData.CUSTOMER_PO_NUMBER; //purchaseOrderNumber,
            result.order_number = deliveryData.SALES_ORDER_NUMBER; //orderId
            result.order_date = deliveryData.ORDER_DATE;
            result.order_pos_no = deliveryData.SALES_ORDER_ITEM;
            result.quantity_ordered = (int)deliveryData.ORDERED_QUANTITY;
            result.material_number = deliveryData.MATERIAL_NUMBER_Q;
            result.material_text = deliveryData.MATERIAL_NUMBER_Q_TEXT;
            result.material_number_11 = deliveryData.MATERIAL_NUMBER_11;
            result.material_text_11 = deliveryData.MATERIAL_NUMBER_11_TEXT;
            result.delivery_number = deliveryData.DELIVERY_NUMBER;
            result.shipment_date = deliveryData.SHIPMENT_DATE;
            result.batch_number = deliveryData.BATCH_NUMBER;
            result.lot_id = deliveryData.LOT_ID;
            result.box_id = deliveryData.BOX_ID;

            return result;
        }

        public abstract class FilesToUpload
        {
            public string TargetPath { get; set; }
        }

        public class FilesToUploadBytes : FilesToUpload
        {
            public byte[] Content { get; set; }
        }

        public class FilesToUploadSourceFile : FilesToUpload
        {
            public byte[] Content { get; set; }
            public string SourcePath { get; set; }
            public bool DeleteAfterUpload { get; set; }
        }
    }
}
