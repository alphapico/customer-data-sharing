using CustomerDataSharingLogic.ExternalConnections.ADLS;
using CustomerDataSharingLogic.ExternalConnections.CosmosDB;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace CustomerDataSharingLogic.Eviyos
{
    public class EviyosSampleDataGeneration
    {
        public static void CreateSampleData()
        {
            String customerId = "512345";
            String customerGroupId = "7099999";
            string customerName = "Dummy Customer";
            bool deleteAllOld = false;

            if (deleteAllOld) //only cosmos, not files
            {
                DeleteEntries(customerId);
            }

            Random rnd = new Random();
            var sampleFileContent = File.ReadAllBytes(@"C:\Temp\Sample.bin");
            int lotNumber = 70001;
            int waferNumber = 1;

            for (int i = 0; i < 20; i++) //20 orders
            {
                //create an order id in the range of 21000000 until 21999999
                var orderId = rnd.Next(21000000, 21999999);
                var startDate = new DateTime(2024, 6, 1);
                var endDate = new DateTime(2024, 7, 25);
                DateTime orderDate = startDate.AddDays(rnd.Next(0, (endDate - startDate).Days));
                var amsOsramOrderId = (long)420 * rnd.Next(1000000, 9999999);

                var orderPosAmount = rnd.Next(1, 3); //1-order positions deliveries
                for (int j = 1; j <= orderPosAmount; j++)
                {
                    List<EviyosBox> boxes = new List<EviyosBox>();

                    int orderPos = j * 10;

                    var deliveryAmount = rnd.Next(1, 3); //1-3 deliveries
                    for (int k = 0; k < deliveryAmount; k++)
                    {
                        var deliveryId = rnd.Next(51000000, 51999999);

                        var startShipmentDate = new DateTime(2022, 7, 7);
                        if (orderDate > startShipmentDate)
                            startShipmentDate = orderDate.AddDays(1);
                        var endShipmentDate = new DateTime(2023, 1, 31);
                        if (orderDate > endShipmentDate)
                            endShipmentDate = orderDate.AddDays(1);
                        if (startShipmentDate > endShipmentDate)
                            endShipmentDate = startShipmentDate.AddDays(1);
                        DateTime shipmentDate = startShipmentDate.AddDays(rnd.Next(0, (endShipmentDate - startShipmentDate).Days));

                        int batchQuantity = rnd.Next(1, 5); //1-5 batches
                        for (int l = 0; l < batchQuantity; l++)
                        {
                            int batchNumber = rnd.Next(1000000000, 1099999999);
                            int quantityInBatch = 25 * rnd.Next(1, 6); //25-150 products in this batch

                            EviyosBox box = new EviyosBox()
                            {
                                id = Guid.NewGuid().ToString(),
                                customer_id = customerId,
                                customer_name = customerName,
                                customer_group = customerGroupId,
                                purchase_order_number = amsOsramOrderId.ToString(),
                                order_number = orderId.ToString(),
                                order_date = orderDate.Date,
                                order_pos_no = orderPos.ToString(),
                                quantity_ordered = (int)0, //will be set later
                                material_number = "Q65113A0090",
                                material_text = "KEW GBCLD1U",
                                delivery_number = deliveryId.ToString(),
                                shipment_date = shipmentDate,
                                batch_number = batchNumber.ToString(),
                                batch_quantity = batchQuantity,
                                quantity_in_batch = quantityInBatch,
                                box_id = null,//batchNumber.ToString(), //=same as box property
                                lot_id = "HRG2400F19",
                                product="Eviyos",
                                products = new EviyosProduct[quantityInBatch]
                            };
                            boxes.Add(box);

                            if (waferNumber == 24)
                            {
                                lotNumber++;
                                waferNumber = 1;
                            }
                            int xPos = 1;
                            int yPos = 1;

                            for (int m = 0; m < quantityInBatch; m++)
                            {
                                if (yPos > 10)
                                {
                                    xPos++;
                                    yPos = 1;
                                }

                                var fileName = $"{lotNumber}_{waferNumber}_{xPos}_{yPos++}";

                                //var uploadTask = StorageConnection.Default.UploadFileContent($"{lotNumber}/{waferNumber}/", $"{fileName}.bin", sampleFileContent);
                                //uploadTask.Wait();

                                box.products[m] = new EviyosProduct()
                                {
                                    dmc = "0SF2BA0102",
                                    ic_code = fileName,
                                };
                            }
                            waferNumber++;
                        }
                    }

                    var amountOrdered = boxes.Sum(b => b.quantity_in_batch);
                    boxes.ForEach(b => b.quantity_ordered = amountOrdered);

                    var insertTask = CosmosConnection.Default.InsertMassData(boxes, customerId);
                    insertTask.Wait();
                }
            }
        }

        private static void DeleteEntries(string customerId)
        {
            var cosmosData = CosmosConnection.Default.GetData<EviyosBox>($"SELECT * FROM c where c.customer_id=\"" + customerId+"\"");
            cosmosData.Wait();
            Console.WriteLine($"{cosmosData.Result.Count} items found in cosmos db");
            List<EviyosBox> data = cosmosData.Result;
            foreach (var e in data)
            {
                var delTask = CosmosConnection.Default.Delete<EviyosBox>(e.id, e.customer_id);
                delTask.Wait();
            }
        }
    }
}