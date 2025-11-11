using CustomerDataSharingLogic.ExternalConnections.IMO;
using CustomerDataSharingLogic.ExternalConnections.Snowflake;
using CustomerDataSharingLogic.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace CustomerDataSharingLogic.IMSE5515
{
    public static class IMSE5515EmailCreation
    {
        internal static void CreateMail(string content, VW_DELIVERY_DATA_INCL_IMO deliveryData, REP_T_VD_W_IMO imoData, string fileName)
        {
            String CustomerMaterial = "ToDo Customer material";
            if (deliveryData.CUSTOMER_ID == "511158" && deliveryData.MATERIAL_NUMBER_Q == "Q65113A6771") //EBV Elektronik GmbH = Webasto
                CustomerMaterial = "OOSKRTBAELPS2324!B";
            else if (deliveryData.CUSTOMER_ID == "512341" && deliveryData.MATERIAL_NUMBER_Q.IsContainedIn("Q65113A8829", "Q65113A4846")) //APAG Elektronik s.r.o.
                CustomerMaterial = "0210-7931";
            else
            {
                throw new Exception($"Customer material not known yet... (Q Number: {deliveryData.MATERIAL_NUMBER_Q}; customer: {deliveryData.CUSTOMER_ID}");
            }

            string mailSubject = $"Osram KRTB AELPS2.32 [{CustomerMaterial}],[{deliveryData.SALES_ORDER_NUMBER}],[{deliveryData.MATERIAL_NUMBER_11} {imoData.WI_BATCH_NUM} {imoData.WI_IN_CHECK_CODE}_{imoData.WI_OUT_CHECK_CODE ?? imoData.WI_MID_CHECK_CODE}]";

            string mailBody = $@"Dear Customer,

Attached please find the test data for your order of KRTB AELPS2.32 [{CustomerMaterial}]:

Details:
- Order / Position#: {deliveryData.SALES_ORDER_NUMBER} / {deliveryData.SALES_ORDER_ITEM}
- OSmat#: {deliveryData.MATERIAL_NUMBER_11}
- Q#: {deliveryData.MATERIAL_NUMBER_Q}
- File name: {fileName}

Thanks for your order.

Best regards,
Osram Customer Service
";

            //String receiverStr = "V.Schmidts@osram.com;qingqing.wu@ams-osram.com;dongxu.qin@ams-osram.com;yu.li@ams-osram.com;chin-hock.ong@ams-osram.com";
            String receiverStr = "V.Schmidts@osram.com";
#if DEBUG
#else
            if (deliveryData.CUSTOMER_ID == "511158") //EBV Elektronik GmbH = Webasto
                receiverStr = "dominique.deusch@webasto.com;wolfgang.lettko@webasto.com;nils.wittler@webasto.com";
            else if (deliveryData.CUSTOMER_ID == "512341")  //APAG Elektronik s.r.o.
                receiverStr = "Pavel.Kubant@apagcosyst.com";
#endif

            var mh = new MailHelper()
            {
                Subject = mailSubject,
                Body = mailBody,
                From = "serviceinfo@osram.com",
                AttachmentContents = new Dictionary<string, byte[]>()
                {
                    { fileName, Encoding.UTF8.GetBytes(content) }
                },
                ReceiverStr = receiverStr,
                BccStr = "V.Schmidts@osram.com"
            };
            mh.Send();
        }
    }
}
