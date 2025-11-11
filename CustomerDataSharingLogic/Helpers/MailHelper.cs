using osram.OSAS.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Net.Mime;

namespace CustomerDataSharingLogic.Helpers
{
    /// <summary>
    ///     this class is able to send an email
    ///     instantiation of this class is possible in two ways:
    ///     use a parameterless constructor and defaults for smtp server and from address are used
    ///     use a parameterized constructor to define smtp server and from address
    /// </summary>
    public class MailHelper
    {
        #region variables

        // some constants
        private const String FROM_DEFAULT = "EviyosWebPortal@ams-osram.com";

        private const String SMTP_SERVER_DEFAULT = "intrelay.osram-light.com";

        #endregion variables

        #region properties

        /// <summary>
        ///     receiver for the email
        /// </summary>
        public List<MailAddress> Receiver { get; set; }

        /// <summary>
        ///     receiver as string
        /// </summary>
        public String ReceiverStr
        {
            get
            {
                if (Receiver == null || Receiver.Count == 0)
                    return "";
                else
                    return Receiver[0].Address;
            }
            set
            {
                Receiver.Clear();
                if (value != null)
                {
                    String[] receivers = value.Split(new char[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries);
                    foreach (String receiver in receivers)
                        Receiver.Add(new MailAddress(receiver));
                }
            }
        }

        /// <summary>
        ///     Cc receiver for the email
        /// </summary>
        public String[] Cc { get; set; }

        /// <summary>
        ///     Cc as string
        /// </summary>
        public String CcStr
        {
            get { return String.Join(",", Cc); }
            set { Cc = value.Split(new char[] { ',' }); }
        }

        /// <summary>
        ///     bcc receiver for the email
        /// </summary>
        public String[] Bcc { get; set; }

        /// <summary>
        ///     Bcc as string
        /// </summary>
        public String BccStr
        {
            get { return String.Join(",", Bcc); }
            set { Bcc = value.Split(new char[] { ',' }); }
        }

        /// <summary>
        ///     subject of the email
        /// </summary>
        public String Subject { get; set; }

        /// <summary>
        ///     body of the email
        /// </summary>
        public String Body { get; set; }

        /// <summary>
        ///     sender of the email
        /// </summary>
        public String From { get; set; }

        /// <summary>
        ///     smtp-Server for sending the email
        /// </summary>
        public String SmtpServer { get; set; }

        /// <summary>
        ///     sets or gets if body is html
        /// </summary>
        public bool IsBodyHtml { get; set; }

        /// <summary>
        ///     sets or gets the attachments of the email
        /// </summary>
        public List<Attachment> Attachments { get; set; }
        public Dictionary<string, byte[]> AttachmentContents { get; set; }

        /// <summary>
        ///     Attachments as string
        /// </summary>
        public String AttachmentStr
        {
            get { return String.Join(",", Attachments.Select(a => a.Name)); }
            set
            {
                foreach (String path in value.Split(new char[] { ',' }))
                {
                    System.IO.FileInfo file = new System.IO.FileInfo(path);
                    if (!file.Exists)
                        throw new Exception("File " + file + " doesn't exist");
                    AddAttachment(path);
                }
            }
        }

        public MailPriority MailPriority { get; set; }

        #endregion properties

        #region constructor

        /// <summary>
        ///     ctor that uses default for smtp server and from address
        /// </summary>
        public MailHelper()
        {
            SmtpServer = SMTP_SERVER_DEFAULT;
            if (String.IsNullOrEmpty(MyApplicationSettings.GetSetting("MailFrom")))
                From = FROM_DEFAULT;
            else
                From = MyApplicationSettings.GetSetting("MailFrom");
            Bcc = new String[0];
            Cc = new String[0];
            Receiver = new List<MailAddress>();
            Attachments = new List<Attachment>();
        }

        #endregion constructor

        #region functions

        /// <summary>
        ///     attachment which can be added to the email;
        ///     content type is fixed
        /// </summary>
        /// <param name="file">path of the attachment</param>
        public void AddAttachment(String file)
        {
            ContentType ct = new ContentType(MediaTypeNames.Application.Octet);
            AddAttachment(file, ct);
        }

        ///// <summary>
        /////     attachment which can be added to the email;
        /////     content type is fixed
        ///// </summary>
        ///// <param name="fileContent">content of the attachment</param>
        ///// <param name="fileName">name of the attachment</param>
        //public void AddAttachment(byte[] fileContent, string fileName)
        //{
        //    using (Stream mailStream = new MemoryStream(fileContent))
        //    {
        //        Attachment attachment = new System.Net.Mail.Attachment(mailStream, fileName);
        //        Attachments.Add(attachment);
        //    }
        //}

        /// <summary>
        ///     attachment which can be added to the email;
        ///     content type is fixed
        /// </summary>
        /// <param name="fileContent">content of the attachment</param>
        /// <param name="fileName">name of the attachment</param>
        public void AddAttachmentContent(byte[] fileContent, string fileName)
        {
            if (AttachmentContents == null)
                AttachmentContents = new Dictionary<string, byte[]>();

            AttachmentContents[fileName] = fileContent;
        }

        /// <summary>
        ///     attachment which can be added to the email;
        ///     content type can be defined
        /// </summary>
        /// <param name="file">path of the attachment</param>
        /// <param name="mediaType">type of the attachment</param>
        public void AddAttachment(String file, ContentType mediaType)
        {
            // Create  the file attachment for this e-mail message
            Attachment data = new Attachment(file, mediaType);

            // Add time stamp information for the file
            ContentDisposition disposition = data.ContentDisposition;
            disposition.CreationDate = System.IO.File.GetCreationTime(file);
            disposition.ModificationDate = System.IO.File.GetLastWriteTime(file);
            disposition.ReadDate = System.IO.File.GetLastAccessTime(file);

            //add the attachment to the list
            Attachments.Add(data);
        }

        /// <summary>
        ///     send the message with the given parameters
        /// </summary>
        /// <returns>if the mail could be sent successfully</returns>
        public bool Send()
        {
            LogHelper.Debug(this, "Start sending an email ...");
            bool result = false;

            //test if there is a receiver
            if (Receiver.Count == 0)
            {
                LogHelper.Warn(this, "No receiver for sending the mail");
            }
            else
            {
                //create a new message
                MailMessage mail = new MailMessage()
                {
                    From = new MailAddress(From),
                    Subject = Subject,
                    Body = Body,
                    IsBodyHtml = IsBodyHtml,
                    Priority = MailPriority
                };

                LogHelper.Debug(this, "Receiver: " + Receiver[0]);

                // add additional receivers
                foreach (MailAddress receiver in Receiver)
                {
                    MailAddress tmpReceiver = new MailAddress(receiver.ToString());
                    mail.To.Add(tmpReceiver);
                    LogHelper.Debug(this, "Receiver: " + receiver);
                }

                //add cc
                for (int i = 0; i < Cc.Length; i++)
                {
                    MailAddress tmpCc = new MailAddress(Cc[i]);
                    mail.CC.Add(tmpCc);
                }

                //add bcc
                for (int i = 0; i < Bcc.Length; i++)
                {
                    MailAddress tmpBcc = new MailAddress(Bcc[i]);
                    mail.Bcc.Add(tmpBcc);
                }

                // Add the file attachments to this email message
                foreach (Attachment att in Attachments)
                    mail.Attachments.Add(att);

                if (AttachmentContents != null)
                {
                    foreach (String fileName in AttachmentContents.Keys)
                    {
                        Attachment attachment = new System.Net.Mail.Attachment(new MemoryStream(AttachmentContents[fileName]), fileName);
                        mail.Attachments.Add(attachment);

                        //using (Stream mailStream = new MemoryStream(AttachmentContents[fileName]))
                        //{
                        //    Attachment attachment = new System.Net.Mail.Attachment(mailStream, fileName);
                        //    mail.Attachments.Add(attachment);
                        //}
                    }
                }

                //connect to the smtpServer
                SmtpClient client = new SmtpClient(SmtpServer);

                // Add the network-credentials (some SMTP server requires them)
                client.Credentials = CredentialCache.DefaultNetworkCredentials;
                try
                {
                    //send the mail
                    client.Send(mail);
                    LogHelper.Debug(this, "Sending an email finished successfully -> try to store");
                    result = true;
                }
                catch (Exception e)
                {
                    LogHelper.Error(this, "Error sending Email", e);
                }
            }

            LogHelper.Debug(this, "End of sending an email");
            return result;
        }

        #endregion functions

        #region static functions

        /// <summary>
        ///     sends an email to one reveiver
        /// </summary>
        /// <param name="receiver">receiver of the email</param>
        /// <param name="subject">subject of the email</param>
        /// <param name="body">body of the email</param>
        public static void SendMail(List<String> receivers, String subject, String body)
        {
            //create a new instance of the Mail-class
            MailHelper mail = new MailHelper();

            foreach (var receiver in receivers)
                mail.Receiver.Add(new MailAddress(receiver));

            mail.Subject = subject;
            mail.Body = body;
            mail.IsBodyHtml = body != null && body.Trim().ToLower().StartsWith("<html>");

            //send the mail
            mail.Send();
        }

        #endregion static functions
    }
}
