using System;
using System.IO;
using System.Security.Cryptography;

namespace CustomerDataSharingLogic.Helpers
{
    /// <summary>
    ///     Class for encrypting and decrypting strings
    /// </summary>
    public static class EncryptionHelper
    {
        #region Fields

        /// <summary>
        ///     the used alghorithm
        /// </summary>
        private static AesManaged managedAes = new AesManaged();

        #endregion Fields

        #region Public Methods

        /// <summary>
        ///     Encrypts the given text using the given key and vector.
        ///     Key and vector can be created by using a static method within the class.
        /// </summary>
        /// <param name="textToEncrypt">The text to encrypt</param>
        /// <param name="key">The key</param>
        /// <param name="vector">The vector</param>
        /// <returns>The encrypted text</returns>
        public static string EncryptText(string textToEncrypt, byte[] key, byte[] vector)
        {
            var encryptedBytes = EncryptTextToByteArray(textToEncrypt, key, vector);

            return ConvertByteArrayToString(encryptedBytes);
        }

        /// <summary>
        ///     Encrypts the given text using the given key and vector.
        ///     Key and vector can be created by using a static method within the class.
        /// </summary>
        /// <param name="textToEncrypt">The text to encrypt</param>
        /// <param name="key">The key</param>
        /// <param name="vector">The vector</param>
        /// <returns>The encrypted text</returns>
        public static string EncryptText(string textToEncrypt, string key, string vector)
        {
            byte[] keyAsByte;
            byte[] vectorAsByte;

            try
            {
                keyAsByte = Convert.FromBase64String(key);
                vectorAsByte = Convert.FromBase64String(vector);
            }
            catch
            {
                return "At least one String was not a base64 string.";
            }

            return EncryptText(textToEncrypt, keyAsByte, vectorAsByte);
        }

        /// <summary>
        ///     Decrypts the given text using the key and vector.
        /// </summary>
        /// <param name="textToDecrypt">The text to decrypt</param>
        /// <param name="key">Key used to decrypt</param>
        /// <param name="vector">Vector used to decrypt</param>
        /// <returns>The decrypted text </returns>
        public static string DecryptText(string textToDecrypt, byte[] key, byte[] vector)
        {
            byte[] convertedText = null;

            try
            {
                convertedText = Convert.FromBase64String(textToDecrypt);
            }
            catch
            {
                return "Given text is not a encrypted Text";
            }

            return DecryptStringFromBytes(convertedText, key, vector);
        }

        /// <summary>
        ///     Decrypts the given text using the key and vector.
        /// </summary>
        /// <param name="textToDecrypt">The text to decrypt</param>
        /// <param name="key">Key used to decrypt</param>
        /// <param name="vector">Vector used to decrypt</param>
        /// <returns>The decrypted text </returns>
        public static string DecryptText(string textToDecrypt, string key, string vector)
        {
            byte[] keyAsByte;
            byte[] vectorAsByte;

            try
            {
                keyAsByte = Convert.FromBase64String(key);
                vectorAsByte = Convert.FromBase64String(vector);
            }
            catch
            {
                return "At least one String was not a base64 string.";
            }

            return DecryptText(textToDecrypt, keyAsByte, vectorAsByte);
        }

        /// <summary>
        ///     Creates a key as byte-array.
        /// </summary>
        /// <returns>The key</returns>
        public static byte[] CreateKey()
        {
            managedAes.GenerateKey();
            return managedAes.Key;
        }

        /// <summary>
        ///     Creates a vector as byte-array.
        /// </summary>
        /// <returns>The vector</returns>
        public static byte[] CreateVector()
        {
            managedAes.GenerateIV();
            return managedAes.IV;
        }

        #endregion Public Methods

        #region Private Methods

        /// <summary>
        ///     Converts the given string in a byte-arraz
        /// </summary>
        /// <param name="textToEncrypt">Text to encrypt</param>
        /// <param name="key">The used Key</param>
        /// <param name="vector">The used vector</param>
        /// <returns>Encrypted Byte-Array</returns>
        private static byte[] EncryptTextToByteArray(string textToEncrypt, byte[] key, byte[] vector)
        {
            byte[] encrypted;
            var encryptor = managedAes.CreateEncryptor(key, vector);

            // Create the streams used for encryption.
            using (MemoryStream msEncrypt = new MemoryStream())
            {
                CryptoStream csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write);
                StreamWriter swEncrypt = new StreamWriter(csEncrypt);

                //Write all data to the stream.
                swEncrypt.Write(textToEncrypt);

                encrypted = msEncrypt.ToArray();
            }

            return encrypted;
        }

        /// <summary>
        ///     Converts the given string in a byte-arraz
        /// </summary>
        /// <param name="textAsBytes">Text to decrypt</param>
        /// <param name="key">The used Key</param>
        /// <param name="vector">The used vector</param>
        /// <returns>Decrypted Text</returns>
        private static string DecryptStringFromBytes(byte[] textAsBytes, byte[] key, byte[] vector)
        {
            // Check arguments.
            if (textAsBytes == null || textAsBytes.Length <= 0)
                throw new ArgumentNullException("cipherText");
            if (key == null || key.Length <= 0)
                throw new ArgumentNullException("Key");
            if (vector == null || vector.Length <= 0)
                throw new ArgumentNullException("Key");

            // Declare the string used to hold
            // the decrypted text.
            string plaintext = null;

            // Create an AesManaged object
            // with the specified key and IV.
            using (AesManaged aesAlg = new AesManaged())
            {
                aesAlg.Key = key;
                aesAlg.IV = vector;

                // Create a decrytor to perform the stream transform.
                ICryptoTransform decryptor = aesAlg.CreateDecryptor();

                // Create the streams used for decryption.
                using (MemoryStream msDecrypt = new MemoryStream(textAsBytes))
                {
                    CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read);
                    StreamReader srDecrypt = new StreamReader(csDecrypt);

                    // Read the decrypted bytes from the decrypting stream
                    // and place them in a string.
                    plaintext = srDecrypt.ReadToEnd();
                }
            }

            return plaintext;
        }

        /// <summary>
        ///     Converts the given Byte-Array in the Base64-string
        /// </summary>
        /// <param name="arrayToConvert">Byte-Array to Convert</param>
        /// <returns>Converted Text</returns>
        private static string ConvertByteArrayToString(Byte[] arrayToConvert)
        {
            return Convert.ToBase64String(arrayToConvert);
        }

        #endregion Private Methods
    }
}