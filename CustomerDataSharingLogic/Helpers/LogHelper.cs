
    #region references

    // system references

    using log4net;
    using log4net.Config;
    using System;
    using System.Runtime;
    using System.Configuration;
    using System.IO;
using System.Reflection;

// 3rd party references

#endregion references


namespace CustomerDataSharingLogic.Helpers
{
    public class LogHelper
    {
        #region fields

        /// <summary>
        ///     status of logger's configuration
        /// </summary>
        private static bool isConfigurate;

        /// <summary>
        ///     shows, whether config file exists or not
        /// </summary>
        private static bool hasConfigFile;

        /// <summary>
        ///     status of logger's configuration
        /// </summary>
        public static bool IsConfigurate
        {
            get { return LogHelper.isConfigurate; }
        }

        /// <summary>
        ///     shows, whether config file exists or not
        /// </summary>
        public static bool HasConfigFile
        {
            get { return LogHelper.hasConfigFile; }
        }

        public static string AssemblyDirectory
        {
            get
            {
                string codeBase = Assembly.GetExecutingAssembly().CodeBase;
                UriBuilder uri = new UriBuilder(codeBase);
                string path = Uri.UnescapeDataString(uri.Path);
                return Path.GetDirectoryName(path);
            }
        }

        #endregion fields

        #region statecheck

        /// <summary>
        ///     checks, whether debug level is enabled
        /// </summary>
        /// <param name="sender">sender object</param>
        /// <returns>true or false</returns>
        public static bool IsDebugEnabled(object sender)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());
                return log.IsDebugEnabled;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        ///     checks, whether info level is enabled
        /// </summary>
        /// <param name="sender">sender object</param>
        /// <returns>true (=enables)or false</returns>
        public static bool IsInfoEnabled(object sender)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());
                return log.IsInfoEnabled;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        ///     checks, whether warning level is enabled
        /// </summary>
        /// <param name="sender">sender object</param>
        /// <returns>true (=enables)or false</returns>
        public static bool IsWarnEnabled(object sender)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());
                return log.IsWarnEnabled;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        ///     checks, whether error level is enabled
        /// </summary>
        /// <param name="sender">sender object</param>
        /// <returns>true (=enables)or false</returns>
        public static bool IsErrorEnabled(object sender)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());
                return log.IsErrorEnabled;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        ///     checks, whether fatal level is enabled
        /// </summary>
        /// <param name="sender">sender object</param>
        /// <returns>true (=enables)or false</returns>
        public static bool IsFatalEnabled(object sender)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());
                return log.IsFatalEnabled;
            }
            else
            {
                return false;
            }
        }

        #endregion statecheck

        #region Configuration

        /// <summary>
        ///     Configuration By XML-File
        /// </summary>
        /// <param name="filename">name of configuration file</param>
        public static void configurateByFile(string filename)
        {
            try
            {
                FileInfo LogConfig = new FileInfo(Path.Combine(AssemblyDirectory, filename)); // AppDomain.CurrentDomain.SetupInformation.ApplicationBase + filename

                if (LogConfig.Exists)
                {
                    XmlConfigurator.ConfigureAndWatch(ReplaceEnvironmentVariables(LogConfig));
                    isConfigurate = true;
                }
            }
            catch (Exception excp)
            {
                System.Console.Write(excp.Message);
                isConfigurate = false;
            }
        }

        /// <summary>
        ///     load the Configuration of LogManager
        /// </summary>
        public static void configurateByFile()
        {
            string filename = MyApplicationSettings.GetSetting("LoggerConfig");

            if (filename != string.Empty || filename.Length != 0)
            {
                hasConfigFile = true;
                configurateByFile(filename);
            }
            else
            {
                hasConfigFile = false;
            }
        }

        #endregion Configuration

        #region ShutDown

        /// <summary>
        ///     shut down of the logging
        /// </summary>
        public static void shutDown()
        {
            try
            {
                LogManager.Shutdown();
            }
            catch (Exception excp)
            {
                System.Console.Write(excp.Message);
                isConfigurate = false;
            }
        }

        #endregion ShutDown

        #region Debug

        /// <summary>
        ///     writes a log entry on level DEBUG
        ///     This method first checks if this logger is <c>DEBUG</c>
        ///     enabled by comparing the level of this logger with the
        ///     level. If this logger is <c>DEBUG</c> enabled, then it converts the message object
        ///     (passed as parameter) to a string by invoking the appropriate
        ///     It then proceeds to call all the registered appenders in this logger
        ///     and also higher in the hierarchy depending on the value of
        ///     the additivity flag.
        /// </summary>
        /// <param name="sender">The sender</param>
        /// <param name="message">passed message</param>
        public static void Debug(object sender, object message)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());

                if (log.IsDebugEnabled)
                {
                    log.Debug(message);
                }
            }
        }

        /// <summary>
        ///     writes a log entry on level DEBUG including the stack trace
        ///     This method first checks if this logger is <c>DEBUG</c>
        ///     enabled by comparing the level of this logger with the
        ///     level. If this logger is <c>DEBUG</c> enabled, then it converts the message object
        ///     (passed as parameter) to a string by invoking the appropriate
        ///     It then proceeds to call all the registered appenders in this logger
        ///     and also higher in the hierarchy depending on the value of
        ///     the additivity flag.
        /// </summary>
        /// <param name="sender">The sender</param>
        /// <param name="message">passed message</param>
        /// <param name="e">The exception to log, including its stack trace.</param>
        public static void Debug(object sender, object message, Exception e)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());

                if (log.IsDebugEnabled)
                {
                    log.Debug(message, e);
                }
            }
        }

        #endregion Debug

        #region Info

        /// <summary>
        ///     writes a log entry on level INFO
        ///     This method first checks if this logger is <c>INFO</c>
        ///     enabled by comparing the level of this logger with the
        ///     level. If this logger is <c>INFO</c> enabled, then it converts the message object
        ///     (passed as parameter) to a string by invoking the appropriate
        ///     It then proceeds to call all the registered appenders in this logger
        ///     and also higher in the hierarchy depending on the value of
        ///     the additivity flag.
        /// </summary>
        /// <param name="sender">The sender</param>
        /// <param name="message">passed message</param>
        public static void Info(object sender, object message)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());

                if (log.IsInfoEnabled)
                {
                    log.Info(message);
                }
            }
        }

        /// <summary>
        ///     writes a log entry on level INFO including the stack trace
        ///     This method first checks if this logger is <c>INFO</c>
        ///     enabled by comparing the level of this logger with the
        ///     level. If this logger is <c>INFO</c> enabled, then it converts the message object
        ///     (passed as parameter) to a string by invoking the appropriate
        ///     It then proceeds to call all the registered appenders in this logger
        ///     and also higher in the hierarchy depending on the value of
        ///     the additivity flag.
        /// </summary>
        /// <param name="sender">The sender</param>
        /// <param name="message">passed message</param>
        /// <param name="e">The exception to log, including its stack trace.</param>
        public static void Info(object sender, object message, Exception e)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());

                if (log.IsInfoEnabled)
                {
                    log.Info(message, e);
                }
            }
        }

        /// <summary>
        ///     writes a logging info concerning LDAP
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="message">The message to log</param>
        public static void LDAPInfo(object sender, object message)
        {
            Info(sender, message);
        }

        /// <summary>
        ///     writes a logging info with exception concerning LDAP
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="message">The message to log</param>
        /// <param name="e">passed exception</param>
        public static void LDAPInfo(object sender, object message, Exception e)
        {
            Info(sender, message, e);
        }

        /// <summary>
        ///     writes a logging info concerning eMail
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="message">The message to log</param>
        public static void MailInfo(object sender, object message)
        {
            Info(sender, message);
        }

        /// <summary>
        ///     writes a logging info with exception concerning eMail
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="message">The message to log</param>
        /// <param name="e">passed exception</param>
        public static void MailInfo(object sender, object message, Exception e)
        {
            Info(sender, message, e);
        }

        /// <summary>
        ///     writes a logging info concerning database
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="message">The message to log</param>
        public static void DBInfo(object sender, object message)
        {
            Info(sender, message);
        }

        /// <summary>
        ///     writes a logging info with exception concerning database
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="message">The message to log</param>
        /// <param name="e">passed exception</param>
        public static void DBInfo(object sender, object message, Exception e)
        {
            Info(sender, message, e);
        }

        /// <summary>
        ///     writes a logging info concerning file system
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="message">The message to log</param>
        public static void FilesystemIOInfo(object sender, object message)
        {
            Info(sender, message);
        }

        /// <summary>
        ///     writes a logging info with exception concerning file system
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="message">The message to log</param>
        /// <param name="e">passed exception</param>
        public static void FilesystemIOInfo(object sender, object message, Exception e)
        {
            Info(sender, message, e);
        }

        #endregion Info

        #region Warning

        /// <summary>
        ///     writes a log entry on level WARNING
        ///     This method first checks if this logger is <c>WARN</c>
        ///     enabled by comparing the level of this logger with the
        ///     level. If this logger is <c>WARN</c> enabled, then it converts the message object
        ///     (passed as parameter) to a string by invoking the appropriate
        ///     It then proceeds to call all the registered appenders in this logger
        ///     and also higher in the hierarchy depending on the value of
        ///     the additivity flag.
        /// </summary>
        /// <param name="sender">The sender</param>
        /// <param name="message">passed message</param>
        public static void Warn(object sender, object message)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());

                if (log.IsWarnEnabled)
                {
                    log.Warn(message);
                }
            }
        }

        /// <summary>
        ///     writes a log entry on level WARNING including the stack trace
        ///     This method first checks if this logger is <c>WARN</c>
        ///     enabled by comparing the level of this logger with the
        ///     level. If this logger is <c>WARN</c> enabled, then it converts the message object
        ///     (passed as parameter) to a string by invoking the appropriate
        ///     It then proceeds to call all the registered appenders in this logger
        ///     and also higher in the hierarchy depending on the value of
        ///     the additivity flag.
        /// </summary>
        /// <param name="sender">The sender</param>
        /// <param name="message">passed message</param>
        /// <param name="e">The exception to log, including its stack trace.</param>
        public static void Warn(object sender, object message, Exception e)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());

                if (log.IsWarnEnabled)
                {
                    log.Warn(message, e);
                }
            }
        }

        #endregion Warning

        #region Error

        /// <summary>
        ///     writes a log entry on level ERROR
        ///     This method first checks if this logger is <c>ERROR</c>
        ///     enabled by comparing the level of this logger with the
        ///     level. If this logger is <c>ERROR</c> enabled, then it converts the message object
        ///     (passed as parameter) to a string by invoking the appropriate
        ///     It then proceeds to call all the registered appenders in this logger
        ///     and also higher in the hierarchy depending on the value of
        ///     the additivity flag.
        /// </summary>
        /// <param name="sender">The sender</param>
        /// <param name="message">passed message</param>
        public static void Error(object sender, object message)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());

                if (log.IsErrorEnabled)
                {
                    log.Error(message);
                }
            }
        }

        /// <summary>
        ///     writes a log entry on level ERROR including the stack trace
        ///     This method first checks if this logger is <c>ERROR</c>
        ///     enabled by comparing the level of this logger with the
        ///     level. If this logger is <c>ERROR</c> enabled, then it converts the message object
        ///     (passed as parameter) to a string by invoking the appropriate
        ///     It then proceeds to call all the registered appenders in this logger
        ///     and also higher in the hierarchy depending on the value of
        ///     the additivity flag.
        /// </summary>
        /// <param name="sender">The sender</param>
        /// <param name="message">passed message</param>
        /// <param name="e">The exception to log, including its stack trace.</param>
        public static void Error(object sender, object message, Exception e)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());

                if (log.IsErrorEnabled)
                {
                    log.Error(message, e);
                }
            }
        }

        #endregion Error

        #region Fatal

        /// <summary>
        ///     writes a log entry on level FATAL
        ///     This method first checks if this logger is <c>FATAL</c>
        ///     enabled by comparing the level of this logger with the
        ///     level. If this logger is <c>FATAL</c> enabled, then it converts the message object
        ///     (passed as parameter) to a string by invoking the appropriate
        ///     It then proceeds to call all the registered appenders in this logger
        ///     and also higher in the hierarchy depending on the value of
        ///     the additivity flag.
        /// </summary>
        /// <param name="sender">The sender</param>
        /// <param name="message">passed message</param>
        public static void Fatal(object sender, object message)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());

                if (log.IsFatalEnabled)
                {
                    log.Fatal(message);
                }
            }
        }

        /// <summary>
        ///     writes a log entry on level FATAL including the stack trace
        ///     This method first checks if this logger is <c>FATAL</c>
        ///     enabled by comparing the level of this logger with the
        ///     level. If this logger is <c>FATAL</c> enabled, then it converts the message object
        ///     (passed as parameter) to a string by invoking the appropriate
        ///     It then proceeds to call all the registered appenders in this logger
        ///     and also higher in the hierarchy depending on the value of
        ///     the additivity flag.
        /// </summary>
        /// <param name="sender">The sender</param>
        /// <param name="message">passed message</param>
        /// <param name="e">The exception to log, including its stack trace.</param>
        public static void Fatal(object sender, object message, Exception e)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger((sender is Type) ? (Type)sender : sender.GetType());

                if (log.IsFatalEnabled)
                {
                    log.Fatal(message, e);
                }
            }
        }

        #endregion Fatal

        #region logging Performance

        /// <summary>
        ///     logging of informations concerning performance
        /// </summary>
        /// <param name="notice">passed notice</param>
        /// <param name="endTick">passed end tick</param>
        /// <param name="startTick">passed end tick</param>
        public static void Perfomance(string notice, long endTick, long startTick)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger("Performance");

                if (log.IsInfoEnabled)
                {
                    long delta = endTick - startTick;
                    log.Info("[Perf] " + notice + " [ Ticks : " + delta + " ]");
                }
            }
        }

        /// <summary>
        ///     logging with exception of informations concerning performance
        /// </summary>
        /// <param name="notice">passed notice</param>
        /// <param name="endTick">passed end tick</param>
        /// <param name="startTick">passed start tick</param>
        /// <param name="e">passed exception object</param>
        public static void Perfomance(string notice, long endTick, long startTick, System.Exception e)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger("Performance");

                if (log.IsInfoEnabled)
                {
                    long delta = endTick - startTick;
                    log.Info("[Perf] " + notice + " [ Ticks : " + delta + " ]", e);
                }
            }
        }

        #endregion logging Performance

        #region logging Test

        /// <summary>
        ///     logging for the test classes
        /// </summary>
        /// <param name="message">message</param>
        public static void Test(object message)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger("Test-Logger");
                log.Info(message);
            }
        }

        /// <summary>
        ///     logging with exception object for the test classes
        /// </summary>
        /// <param name="message">passed message</param>
        /// <param name="t">passed exception</param>
        public static void Test(object message, Exception t)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger("Test-Logger");
                log.Info(message, t);
            }
        }

        #endregion logging Test

        #region logging Configuration

        /// <summary>
        ///     logging of informations concerning configuration
        /// </summary>
        /// <param name="message">passed message</param>
        public static void ConfigurationInfo(object message)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger("Configuration");
                log.Info(message);
            }
        }

        /// <summary>
        ///     logging with exception object of informations concerning configuration
        /// </summary>
        /// <param name="message">passed message</param>
        /// <param name="t">passed exception object</param>
        public static void ConfigurationInfo(object message, Exception t)
        {
            if (isConfigurate == false)
            {
                configurateByFile();
            }

            if (isConfigurate)
            {
                ILog log = LogManager.GetLogger("Configuration");
                log.Info(message, t);
            }
        }

        #endregion logging Configuration

        #region replacement of environment variables

        /// <summary>
        ///     replaces environment variables of any type in given filename
        /// </summary>
        /// <param name="LogConfig">file info of the current configuration file</param>
        private static FileInfo ReplaceEnvironmentVariables(FileInfo LogConfig)
        {
            FileInfo result;

            StreamReader reader;
            StreamWriter writer;
            String line;
            String content = String.Empty;
            int startIndex, endIndex;
            String variableName, variableValue;
            String filename = LogConfig.FullName;

            // read the original config file, replace environment variables and save this content
            reader = new StreamReader(filename);
            while (!reader.EndOfStream)
            {
                line = reader.ReadLine();
                if ((startIndex = line.IndexOf("%")) > 0)
                {
                    variableName = null;
                    variableValue = null;

                    // environment variable found in config file, extract the name
                    endIndex = line.LastIndexOf("%", startIndex + 1);

                    if (endIndex > startIndex)
                        variableName = line.Substring(startIndex + 1, endIndex - startIndex - 1);

                    if (variableName != null)
                    {
                        //// find out the value of this variable
                        //variableValue =
                        //    EnvironmentHelper.GetEnvironmentVariable(EnvironmentVariableTarget.User,
                        //                                                        variableName);
                        //// if not found in user environment, search in system environment
                        //if (variableValue == null)
                        //    variableValue =
                        //        EnvironmentHelper.GetEnvironmentVariable(EnvironmentVariableTarget.Machine,
                        //                                                            variableName);
                        //// if not found in machine environment, search in process environment
                        //if (variableValue == null)
                        //    variableValue =
                        //        EnvironmentHelper.GetEnvironmentVariable(EnvironmentVariableTarget.Process,
                        //                                                            variableName);
                    }
                    // create content
                    if (variableValue != null)
                    {
                        // replace in stream
                        content += line.Replace("%" + variableName + "%", variableValue) + System.Environment.NewLine;
                    }
                    else
                    {
                        // use original one
                        content += line + System.Environment.NewLine;
                    }
                }
                else
                {
                    content += line + System.Environment.NewLine;
                }
            }
            reader.Close();

            // find out the temp directory (try user first, then try machine)
            variableValue = null;
            //EnvironmentHelper.GetEnvironmentVariable(EnvironmentVariableTarget.User, "TEMP");
            //if (variableValue == null)
            //    variableValue = EnvironmentHelper.GetEnvironmentVariable(EnvironmentVariableTarget.User,
            //                                                                        "TMP");
            //if (variableValue == null)
            //    variableValue = EnvironmentHelper.GetEnvironmentVariable(EnvironmentVariableTarget.Machine,
            //                                                                        "TEMP");
            //if (variableValue == null)
            //    variableValue = EnvironmentHelper.GetEnvironmentVariable(EnvironmentVariableTarget.Machine,
            //                                                                        "TMP");

            if (variableValue == null)
            {
                // no temporary path found
                writer = new StreamWriter(LogConfig.FullName);
                result = LogConfig;
            }
            else
            {
                // temporary path found, use it
                writer = new StreamWriter(variableValue + "\\" + LogConfig.Name);
                result = new FileInfo(variableValue + "\\" + LogConfig.Name);
            }

            writer.Write(content);
            writer.Close();

            return result;
        }

        #endregion replacement of environment variables
    }
}