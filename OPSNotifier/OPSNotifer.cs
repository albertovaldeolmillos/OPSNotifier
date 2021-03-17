﻿using System;
using OPS.Comm;

namespace OPSNotifier
{
    class OPSNotifer
    {
        static ILogger _logger;
        private static string OPS_DB_CONNECTION_STRING;
        private static Notifier oNotifier = null;

        public static ILogger Logger
        {
            get { return _logger; }
        }

        static void Main(string[] args)
        {
            if (Init())
            {
                oNotifier.Notify();
            }

            Logger_AddLogMessage("Exiting application", LoggerSeverities.Info);
        }

        private static bool Init()
        {
            bool bReturn = false;
            try
            {
                _logger = new FileLogger(LoggerSeverities.Debug, "c:\\temp\\{0}_OPSNotifier.log");
                LoggerSeverities logSeverity = ReadLoggerSeverity();
                Logger_AddLogMessage(string.Format("Setting logger severity to: {0} ", logSeverity.ToString()), LoggerSeverities.Info);
                _logger.Severity = logSeverity;

                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                OPS_DB_CONNECTION_STRING = (string)appSettings.GetValue("OPSConnectionString", typeof(string));
                Logger_AddLogMessage(string.Format("OPS Connection String: {0} ", OPS_DB_CONNECTION_STRING.ToString()), LoggerSeverities.Info);
                oNotifier = new Notifier();
                bReturn = oNotifier.Init(OPS_DB_CONNECTION_STRING, _logger);
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
                bReturn = false;
            }

            return bReturn;
        }

        private static void Logger_AddLogMessage(string msg, LoggerSeverities severity)
        {
            _logger.AddLog(msg, severity);
        }

        private static void Logger_AddLogException(Exception ex)
        {
            _logger.AddLog(ex);
        }

        /// <summary>
        /// Reads logger severity level from the app.config file
        /// </summary>
        private static LoggerSeverities ReadLoggerSeverity()
        {
            LoggerSeverities logSeverity = LoggerSeverities.Error;
            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                string logLevel = (string)appSettings.GetValue("LoggerSeverity", typeof(string));
                if (logLevel != null)
                {
                    logSeverity = (LoggerSeverities)Enum.Parse(
                        typeof(LoggerSeverities), logLevel, true);
                }
            }
            catch { }

            return logSeverity;
        }
    }
}
