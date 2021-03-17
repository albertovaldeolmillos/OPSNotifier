using System;
using System.Collections.Generic;
using System.Text;
using System.Data.OracleClient;
using System.Data.SqlClient;
using System.Net;
using OPS.Comm;
using System.IO;
using PushSharp;
using PushSharp.Apple;
using PushSharp.Core;
using System.Linq;
using System.Threading;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;

namespace OPSNotifier
{
    class Notifier
    {
        protected string OPS_DB_CONNECTION_STRING;
        protected OPS.Comm.ILogger _logger;
        protected int _iMaxRegisters = 100;
        protected OracleConnection OPSBDCon = null;
        protected OracleTransaction oraOPSTransaction = null;

        public class MessageData
        {
            public string registration_id { get; set; }
            public string userName { get; set; }
            public string type { get; set; }
            public string description { get; set; }
            public string plate { get; set; }
            public string operation { get; set; }

            public MessageData(string registration_id, string userName, string type, string description, string plate, string operation)
            {
                this.registration_id = registration_id;
                this.userName = userName;
                this.type = type;
                this.description = description;
                this.plate = plate;
                this.operation = operation;
            }

            public string GetMessage()
            {
                string message = "{\"data\": {\"type\": \"" + type + "\", \"description\": \"" + description + "\", \"user\": \"" + userName + "\", \"plate\": \"" + plate + "\", \"on\": \"" + operation + "\"},\"registration_ids\":[\"" + registration_id + "\"] }";

                return message;
            }
        }

        public class AppleNotifier
        {

            PushBroker _push = null;

            public AppleNotifier()
            {
                _push = new PushBroker();

                //System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                //string strCertPath = appSettings.GetValue("iOS.CertPath", typeof(string)).ToString();
                //var appleCert = File.ReadAllBytes(strCertPath);
                //string strPassword = appSettings.GetValue("iOS.CertPassword", typeof(string)).ToString();
                //_push.RegisterAppleService(new ApplePushChannelSettings(true, appleCert, strPassword));

                var thumbprint = "F7887DBE384F7013173959A3F987C5191E42F7A0";
                var cert = FindApnsCert(thumbprint);
                //bool isProduction = cert.SubjectName.Name.Contains("Production");
                bool isProduction = true;
                _push.RegisterService<AppleNotification>(new ApplePushService(new ApplePushChannelSettings(isProduction, cert)));

            }

            ~AppleNotifier()
            {
                _push.StopAllServices(waitForQueuesToFinish: true);
            }

            private X509Certificate2 FindApnsCert(string thumbprint)
            {
                var store = new X509Store(StoreName.My, StoreLocation.LocalMachine);
                store.Open(OpenFlags.OpenExistingOnly | OpenFlags.ReadOnly);

                var cert = store.Certificates
                    .Cast<X509Certificate2>()
                    .SingleOrDefault(c => string.Equals(c.Thumbprint, thumbprint, StringComparison.OrdinalIgnoreCase));

                if (cert == null)
                    throw new Exception("No certificate with thumprint: " + thumbprint);

                return cert;
            }

            public bool SendNotification(MessageData message, out string strErrorMsg)
            {
                bool bResult = false;

                strErrorMsg = "";

                try
                {
                    AppleNotificationPayload payLoad = new AppleNotificationPayload(message.description);
                    payLoad.AddCustom("type", message.type);
                    payLoad.AddCustom("plate", message.plate);
                    payLoad.AddCustom("user", message.userName);
                    payLoad.AddCustom("on", message.operation);
                    payLoad.Sound = "default";

                    _push.QueueNotification(new AppleNotification(message.registration_id, payLoad));
                    bResult = true;
                }
                catch (Exception ex)
                {
                    strErrorMsg = ex.Message;
                }

                return bResult;
            }
        }

        public Notifier()
        {
        }

        public bool Init(string OPSDBConnectionString, OPS.Comm.ILogger logger)
        {
            bool bReturn = true;
            OPS_DB_CONNECTION_STRING = OPSDBConnectionString;
            _logger = logger;
            System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
            _iMaxRegisters = Convert.ToInt32(appSettings.GetValue("MaxRegisters", typeof(Int32)));

            return bReturn;
        }

        public bool Notify()
        {
            bool bReturn = true;

            bool bNotifyParkings = false;
            bool bNotifyRecharges = false;
            bool bNotifyFines = false;
            bool bNotifyBalance = false;
            bool bNotifyResidents = false;

            try
            {
                SendAppleNotification("e682e6cb912ed8ed6d23b53525b3d87f5c06fa959df2c8668ee419340795fb0d", "Prueba de envío OTS");

                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                if (Convert.ToInt32(appSettings.GetValue("NotifyParkings", typeof(Int32))) == 1)
                {
                    bNotifyParkings = true;
                    Logger_AddLogMessage("Enable parking notifications", LoggerSeverities.Info);
                }
                if (Convert.ToInt32(appSettings.GetValue("NotifyRecharges", typeof(Int32))) == 1)
                {
                    bNotifyRecharges = true;
                    Logger_AddLogMessage("Enable recharge notifications", LoggerSeverities.Info);
                }
                if (Convert.ToInt32(appSettings.GetValue("NotifyFines", typeof(Int32))) == 1)
                {
                    bNotifyFines = true;
                    Logger_AddLogMessage("Enable fine notifications", LoggerSeverities.Info);
                }
                if (Convert.ToInt32(appSettings.GetValue("NotifyBalance", typeof(Int32))) == 1)
                {
                    bNotifyBalance = true;
                    Logger_AddLogMessage("Enable balance notifications", LoggerSeverities.Info);
                }
                if (Convert.ToInt32(appSettings.GetValue("NotifyResidents", typeof(Int32))) == 1)
                {
                    bNotifyResidents = true;
                    Logger_AddLogMessage("Enable resident notifications", LoggerSeverities.Info);
                }

                if (OpenDB())
                {
                    if (bNotifyParkings)
                        NotifyParkings();

                    if (bNotifyRecharges)
                        NotifyRecharges();

                    if (bNotifyFines)
                        NotifyFines();

                    if (bNotifyBalance)
                        NotifyBalance();

                    if (bNotifyResidents)
                    {
                        //NotifyResidents5Days();
                        //NotifyResidents15Days();
                        //NotifyResidents30Days();
                    }

                    GenerateNotifications();
                }
                else
                    bReturn = false;

            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
                bReturn = false;
            }

            finally
            {
                CloseDB();
            }

            return bReturn;
        }

        private bool NotifyParkings()
        {
            bool bReturn = true;
            OracleDataReader dr = null;
            OracleCommand selOperationssCmd = null;
            int nMobileUser = -1;
            int nType = -1;
            int nOperId = -1;
            string strCloudToken = "";
            int nOS = -1;
            string strPlate = "";
            string strEnddate = "";
            int nIndex = 0;

            string sSQLCommand = "";

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                nType = Convert.ToInt32(appSettings.GetValue("NotificationType.Parking", typeof(Int32)));

                oraOPSTransaction = OPSBDCon.BeginTransaction();

                selOperationssCmd = new OracleCommand();
                selOperationssCmd.Connection = OPSBDCon;
                selOperationssCmd.Transaction = oraOPSTransaction;

                selOperationssCmd.CommandText = "select mu_id, mup_last_parking_oper, mup_device_os, mup_cloud_token, ope_vehicleid, to_char(ope_enddate, 'hh24:mi') as ope_enddate from mobile_users, mobile_users_plates, operations "
                                                + "where mup_mu_id = mu_id and mup_last_parking_oper = ope_id and ope_dope_id in (1,2) and "
                                                + "sysdate > ope_enddate - ( mu_unpark_notify_time / 1440 ) and "
                                                + "ope_enddate > mu_insertion_date and "
                                                + "mu_activate_account = 1 and mu_deleted = 0 and "
                                                + "mu_unpark_notify = 1 and ope_notified = 0 and mup_device_os is not null and mup_cloud_token is not null order by mu_id";

                dr = selOperationssCmd.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        nMobileUser = dr.GetInt32(dr.GetOrdinal("MU_ID"));
                        nOperId = dr.GetInt32(dr.GetOrdinal("MUP_LAST_PARKING_OPER"));
                        strCloudToken = dr.GetString(dr.GetOrdinal("MUP_CLOUD_TOKEN"));
                        nOS = dr.GetInt32(dr.GetOrdinal("MUP_DEVICE_OS"));
                        strPlate = dr.GetString(dr.GetOrdinal("OPE_VEHICLEID"));
                        strEnddate = dr.GetString(dr.GetOrdinal("OPE_ENDDATE"));

                        Logger_AddLogMessage(string.Format("Found parking notification for user: {0}, operation: {1}", nMobileUser, nOperId),
                                            LoggerSeverities.Info);

                        if (!InsertNotification(nMobileUser, nType, strCloudToken, nOS, strPlate, nOperId.ToString(), strEnddate))
                            throw (new Exception(string.Format("Error inserting parking notification for user: {0}", nMobileUser)));

                        if (!UpdateOperNotifyFlag(nOperId))
                            throw (new Exception(string.Format("Error updating notification flag for operation: {0}", nOperId)));

                        if (++nIndex >= _iMaxRegisters)
                            break;
                    }

                    Commit();
                }
            }
            catch (Exception e)
            {
                Logger_AddLogMessage(sSQLCommand, LoggerSeverities.Error);
                Logger_AddLogException(e);
                Rollback();
            }

            finally
            {
                if (dr != null)
                {
                    dr.Close();
                    dr.Dispose();
                    dr = null;
                }

                if (selOperationssCmd != null)
                {
                    selOperationssCmd.Dispose();
                    selOperationssCmd = null;
                }

                if (oraOPSTransaction != null)
                {
                    oraOPSTransaction.Dispose();
                    oraOPSTransaction = null;
                }
            }

            return bReturn;
        }

        private bool NotifyRecharges()
        {
            bool bReturn = true;
            OracleDataReader dr = null;
            OracleCommand selOperationssCmd = null;
            int nMobileUser = -1;
            int nType = -1;
            int nOperId = -1;
            string strCloudToken = "";
            int nOS = -1;
            string strPlate = "";
            int nIndex = 0;

            string sSQLCommand = "";

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                nType = Convert.ToInt32(appSettings.GetValue("NotificationType.Recharge", typeof(Int32)));

                oraOPSTransaction = OPSBDCon.BeginTransaction();

                selOperationssCmd = new OracleCommand();
                selOperationssCmd.Connection = OPSBDCon;
                selOperationssCmd.Transaction = oraOPSTransaction;

                selOperationssCmd.CommandText = "select ope_mobi_user_id, ope_id, mu_device_os, mu_cloud_token from operations, mobile_users "
                                                + "where ope_mobi_user_id = mu_id and "
                                                + "ope_dope_id = 5 and ope_mobi_user_id is not null and "
                                                + "mu_activate_account = 1 and mu_deleted = 0 and "
                                                + "mu_recharge_notify = 1 and ope_notified = 0 and mu_device_os is not null and mu_cloud_token is not null order by mu_id";

                dr = selOperationssCmd.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        nMobileUser = dr.GetInt32(dr.GetOrdinal("OPE_MOBI_USER_ID"));
                        nOperId = dr.GetInt32(dr.GetOrdinal("OPE_ID"));
                        strCloudToken = dr.GetString(dr.GetOrdinal("MU_CLOUD_TOKEN"));
                        nOS = dr.GetInt32(dr.GetOrdinal("MU_DEVICE_OS"));

                        Logger_AddLogMessage(string.Format("Found recharge notification for user: {0}, operation: {1}", nMobileUser, nOperId),
                                            LoggerSeverities.Info);

                        if (!InsertNotification(nMobileUser, nType, strCloudToken, nOS, strPlate, nOperId.ToString()))
                            throw (new Exception(string.Format("Error inserting recharge notification for user: {0}", nMobileUser)));

                        if (!UpdateOperNotifyFlag(nOperId))
                            throw (new Exception(string.Format("Error updating notification flag for operation: {0}", nOperId)));

                        if (++nIndex >= _iMaxRegisters)
                            break;
                    }

                    Commit();
                }
            }
            catch (Exception e)
            {
                Logger_AddLogMessage(sSQLCommand, LoggerSeverities.Error);
                Logger_AddLogException(e);
                Rollback();
            }

            finally
            {
                if (dr != null)
                {
                    dr.Close();
                    dr.Dispose();
                    dr = null;
                }

                if (selOperationssCmd != null)
                {
                    selOperationssCmd.Dispose();
                    selOperationssCmd = null;
                }

                if (oraOPSTransaction != null)
                {
                    oraOPSTransaction.Dispose();
                    oraOPSTransaction = null;
                }
            }

            return bReturn;
        }

        private bool NotifyFines()
        {
            bool bReturn = true;
            OracleDataReader dr = null;
            OracleCommand selOperationssCmd = null;
            int nMobileUser = -1;
            int nType = -1;
            int nFineId = -1;
            string strCloudToken = "";
            int nOS = -1;
            string strPlate = "";
            int nIndex = 0;

            string sSQLCommand = "";

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                nType = Convert.ToInt32(appSettings.GetValue("NotificationType.Fine", typeof(Int32)));

                oraOPSTransaction = OPSBDCon.BeginTransaction();

                selOperationssCmd = new OracleCommand();
                selOperationssCmd.Connection = OPSBDCon;
                selOperationssCmd.Transaction = oraOPSTransaction;

                selOperationssCmd.CommandText = "select mu_id, fin_id, mup_device_os, mup_cloud_token, mu_device_os, mu_cloud_token from fines, mobile_users, mobile_users_plates "
                                                + "where fin_vehicleid = mup_plate and mup_mu_id = mu_id and "
                                                + "mu_activate_account = 1 and mu_deleted = 0 and "
                                                + "fin_date > mu_insertion_date and "
                                                + "mu_fine_notify = 1 and fin_notified = 0 and mu_device_os is not null and mu_cloud_token is not null order by mu_id";

                dr = selOperationssCmd.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        nMobileUser = dr.GetInt32(dr.GetOrdinal("MU_ID"));
                        nFineId = dr.GetInt32(dr.GetOrdinal("FIN_ID"));
                        if (dr.IsDBNull(2) || dr.IsDBNull(3))
                        {
                            strCloudToken = dr.GetString(dr.GetOrdinal("MU_CLOUD_TOKEN"));
                            nOS = dr.GetInt32(dr.GetOrdinal("MU_DEVICE_OS"));
                            Logger_AddLogMessage(string.Format("No token data for user plate, using mobile data"),
                                  LoggerSeverities.Info);
                        }
                        else
                        {
                            strCloudToken = dr.GetString(dr.GetOrdinal("MUP_CLOUD_TOKEN"));
                            nOS = dr.GetInt32(dr.GetOrdinal("MUP_DEVICE_OS"));
                            if (strCloudToken.Length == 0 || nOS <= 0)
                            {
                                strCloudToken = dr.GetString(dr.GetOrdinal("MU_CLOUD_TOKEN"));
                                nOS = dr.GetInt32(dr.GetOrdinal("MU_DEVICE_OS"));
                                Logger_AddLogMessage(string.Format("No token data for user plate, using mobile data"),
                                      LoggerSeverities.Info);
                            }
                        }

                        Logger_AddLogMessage(string.Format("Found fine notification for user: {0}, fine: {1}", nMobileUser, nFineId),
                                            LoggerSeverities.Info);

                        if (!InsertNotification(nMobileUser, nType, strCloudToken, nOS, strPlate, nFineId.ToString()))
                            throw (new Exception(string.Format("Error inserting fine notification for user: {0}", nMobileUser)));

                        if (!UpdateFineNotifyFlag(nFineId))
                            throw (new Exception(string.Format("Error updating notification flag for fine: {0}", nFineId)));

                        if (++nIndex >= _iMaxRegisters)
                            break;
                    }

                    Commit();
                }
            }
            catch (Exception e)
            {
                Logger_AddLogMessage(sSQLCommand, LoggerSeverities.Error);
                Logger_AddLogException(e);
                Rollback();
            }

            finally
            {
                if (dr != null)
                {
                    dr.Close();
                    dr.Dispose();
                    dr = null;
                }

                if (selOperationssCmd != null)
                {
                    selOperationssCmd.Dispose();
                    selOperationssCmd = null;
                }

                if (oraOPSTransaction != null)
                {
                    oraOPSTransaction.Dispose();
                    oraOPSTransaction = null;
                }
            }

            return bReturn;
        }

        private bool NotifyBalance()
        {
            bool bReturn = true;
            OracleDataReader dr = null;
            OracleCommand selOperationssCmd = null;
            int nMobileUser = -1;
            int nType = -1;
            string strCloudToken = "";
            int nOS = -1;
            string strPlate = "";
            int nIndex = 0;

            string sSQLCommand = "";

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                nType = Convert.ToInt32(appSettings.GetValue("NotificationType.Balance", typeof(Int32)));

                oraOPSTransaction = OPSBDCon.BeginTransaction();

                selOperationssCmd = new OracleCommand();
                selOperationssCmd.Connection = OPSBDCon;
                selOperationssCmd.Transaction = oraOPSTransaction;

                selOperationssCmd.CommandText = "select mu_id, mu_device_os, mu_cloud_token from mobile_users "
                                                + "where mu_activate_account = 1 and mu_deleted = 0 and "
                                                + " mu_funds <= mu_balance_notify_amount and "
                                                + "mu_balance_notify = 1 and mu_notified = 0 and mu_device_os is not null and mu_cloud_token is not null order by mu_id";

                dr = selOperationssCmd.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        nMobileUser = dr.GetInt32(dr.GetOrdinal("MU_ID"));
                        strCloudToken = dr.GetString(dr.GetOrdinal("MU_CLOUD_TOKEN"));
                        nOS = dr.GetInt32(dr.GetOrdinal("MU_DEVICE_OS"));

                        Logger_AddLogMessage(string.Format("Found balance notification for user: {0}", nMobileUser),
                                            LoggerSeverities.Info);

                        if (!InsertNotification(nMobileUser, nType, strCloudToken, nOS, strPlate))
                            throw (new Exception(string.Format("Error inserting balance notification for user: {0}", nMobileUser)));

                        if (!UpdateUserNotifyFlag(nMobileUser))
                            throw (new Exception(string.Format("Error updating notification flag for user: {0}", nMobileUser)));

                        if (++nIndex >= _iMaxRegisters)
                            break;
                    }

                    Commit();
                }
            }
            catch (Exception e)
            {
                Logger_AddLogMessage(sSQLCommand, LoggerSeverities.Error);
                Logger_AddLogException(e);
                Rollback();
            }

            finally
            {
                if (dr != null)
                {
                    dr.Close();
                    dr.Dispose();
                    dr = null;
                }

                if (selOperationssCmd != null)
                {
                    selOperationssCmd.Dispose();
                    selOperationssCmd = null;
                }

                if (oraOPSTransaction != null)
                {
                    oraOPSTransaction.Dispose();
                    oraOPSTransaction = null;
                }
            }

            return bReturn;
        }

        private bool NotifyResidents5Days()
        {
            bool bReturn = true;
            OracleDataReader drResidents = null;
            OracleCommand selResidentsCmd = null;
            OracleDataReader drUsers = null;
            OracleCommand selUsersCmd = null;
            int nResId = -1;
            string strPlate = "";
            int nMobileUser = -1;
            int nType = -1;
            string strCloudToken = "";
            int nOS = -1;
            string strDate = "";
            int nIndex = 0;
            string sSQLCommand = "";

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                nType = Convert.ToInt32(appSettings.GetValue("NotificationType.ResWarn", typeof(Int32)));

                oraOPSTransaction = OPSBDCon.BeginTransaction();

                selResidentsCmd = new OracleCommand();
                selResidentsCmd.Connection = OPSBDCon;
                selResidentsCmd.Transaction = oraOPSTransaction;

                // First, find residents with title that's about to expire
                sSQLCommand = "select resd_id, resd_vehicleid, rtrim(to_char(last_day( add_months( resd_req_date, 12 )), 'dd/mm/yy')) as exp_date "
                                + "from residents_data "
                                + "where trunc( last_day( add_months( resd_req_date, 12 )) - sysdate ) between 1 and 5 and "
                                + "resd_actived = " + appSettings.GetValue("ResActived.Active", typeof(string)) + " and "
                                + "resd_notified <= " + appSettings.GetValue("ResNotifState.15day", typeof(string))
                                + "order by resd_id";

                selResidentsCmd.CommandText = sSQLCommand;
                drResidents = selResidentsCmd.ExecuteReader();

                if (drResidents.HasRows)
                {
                    while (drResidents.Read())
                    {
                        nResId = drResidents.GetInt32(drResidents.GetOrdinal("RESD_ID"));
                        strPlate = drResidents.GetString(drResidents.GetOrdinal("RESD_VEHICLEID"));
                        strDate = drResidents.GetString(drResidents.GetOrdinal("EXP_DATE"));

                        Logger_AddLogMessage(string.Format("Found possible 5 day notification for resident: {0}, plate: {1}", nResId, strPlate),
                                            LoggerSeverities.Info);

                        // Next, look for all the users with the same license plate as the owner of the resident title
                        selUsersCmd = new OracleCommand();
                        selUsersCmd.Connection = OPSBDCon;
                        selUsersCmd.Transaction = oraOPSTransaction;

                        sSQLCommand = "select mu_id, mu_device_os, mu_cloud_token "
                                        + "from mobile_users, mobile_users_plates "
                                        + "where mu_id = mup_mu_id and mup_plate = '" + strPlate + "' and "
                                        + "mu_activate_account = 1 and mu_deleted = 0 and mup_deleted = 0 and "
                                        + "mu_device_os is not null and mu_cloud_token is not null order by mu_id";

                        selUsersCmd.CommandText = sSQLCommand;
                        drUsers = selUsersCmd.ExecuteReader();

                        if (drUsers.HasRows)
                        {
                            while (drUsers.Read())
                            {
                                nMobileUser = drUsers.GetInt32(drUsers.GetOrdinal("MU_ID"));
                                strCloudToken = drUsers.GetString(drUsers.GetOrdinal("MU_CLOUD_TOKEN"));
                                nOS = drUsers.GetInt32(drUsers.GetOrdinal("MU_DEVICE_OS"));

                                Logger_AddLogMessage(string.Format("Found resident 5 day notification for user: {0}", nMobileUser),
                                                    LoggerSeverities.Info);

                                if (!InsertTextNotification(nMobileUser, nType, strCloudToken, nOS, strDate))
                                    throw (new Exception(string.Format("Error inserting resident notification for user: {0}", nMobileUser)));

                                nIndex++;
                            }

                            if (!UpdateResNotifyFlag(nResId, Convert.ToInt32(appSettings.GetValue("ResNotifState.5day", typeof(Int32)))))
                                throw (new Exception(string.Format("Error updating notification flag for resident: {0}", nResId)));
                        }
                        else
                        {
                            Logger_AddLogMessage(string.Format("No mobile users were found for resident: {0}, setting 5 day flag", nResId),
                                                    LoggerSeverities.Info);

                            if (!UpdateResNotifyFlag(nResId, Convert.ToInt32(appSettings.GetValue("ResNotifState.5day", typeof(Int32)))))
                                throw (new Exception(string.Format("Error updating notification flag for resident: {0}", nResId)));

                            nIndex++;
                        }

                        if (nIndex >= _iMaxRegisters)
                            break;
                    }

                    if (nIndex > 0)
                        Commit();
                }
            }
            catch (Exception e)
            {
                Logger_AddLogMessage(sSQLCommand, LoggerSeverities.Error);
                Logger_AddLogException(e);
                Rollback();
            }

            finally
            {
                if (drResidents != null)
                {
                    drResidents.Close();
                    drResidents.Dispose();
                    drResidents = null;
                }

                if (drUsers != null)
                {
                    drUsers.Close();
                    drUsers.Dispose();
                    drUsers = null;
                }

                if (selResidentsCmd != null)
                {
                    selResidentsCmd.Dispose();
                    selResidentsCmd = null;
                }

                if (selUsersCmd != null)
                {
                    selUsersCmd.Dispose();
                    selUsersCmd = null;
                }

                if (oraOPSTransaction != null)
                {
                    oraOPSTransaction.Dispose();
                    oraOPSTransaction = null;
                }
            }

            return bReturn;
        }

        private bool NotifyResidents15Days()
        {
            bool bReturn = true;
            OracleDataReader drResidents = null;
            OracleCommand selResidentsCmd = null;
            OracleDataReader drUsers = null;
            OracleCommand selUsersCmd = null;
            int nResId = -1;
            string strPlate = "";
            int nMobileUser = -1;
            int nType = -1;
            string strCloudToken = "";
            int nOS = -1;
            string strDate = "";
            int nIndex = 0;
            string sSQLCommand = "";

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                nType = Convert.ToInt32(appSettings.GetValue("NotificationType.ResWarn", typeof(Int32)));

                oraOPSTransaction = OPSBDCon.BeginTransaction();

                selResidentsCmd = new OracleCommand();
                selResidentsCmd.Connection = OPSBDCon;
                selResidentsCmd.Transaction = oraOPSTransaction;

                // First, find residents with title that's about to expire
                sSQLCommand = "select resd_id, resd_vehicleid, rtrim(to_char(last_day( add_months( resd_req_date, 12 )), 'dd/mm/yy')) as exp_date "
                                + "from residents_data "
                                + "where trunc( last_day( add_months( resd_req_date, 12 )) - sysdate ) between 6 and 15 and "
                                + "resd_actived = " + appSettings.GetValue("ResActived.Active", typeof(string)) + " and "
                                + "resd_notified <= " + appSettings.GetValue("ResNotifState.30day", typeof(string))
                                + "order by resd_id";

                selResidentsCmd.CommandText = sSQLCommand;
                drResidents = selResidentsCmd.ExecuteReader();

                if (drResidents.HasRows)
                {
                    while (drResidents.Read())
                    {
                        nResId = drResidents.GetInt32(drResidents.GetOrdinal("RESD_ID"));
                        strPlate = drResidents.GetString(drResidents.GetOrdinal("RESD_VEHICLEID"));
                        strDate = drResidents.GetString(drResidents.GetOrdinal("EXP_DATE"));

                        Logger_AddLogMessage(string.Format("Found possible 15 day notification for resident: {0}, plate: {1}", nResId, strPlate),
                                            LoggerSeverities.Info);

                        // Next, look for all the users with the same license plate as the owner of the resident title
                        selUsersCmd = new OracleCommand();
                        selUsersCmd.Connection = OPSBDCon;
                        selUsersCmd.Transaction = oraOPSTransaction;

                        sSQLCommand = "select mu_id, mu_device_os, mu_cloud_token "
                                        + "from mobile_users, mobile_users_plates "
                                        + "where mu_id = mup_mu_id and mup_plate = '" + strPlate + "' and "
                                        + "mu_activate_account = 1 and mu_deleted = 0 and mup_deleted = 0 and "
                                        + "mu_device_os is not null and mu_cloud_token is not null order by mu_id";

                        selUsersCmd.CommandText = sSQLCommand;
                        drUsers = selUsersCmd.ExecuteReader();

                        if (drUsers.HasRows)
                        {
                            while (drUsers.Read())
                            {
                                nMobileUser = drUsers.GetInt32(drUsers.GetOrdinal("MU_ID"));
                                strCloudToken = drUsers.GetString(drUsers.GetOrdinal("MU_CLOUD_TOKEN"));
                                nOS = drUsers.GetInt32(drUsers.GetOrdinal("MU_DEVICE_OS"));

                                Logger_AddLogMessage(string.Format("Found resident 15 day notification for user: {0}", nMobileUser),
                                                    LoggerSeverities.Info);

                                if (!InsertTextNotification(nMobileUser, nType, strCloudToken, nOS, strDate))
                                    throw (new Exception(string.Format("Error inserting resident notification for user: {0}", nMobileUser)));

                                nIndex++;
                            }

                            if (!UpdateResNotifyFlag(nResId, Convert.ToInt32(appSettings.GetValue("ResNotifState.15day", typeof(Int32)))))
                                throw (new Exception(string.Format("Error updating notification flag for resident: {0}", nResId)));
                        }
                        else
                        {
                            Logger_AddLogMessage(string.Format("No mobile users were found for resident: {0}, setting 15 day flag", nResId),
                                                    LoggerSeverities.Info);

                            if (!UpdateResNotifyFlag(nResId, Convert.ToInt32(appSettings.GetValue("ResNotifState.15day", typeof(Int32)))))
                                throw (new Exception(string.Format("Error updating notification flag for resident: {0}", nResId)));

                            nIndex++;
                        }

                        if (nIndex >= _iMaxRegisters)
                            break;
                    }

                    if (nIndex > 0)
                        Commit();
                }
            }
            catch (Exception e)
            {
                Logger_AddLogMessage(sSQLCommand, LoggerSeverities.Error);
                Logger_AddLogException(e);
                Rollback();
            }

            finally
            {
                if (drResidents != null)
                {
                    drResidents.Close();
                    drResidents.Dispose();
                    drResidents = null;
                }

                if (drUsers != null)
                {
                    drUsers.Close();
                    drUsers.Dispose();
                    drUsers = null;
                }

                if (selResidentsCmd != null)
                {
                    selResidentsCmd.Dispose();
                    selResidentsCmd = null;
                }

                if (selUsersCmd != null)
                {
                    selUsersCmd.Dispose();
                    selUsersCmd = null;
                }

                if (oraOPSTransaction != null)
                {
                    oraOPSTransaction.Dispose();
                    oraOPSTransaction = null;
                }
            }

            return bReturn;
        }

        private bool NotifyResidents30Days()
        {
            bool bReturn = true;
            OracleDataReader drResidents = null;
            OracleCommand selResidentsCmd = null;
            OracleDataReader drUsers = null;
            OracleCommand selUsersCmd = null;
            int nResId = -1;
            string strPlate = "";
            int nMobileUser = -1;
            int nType = -1;
            string strCloudToken = "";
            int nOS = -1;
            string strDate = "";
            int nIndex = 0;
            string sSQLCommand = "";

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                nType = Convert.ToInt32(appSettings.GetValue("NotificationType.ResWarn", typeof(Int32)));

                oraOPSTransaction = OPSBDCon.BeginTransaction();

                selResidentsCmd = new OracleCommand();
                selResidentsCmd.Connection = OPSBDCon;
                selResidentsCmd.Transaction = oraOPSTransaction;

                // First, find residents with title that's about to expire
                sSQLCommand = "select resd_id, resd_vehicleid, rtrim(to_char(last_day( add_months( resd_req_date, 12 )), 'dd/mm/yy')) as exp_date "
                                + "from residents_data "
                                + "where trunc( last_day( add_months( resd_req_date, 12 )) - sysdate ) between 16 and 30 and "
                                + "resd_actived = " + appSettings.GetValue("ResActived.Active", typeof(string)) + " and "
                                + "resd_notified = " + appSettings.GetValue("ResNotifState.Pending", typeof(string))
                                + "order by resd_id";

                selResidentsCmd.CommandText = sSQLCommand;
                drResidents = selResidentsCmd.ExecuteReader();

                if (drResidents.HasRows)
                {
                    while (drResidents.Read())
                    {
                        nResId = drResidents.GetInt32(drResidents.GetOrdinal("RESD_ID"));
                        strPlate = drResidents.GetString(drResidents.GetOrdinal("RESD_VEHICLEID"));
                        strDate = drResidents.GetString(drResidents.GetOrdinal("EXP_DATE"));

                        Logger_AddLogMessage(string.Format("Found possible 30 day notification for resident: {0}, plate: {1}", nResId, strPlate),
                                            LoggerSeverities.Info);

                        // Next, look for all the mobile users with the same license plate as the owner of the resident title
                        selUsersCmd = new OracleCommand();
                        selUsersCmd.Connection = OPSBDCon;
                        selUsersCmd.Transaction = oraOPSTransaction;

                        sSQLCommand = "select mu_id, mu_device_os, mu_cloud_token "
                                        + "from mobile_users, mobile_users_plates "
                                        + "where mu_id = mup_mu_id and mup_plate = '" + strPlate + "' and "
                                        + "mu_activate_account = 1 and mu_deleted = 0 and mup_deleted = 0 and "
                                        + "mu_device_os is not null and mu_cloud_token is not null order by mu_id";

                        selUsersCmd.CommandText = sSQLCommand;
                        drUsers = selUsersCmd.ExecuteReader();

                        if (drUsers.HasRows)
                        {
                            while (drUsers.Read())
                            {
                                nMobileUser = drUsers.GetInt32(drUsers.GetOrdinal("MU_ID"));
                                strCloudToken = drUsers.GetString(drUsers.GetOrdinal("MU_CLOUD_TOKEN"));
                                nOS = drUsers.GetInt32(drUsers.GetOrdinal("MU_DEVICE_OS"));

                                Logger_AddLogMessage(string.Format("Found resident 30 day notification for user: {0} with plate {1}", nMobileUser, strPlate),
                                                    LoggerSeverities.Info);

                                string strText = appSettings.GetValue("NotificationText.ResWarn", typeof(string)).ToString();
                                strText = string.Format(strText, strDate);

                                if (!InsertTextNotification(nMobileUser, nType, strCloudToken, nOS, strText))
                                    throw (new Exception(string.Format("Error inserting resident notification for user {0} with plate {1}", nMobileUser, strPlate)));
                            }

                            if (!UpdateResNotifyFlag(nResId, Convert.ToInt32(appSettings.GetValue("ResNotifState.30day", typeof(Int32)))))
                                throw (new Exception(string.Format("Error updating notification flag for resident: {0}", nResId)));

                            nIndex++;
                        }
                        else
                        {
                            Logger_AddLogMessage(string.Format("No mobile users/plates were found for resident {0} with plate {1}, setting 30 day flag", nResId, strPlate),
                                                    LoggerSeverities.Info);

                            if (!UpdateResNotifyFlag(nResId, Convert.ToInt32(appSettings.GetValue("ResNotifState.30day", typeof(Int32)))))
                                throw (new Exception(string.Format("Error updating notification flag for resident: {0}", nResId)));

                            nIndex++;
                        }

                        if (nIndex >= _iMaxRegisters)
                            break;
                    }

                    if (nIndex > 0)
                        Commit();
                }
            }
            catch (Exception e)
            {
                Logger_AddLogMessage(sSQLCommand, LoggerSeverities.Error);
                Logger_AddLogException(e);
                Rollback();
            }

            finally
            {
                if (drResidents != null)
                {
                    drResidents.Close();
                    drResidents.Dispose();
                    drResidents = null;
                }

                if (drUsers != null)
                {
                    drUsers.Close();
                    drUsers.Dispose();
                    drUsers = null;
                }

                if (selResidentsCmd != null)
                {
                    selResidentsCmd.Dispose();
                    selResidentsCmd = null;
                }

                if (selUsersCmd != null)
                {
                    selUsersCmd.Dispose();
                    selUsersCmd = null;
                }

                if (oraOPSTransaction != null)
                {
                    oraOPSTransaction.Dispose();
                    oraOPSTransaction = null;
                }
            }

            return bReturn;
        }

        private bool GenerateNotifications()
        {
            bool bReturn = true;
            OracleDataReader dr = null;
            OracleCommand selOperationssCmd = null;
            int nNotId = -1;
            int nMobileUser = -1;
            string strUserName = "";
            int nType = -1;
            string strTypeText = "";
            string strCloudToken = "";
            int nNotifyAmount = -1;
            string strDesc = "";
            int nDeviceOS = -1;
            string strPlate = "";
            string strOperation = "";
            string strText = "";
            int nIndex = 0;

            string sSQLCommand = "";

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();

                oraOPSTransaction = OPSBDCon.BeginTransaction();

                selOperationssCmd = new OracleCommand();
                selOperationssCmd.Connection = OPSBDCon;
                selOperationssCmd.Transaction = oraOPSTransaction;

                selOperationssCmd.CommandText = "select no_id, no_mu_id, no_type, no_cloud_token, mu_balance_notify_amount, mu_login, no_device_os, no_vehicleid, no_operation, no_text from notifications, mobile_users "
                                                + "where no_mu_id = mu_id and no_state = " + appSettings.GetValue("NotificationState.Pending", typeof(string));

                dr = selOperationssCmd.ExecuteReader();

                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        nNotId = dr.GetInt32(dr.GetOrdinal("NO_ID"));
                        nMobileUser = dr.GetInt32(dr.GetOrdinal("NO_MU_ID"));
                        strUserName = dr.GetString(dr.GetOrdinal("MU_LOGIN"));
                        nType = dr.GetInt32(dr.GetOrdinal("NO_TYPE"));
                        strCloudToken = dr.GetString(dr.GetOrdinal("NO_CLOUD_TOKEN"));
                        nNotifyAmount = dr.GetInt32(dr.GetOrdinal("MU_BALANCE_NOTIFY_AMOUNT"));
                        nDeviceOS = dr.GetInt32(dr.GetOrdinal("NO_DEVICE_OS"));
                        if (!dr.IsDBNull(7))
                            strPlate = dr.GetString(dr.GetOrdinal("NO_VEHICLEID"));
                        if (!dr.IsDBNull(8))
                            strOperation = dr.GetInt32(dr.GetOrdinal("NO_OPERATION")).ToString();
                        if (!dr.IsDBNull(9))
                            strText = dr.GetString(dr.GetOrdinal("NO_TEXT")).ToString();

                        Logger_AddLogMessage(string.Format("Found notification: ID: {0}, User: {1}, Login: {2}, Type: {3}, Token: {4}, Balance: {5}, OS: {6}", nNotId, nMobileUser, strUserName, nType, strCloudToken, nNotifyAmount, nDeviceOS),
                                            LoggerSeverities.Info);

                        if (nType == Convert.ToInt32(appSettings.GetValue("NotificationType.Parking", typeof(Int32))))
                        {
                            strDesc = appSettings.GetValue("NotificationText.Parking", typeof(string)).ToString();
                            if (strText.Length > 0)
                                strDesc += " (" + strText + ")";
                            strTypeText = appSettings.GetValue("NotificationTypeText.Parking", typeof(string)).ToString();
                        }
                        else if (nType == Convert.ToInt32(appSettings.GetValue("NotificationType.Recharge", typeof(Int32))))
                        {
                            strDesc = appSettings.GetValue("NotificationText.Recharge", typeof(string)).ToString();
                            strTypeText = appSettings.GetValue("NotificationTypeText.Recharge", typeof(string)).ToString();
                        }
                        else if (nType == Convert.ToInt32(appSettings.GetValue("NotificationType.Fine", typeof(Int32))))
                        {
                            strDesc = appSettings.GetValue("NotificationText.Fine", typeof(string)).ToString();
                            strTypeText = appSettings.GetValue("NotificationTypeText.Fine", typeof(string)).ToString();
                        }
                        else if (nType == Convert.ToInt32(appSettings.GetValue("NotificationType.Balance", typeof(Int32))))
                        {
                            strDesc = appSettings.GetValue("NotificationText.Balance", typeof(string)).ToString();
                            decimal dFormattedAmount = nNotifyAmount * (decimal)0.01;
                            strDesc = string.Format(strDesc, dFormattedAmount.ToString("0.00").Replace(".", ","));
                            strTypeText = appSettings.GetValue("NotificationTypeText.Balance", typeof(string)).ToString();
                        }
                        else if (nType == Convert.ToInt32(appSettings.GetValue("NotificationType.ResWarn", typeof(Int32))))
                        {
                            //strDesc = appSettings.GetValue("NotificationText.ResWarn", typeof(string)).ToString();
                            //strDesc = string.Format(strDesc, strText);
                            strDesc = strText;
                            strTypeText = appSettings.GetValue("NotificationTypeText.ResWarn", typeof(string)).ToString();
                        }

                        if (nDeviceOS == Convert.ToInt32(appSettings.GetValue("DeviceOS.Android", typeof(Int32))))
                        {
                            MessageData message = new MessageData(strCloudToken, strUserName, strTypeText, strDesc, strPlate, strOperation);
                            if (message != null)
                            {
                                if (SendGoogleNotification(message))
                                {
                                    Logger_AddLogMessage(string.Format("GCM notification sent"), LoggerSeverities.Info);
                                    UpdateNotifyFlag(nNotId);
                                }
                                else
                                {
                                    Logger_AddLogMessage(string.Format("Error sending GCM notification"), LoggerSeverities.Error);
                                    UpdateNotifyRetries(nNotId);

                                    int nRetries = GetNotifyRetries(nNotId);
                                    if (nRetries >= Convert.ToInt32(appSettings.GetValue("MaxRetries", typeof(Int32))))
                                    {
                                        Logger_AddLogMessage(string.Format("Exceeded maximum number of retries, flagged as error", nDeviceOS), LoggerSeverities.Info);
                                        UpdateNotifyState(nNotId, Convert.ToInt32(appSettings.GetValue("NotificationState.Error", typeof(Int32))));
                                    }
                                }
                            }
                        }
                        else if (nDeviceOS == Convert.ToInt32(appSettings.GetValue("DeviceOS.iOS", typeof(Int32))))
                        {
                            MessageData message = new MessageData(strCloudToken, strUserName, strTypeText, strDesc, strPlate, strOperation);
                            if (message != null)
                            {
                                AppleNotifier appleNotifier = new AppleNotifier();
                                string strErrorMsg = "";
                                if (appleNotifier.SendNotification(message, out strErrorMsg))
                                {
                                    Logger_AddLogMessage(string.Format("APN notification sent"), LoggerSeverities.Info);
                                    UpdateNotifyFlag(nNotId);
                                    Thread.Sleep(30000);
                                }
                                else
                                {
                                    Logger_AddLogMessage(string.Format("Error sending APN notification - {0}", strErrorMsg), LoggerSeverities.Error);
                                    UpdateNotifyRetries(nNotId);

                                    int nRetries = GetNotifyRetries(nNotId);
                                    if (nRetries >= Convert.ToInt32(appSettings.GetValue("MaxRetries", typeof(Int32))))
                                    {
                                        Logger_AddLogMessage(string.Format("Exceeded maximum number of retries, flagged as error", nDeviceOS), LoggerSeverities.Info);
                                        UpdateNotifyState(nNotId, Convert.ToInt32(appSettings.GetValue("NotificationState.Error", typeof(Int32))));
                                    }
                                }
                            }
                        }
                        else
                        {
                            Logger_AddLogMessage(string.Format("Device type {0} not supported, flagged as error", nDeviceOS), LoggerSeverities.Info);
                            UpdateNotifyState(nNotId, Convert.ToInt32(appSettings.GetValue("NotificationState.Error", typeof(Int32))));
                        }

                        if (++nIndex >= _iMaxRegisters)
                            break;
                    }

                    Commit();
                }
            }
            catch (Exception e)
            {
                Logger_AddLogMessage(sSQLCommand, LoggerSeverities.Error);
                Logger_AddLogException(e);
                Rollback();
            }

            finally
            {
                if (dr != null)
                {
                    dr.Close();
                    dr.Dispose();
                    dr = null;
                }

                if (selOperationssCmd != null)
                {
                    selOperationssCmd.Dispose();
                    selOperationssCmd = null;
                }

                if (oraOPSTransaction != null)
                {
                    oraOPSTransaction.Dispose();
                    oraOPSTransaction = null;
                }
            }

            return bReturn;
        }

        private bool InsertNotification(int nMobileUser, int nType, string strCloudToken, int nOS, string strPlate)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("insert into notifications (no_mu_id, no_type, no_cloud_token, no_device_os, no_vehicleid) values ({0}, {1}, '{2}', {3}, '{4}')",
                    nMobileUser, nType, strCloudToken, nOS, strPlate);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private bool InsertNotification(int nMobileUser, int nType, string strCloudToken, int nOS, string strPlate, string strOperation)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("insert into notifications (no_mu_id, no_type, no_cloud_token, no_device_os, no_vehicleid, no_operation) values ({0}, {1}, '{2}', {3}, '{4}', {5})",
                    nMobileUser, nType, strCloudToken, nOS, strPlate, strOperation);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private bool InsertNotification(int nMobileUser, int nType, string strCloudToken, int nOS, string strPlate, string strOperation, string strText)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("insert into notifications (no_mu_id, no_type, no_cloud_token, no_device_os, no_vehicleid, no_operation, no_text) values ({0}, {1}, '{2}', {3}, '{4}', {5}, '{6}')",
                    nMobileUser, nType, strCloudToken, nOS, strPlate, strOperation, strText);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private bool InsertTextNotification(int nMobileUser, int nType, string strCloudToken, int nOS, string strText)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("insert into notifications (no_mu_id, no_type, no_cloud_token, no_device_os, no_text) values ({0}, {1}, '{2}', {3}, '{4}')",
                    nMobileUser, nType, strCloudToken, nOS, strText);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private bool UpdateOperNotifyFlag(int nOperId)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("update operations set ope_notified = 1 where ope_id = {0}",
                    nOperId);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private bool UpdateFineNotifyFlag(int nFineId)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("update fines set fin_notified = 1 where fin_id = {0}",
                    nFineId);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private bool UpdateUserNotifyFlag(int nMobileUserId)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("update mobile_users set mu_notified = 1 where mu_id = {0}",
                    nMobileUserId);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private bool UpdateResNotifyFlag(int nResidentId, int nState)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();

                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("update residents_data set resd_notified = {0} where resd_id = {1}",
                    nState, nResidentId);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private bool UpdateNotifyFlag(int nNotifyId)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();

                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("update notifications set no_state = {0}, no_actdate = sysdate where no_id = {1}",
                    appSettings.GetValue("NotificationState.Sent", typeof(string)).ToString(), nNotifyId);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private bool UpdateNotifyState(int nNotifyId, int nState)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();

                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("update notifications set no_state = {0}, no_actdate = sysdate where no_id = {1}",
                    nState, nNotifyId);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private bool UpdateNotifyRetries(int nNotifyId)
        {
            bool bResult = false;
            OracleCommand updateCmd = null;

            try
            {
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();

                updateCmd = new OracleCommand();
                updateCmd.Connection = OPSBDCon;
                updateCmd.Transaction = oraOPSTransaction;
                updateCmd.CommandText = string.Format("update notifications set no_retries = no_retries + 1, no_actdate = sysdate where no_id = {0}",
                    nNotifyId);
                if (updateCmd.ExecuteNonQuery() > 0)
                    bResult = true;
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
            }
            finally
            {
                if (updateCmd != null)
                {
                    updateCmd.Dispose();
                    updateCmd = null;
                }
            }

            return bResult;
        }

        private int GetNotifyRetries(int nNotifyId)
        {
            int nRetries = 0;
            OracleDataReader dr = null;
            OracleCommand cmd = null;

            string sSQLCommand = "";

            try
            {
                cmd = new OracleCommand();
                cmd.Connection = OPSBDCon;
                cmd.Transaction = oraOPSTransaction;

                cmd.CommandText = string.Format("select no_retries from notifications where no_id = {0}", nNotifyId);

                dr = cmd.ExecuteReader();

                if (dr.HasRows)
                {
                    if (dr.Read())
                        nRetries = dr.GetInt32(dr.GetOrdinal("NO_RETRIES"));
                }
            }
            catch (Exception e)
            {
                Logger_AddLogMessage(sSQLCommand, LoggerSeverities.Error);
                Logger_AddLogException(e);
            }

            finally
            {
                if (dr != null)
                {
                    dr.Close();
                    dr.Dispose();
                    dr = null;
                }

                if (cmd != null)
                {
                    cmd.Dispose();
                    cmd = null;
                }
            }

            return nRetries;
        }

        private bool OpenDB()
        {
            bool bReturn = true;
            try
            {
                OPSBDCon = new OracleConnection(OPS_DB_CONNECTION_STRING);
                OPSBDCon.Open();

                if (OPSBDCon.State != System.Data.ConnectionState.Open)
                {
                    bReturn = false;
                    OPSBDCon.Close();
                    OPSBDCon.Dispose();
                    Logger_AddLogMessage("Error openning OPS Database", LoggerSeverities.Error);
                }
                else
                    Logger_AddLogMessage("OPS Database opened", LoggerSeverities.Info);
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
                bReturn = false;
            }

            return bReturn;
        }

        private bool CloseDB()
        {
            bool bReturn = true;
            try
            {
                if (oraOPSTransaction != null)
                {
                    oraOPSTransaction.Commit();
                    oraOPSTransaction.Dispose();
                    oraOPSTransaction = null;
                }

                if (OPSBDCon != null)
                {
                    OPSBDCon.Close();
                    OPSBDCon.Dispose();
                    OPSBDCon = null;
                }
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
                bReturn = false;
            }

            return bReturn;
        }

        private bool Commit()
        {
            bool bReturn = true;

            try
            {
                if (oraOPSTransaction != null)
                    oraOPSTransaction.Commit();
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
                bReturn = false;
            }

            return bReturn;
        }

        private bool Rollback()
        {
            bool bReturn = true;

            try
            {
                if (oraOPSTransaction != null)
                    oraOPSTransaction.Rollback();
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
                bReturn = false;
            }

            return bReturn;
        }

        private void Logger_AddLogMessage(string msg, LoggerSeverities severity)
        {
            _logger.AddLog(msg, severity);
        }

        private void Logger_AddLogException(Exception ex)
        {
            _logger.AddLog(ex);
        }

        public bool SendGoogleNotification(MessageData message)
        {
            bool bResult = false;

            try
            {
                var httpWebRequest = (HttpWebRequest)WebRequest.Create("https://android.googleapis.com/gcm/send");
                httpWebRequest.ContentType = "application/json";
                httpWebRequest.Method = "POST";
                System.Configuration.AppSettingsReader appSettings = new System.Configuration.AppSettingsReader();
                httpWebRequest.Headers.Add(string.Format("Authorization: key={0}", appSettings.GetValue("APIKEY", typeof(string))));
                using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                {
                    streamWriter.Write(message.GetMessage());
                    streamWriter.Flush();
                    streamWriter.Close();

                    var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                    using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                    {
                        var result = streamReader.ReadToEnd();
                        if (result.ToString().Contains("\"success\":1"))
                            bResult = true;
                        else
                        {
                            string strResult = result.ToString().Replace("\"", "");
                            string[] strResultArray = strResult.Split(new char[] {','});
                            foreach (string strResWord in strResultArray)
                            {
                                if (strResWord.Contains("error"))
                                {
                                    string[] strErrorArray = strResWord.Split(new char[] { ':' });
                                    string strError = strErrorArray[strErrorArray.Length - 1].TrimEnd(new char[] {'}',']'});
                                    Logger_AddLogMessage(string.Format("Error sending notification - {0}", strError), LoggerSeverities.Error);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Logger_AddLogException(e);
                bResult = false;
            }

            return bResult;
        }

        public void SendAppleNotification(string deviceToken, string notification)
        {
            int port = 2195;
            //string hostname = "gateway.sandbox.push.apple.com";
            string hostname = "gateway.push.apple.com";
            string certificatePath = @"ApParca_APNS_Prod_Certificados.p12";
            X509Certificate2 clientCertificate = new X509Certificate2(File.ReadAllBytes(certificatePath), "dLhMg3qm");
            X509Certificate2Collection certificatesCollection = new X509Certificate2Collection(clientCertificate);
            TcpClient tcpClient = new TcpClient(hostname, port);
            SslStream sslStream = new SslStream(tcpClient.GetStream(), false, new RemoteCertificateValidationCallback(ValidateServerCertificate), null);
            try
            {
                sslStream.AuthenticateAsClient(hostname, certificatesCollection, SslProtocols.Tls, false);
                MemoryStream memoryStream = new MemoryStream();
                BinaryWriter writer = new BinaryWriter(memoryStream);
                writer.Write((byte)0);
                writer.Write((byte)0);
                writer.Write((byte)32);
                writer.Write(HexStringToByteArray(deviceToken.ToUpper()));
                string payload = "{\"aps\":{\"alert\":\"" + notification + "\",\"badge\":1,\"sound\":\"default\"}}";
                writer.Write((byte)0);
                writer.Write((byte)payload.Length);
                byte[] payloadBytes = System.Text.Encoding.UTF8.GetBytes(payload);
                writer.Write(payloadBytes);
                writer.Flush();
                byte[] memoryStreamAsBytes = memoryStream.ToArray();
                sslStream.Write(memoryStreamAsBytes);
                sslStream.Flush();
                tcpClient.Close();
            }

            catch (Exception e)
            {
                Logger_AddLogException(e);
                tcpClient.Close();
            }
        }

        private byte[] HexStringToByteArray(string hexString)
        {
            return Enumerable.Range(0, hexString.Length)
                     .Where(x => x % 2 == 0)
                     .Select(x => Convert.ToByte(hexString.Substring(x, 2), 16))
                     .ToArray();
        }

        private bool ValidateServerCertificate(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None) return true;
            return false;
        }
    }
}
