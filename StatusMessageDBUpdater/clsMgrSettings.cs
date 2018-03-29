
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/16/2009
//
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Windows.Forms;
using System.Xml;
using PRISM;

namespace StatusMessageDBUpdater
{
    /// <summary>
    /// Class for loading, storing and accessing manager parameters.
    ///	Loads initial settings from local config file, then checks to see if remainder of settings should be
    ///	loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
    ///	parameters database.
    /// </summary>
    public class clsMgrSettings : clsEventNotifier, IMgrParams
    {

        #region "Class variables"

        public const string DEACTIVATED_LOCALLY = "Manager deactivated locally";

        #endregion

        #region "Properties"

        public string ErrMsg { get; private set; } = "";

        public Dictionary<string, string> TaskDictionary { get; private set; }

        #endregion

        #region "Methods"

        public bool LoadSettings()
        {
            ErrMsg = "";

            // If the param dictionary exists, it needs to be cleared out
            if (TaskDictionary != null)
            {
                TaskDictionary.Clear();
                TaskDictionary = null;
            }

            // Get settings from config file
            TaskDictionary = LoadMgrSettingsFromFile();

            // Get directory for main executable
            var appPath = Application.ExecutablePath;
            var fi = new FileInfo(appPath);
            TaskDictionary.Add("ApplicationPath", fi.DirectoryName);

            // Test the settings retrieved from the config file
            if (!CheckInitialSettings(TaskDictionary))
            {
                // Error logging handled by CheckInitialSettings
                return false;
            }

            // Determine if manager is deactivated locally
            if (!bool.Parse(GetParam("MgrActive_Local", "false")))
            {
                Console.WriteLine(DEACTIVATED_LOCALLY);
                ErrMsg = DEACTIVATED_LOCALLY;
                return false;
            }

            // Get remaining settings from database
            if (!LoadMgrSettingsFromDB())
            {
                // Error logging handled by LoadMgrSettingsFromDB
                return false;
            }

            // No problems found
            return true;
        }

        private Dictionary<string, string> LoadMgrSettingsFromFile()
        {
            // Load initial settings into dictionary for return
            var paramDict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            // Manager config db connection string
            var dbConnString = Properties.Settings.Default.MgrCnfgDbConnectStr;
            paramDict.Add("MgrCnfgDbConnectStr", dbConnString);

            // Manager active flag
            var mgrActiveLocal = Properties.Settings.Default.MgrActive_Local;
            paramDict.Add("MgrActive_Local", mgrActiveLocal);

            // Manager name
            // If the MgrName setting in the .exe.config file contains the text $ComputerName$
            // then that text is replaced with this computer's domain name
            // This is a case-sensitive comparison
            //
            var mgrName = Properties.Settings.Default.MgrName;
            paramDict.Add("MgrName", mgrName.Replace("$ComputerName$", Environment.MachineName));

            // Default settings in use flag
            var usingDefaults = Properties.Settings.Default.UsingDefaults;
            paramDict.Add("UsingDefaults", usingDefaults);

            // Update check interval
            var updateCheckInterval = Properties.Settings.Default.CheckForUpdateInterval;
            paramDict.Add("CheckForUpdateInterval", updateCheckInterval);

            return paramDict;
        }

        private bool CheckInitialSettings(IReadOnlyDictionary<string, string> paramDict)
        {
            // Verify manager settings dictionary exists
            if (paramDict == null)
            {
                ErrMsg = "clsMgrSettings.CheckInitialSettings(); Manager parameter dictionary not found";
                ConsoleMsgUtils.ShowError(ErrMsg);
                return false;
            }

            // Verify intact config file was found
            if (!paramDict.TryGetValue("UsingDefaults", out var usingDefaults))
            {
                ErrMsg = "clsMgrSettings.CheckInitialSettings(); 'UsingDefaults' entry not found in Config file";
                ConsoleMsgUtils.ShowWarning(ErrMsg);
            }
            else
            {
                if (bool.TryParse(usingDefaults, out var value))
                {
                    if (value)
                    {
                        ErrMsg = "clsMgrSettings.CheckInitialSettings(); Config file problem, contains UsingDefaults=True";
                        ConsoleMsgUtils.ShowWarning(ErrMsg);
                        return false;
                    }
                }
            }

            // No problems found
            return true;
        }

        private string GetGroupNameFromSettings(DataTable dtSettings)
        {

            foreach (DataRow dataRow in dtSettings.Rows)
            {
                // Add the column heading and value to the dictionary
                var paramKey = DbCStr(dataRow[dtSettings.Columns["ParameterName"]]);

                if (string.Equals(paramKey, "MgrSettingGroupName", StringComparison.CurrentCultureIgnoreCase))
                {
                    var groupName = DbCStr(dataRow[dtSettings.Columns["ParameterValue"]]);
                    if (!string.IsNullOrWhiteSpace(groupName))
                    {
                        return groupName;
                    }

                    return string.Empty;
                }

            }

            return string.Empty;

        }

        public bool LoadMgrSettingsFromDB()
        {
            const bool logConnectionErrors = true;
            return LoadMgrSettingsFromDB( logConnectionErrors);
        }

        public bool LoadMgrSettingsFromDB(bool logConnectionErrors)
        {
            // Requests manager parameters from database. Input string specifies view to use. Performs retries if necessary.

            var managerName = GetParam("MgrName", "");

            if (string.IsNullOrEmpty(managerName))
            {
                ErrMsg = "MgrName parameter not found in mParamDictionary; it should be defined in the CaptureTaskManager.exe.config file";
                WriteErrorMsg(ErrMsg);
                return false;
            }

            var success = LoadMgrSettingsFromDBWork(managerName, out var dtSettings, logConnectionErrors, returnErrorIfNoParameters: true);
            if (!success)
            {
                return false;
            }

            success = StoreParameters(dtSettings, skipExistingParameters: false, managerName: managerName);

            if (!success)
                return false;

            while (success)
            {
                var mgrSettingsGroup = GetGroupNameFromSettings(dtSettings);
                if (string.IsNullOrEmpty(mgrSettingsGroup))
                {
                    break;
                }

                // This manager has group-based settings defined; load them now

                success = LoadMgrSettingsFromDBWork(mgrSettingsGroup, out dtSettings, logConnectionErrors, returnErrorIfNoParameters: false);

                if (success)
                {
                    success = StoreParameters(dtSettings, skipExistingParameters: true, managerName: mgrSettingsGroup);
                }

            }

            return success;

        }

        private bool LoadMgrSettingsFromDBWork(string managerName, out DataTable dtSettings, bool logConnectionErrors, bool returnErrorIfNoParameters)
        {

            short retryCount = 3;
            var dbConnectionString = GetParam("MgrCnfgDbConnectStr", "");
            dtSettings = null;

            if (string.IsNullOrEmpty(dbConnectionString))
            {
                ErrMsg = "MgrCnfgDbConnectStr parameter not found in mParamDictionary; it should be defined in the CaptureTaskManager.exe.config file";
                WriteErrorMsg(ErrMsg);
                return false;
            }

            var sqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" + managerName + "'";

            // Get a datatable holding the parameters for this manager
            while (retryCount > 0)
            {
                try
                {
                    using (var cn = new SqlConnection(dbConnectionString))
                    {
                        var cmd = new SqlCommand
                        {
                            CommandType = CommandType.Text,
                            CommandText = sqlStr,
                            Connection = cn,
                            CommandTimeout = 30
                        };

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            using (var ds = new DataSet())
                            {
                                da.Fill(ds);
                                dtSettings = ds.Tables[0];
                            }
                        }
                    }
                    break;
                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var myMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception getting manager settings from database: " + ex.Message;
                    myMsg = myMsg + ", RetryCount = " + retryCount;
                    if (logConnectionErrors)
                        WriteErrorMsg(myMsg);

                    // Delay for 5 seconds before trying again
                    System.Threading.Thread.Sleep(5000);
                }
            }

            // If loop exited due to errors, return false
            if (retryCount < 1)
            {

                ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database";
                if (logConnectionErrors)
                    WriteErrorMsg(ErrMsg);

                return false;
            }

            // Validate that the data table object is initialized
            if (dtSettings == null)
            {
                // Data table not initialized
                ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; dtSettings datatable is null; using " + dbConnectionString;
                if (logConnectionErrors)
                    WriteErrorMsg(ErrMsg);
                return false;
            }

            // Verify at least one row returned
            if (dtSettings.Rows.Count < 1 && returnErrorIfNoParameters)
            {
                // Wrong number of rows returned
                ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Manager " + managerName + " not defined in the manager control database; using " + dbConnectionString;
                WriteErrorMsg(ErrMsg);
                dtSettings.Dispose();
                return false;
            }

            return true;

        }

        public bool StoreParameters(DataTable dtSettings, bool skipExistingParameters, string managerName)
        {
            bool success;

            // Fill a dictionary with the manager parameters that have been found
            try
            {
                foreach (DataRow oRow in dtSettings.Rows)
                {
                    // Add the column heading and value to the dictionary
                    var paramKey = DbCStr(oRow[dtSettings.Columns["ParameterName"]]);
                    var paramVal = DbCStr(oRow[dtSettings.Columns["ParameterValue"]]);

                    if (TaskDictionary.ContainsKey(paramKey))
                    {
                        if (!skipExistingParameters)
                        {
                            TaskDictionary[paramKey] = paramVal;
                        }
                    }
                    else
                    {
                        TaskDictionary.Add(paramKey, paramVal);
                    }
                }
                success = true;
            }
            catch (Exception ex)
            {
                ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling dictionary from table for manager '" + managerName + "': " + ex.Message;
                WriteErrorMsg(ErrMsg);
                success = false;
            }
            finally
            {
                dtSettings?.Dispose();
            }

            return success;
        }

        /// <summary>
        /// Lookup the value of a boolean parameter
        /// </summary>
        /// <param name="itemKey"></param>
        /// <returns>True/false for the given parameter; false if the parameter is not present</returns>
        public bool GetBooleanParam(string itemKey)
        {
            var itemValue = GetParam(itemKey, string.Empty);

            if (string.IsNullOrWhiteSpace(itemValue))
                return false;

            if (bool.TryParse(itemValue, out var itemBool))
                return itemBool;

            return false;

        }

        public string GetParam(string itemKey)
        {
            return GetParam(itemKey, string.Empty);
        }

        /// <summary>
        /// Gets a stored parameter
        /// </summary>
        /// <param name="itemKey">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public string GetParam(string itemKey, string valueIfMissing)
        {
            if (TaskDictionary.TryGetValue(itemKey, out var itemValue))
            {
                return itemValue ?? string.Empty;
            }

            return valueIfMissing ?? string.Empty;
        }

        public void SetParam(string itemKey, string itemValue)
        {
            if (TaskDictionary.ContainsKey(itemKey))
            {
                TaskDictionary[itemKey] = itemValue;
            }
            else
            {
                TaskDictionary.Add(itemKey, itemValue);
            }
        }

        /// <summary>
        /// Writes specfied value to an application config file.
        /// </summary>
        /// <param name="key">Name for parameter (case sensitive)</param>
        /// <param name="value">New value for parameter</param>
        /// <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
        /// <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
        public bool WriteConfigSetting(string key, string value)
        {

            ErrMsg = "";

            // Load the config document
            var doc = LoadConfigDocument();
            if (doc == null)
            {
                // Error message has already been produced by LoadConfigDocument
                return false;
            }

            // Retrieve the settings node
            var appSettingsNode = doc.SelectSingleNode("// applicationSettings");

            if (appSettingsNode == null)
            {
                ErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found";
                return false;
            }

            try
            {
                // Select the element containing the value for the specified key containing the key
                var matchingElement = (XmlElement)appSettingsNode.SelectSingleNode(string.Format("// setting[@name='{0}']/value", key));
                if (matchingElement != null)
                {
                    // Set key to specified value
                    matchingElement.InnerText = value;
                }
                else
                {
                    // Key was not found
                    ErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " + key;
                    return false;
                }
                doc.Save(GetConfigFilePath());
                return true;
            }
            catch (Exception ex)
            {
                ErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Loads an app config file for changing parameters
        /// </summary>
        /// <returns>App config file as an XML document if successful; NOTHING on failure</returns>
        private XmlDocument LoadConfigDocument()
        {
            try
            {
                var doc = new XmlDocument();
                doc.Load(GetConfigFilePath());
                return doc;
            }
            catch (Exception ex)
            {
                ErrMsg = "clsMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
                return null;
            }
        }

        /// <summary>
        /// Specifies the full name and path for the application config file
        /// </summary>
        /// <returns>String containing full name and path</returns>
        private string GetConfigFilePath()
        {
            return Application.ExecutablePath + ".config";
        }

        private string DbCStr(object dataValue)
        {
            if (dataValue == null)
            {
                return "";
            }

            return dataValue.ToString();
        }

        private void WriteErrorMsg(string errorMessage)
        {
            OnErrorEvent(errorMessage);
        }

        #endregion
    }
}
