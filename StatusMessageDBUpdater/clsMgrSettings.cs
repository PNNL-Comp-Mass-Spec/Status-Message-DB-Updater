//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/16/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using PRISM;

namespace StatusMessageDBUpdater
{
    /// <summary>
    /// Class for loading, storing and accessing manager parameters.
    /// </summary>
    /// <remarks>
    /// Loads initial settings from local config file, then checks to see if remainder of settings should be
    /// loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
    /// parameters database.
    /// </remarks>
    public class MgrSettings : EventNotifier
    {
        #region "Constants"

        /// <summary>
        /// Status message for when the manager is deactivated locally
        /// </summary>
        /// <remarks>Used when MgrActive_Local is False in AppName.exe.config</remarks>
        public const string DEACTIVATED_LOCALLY = "Manager deactivated locally";

        /// <summary>
        /// Manager parameter: config database connection string
        /// </summary>
        public const string MGR_PARAM_MGR_CFG_DB_CONN_STRING = "MgrCnfgDbConnectStr";
        /// <summary>
        /// Manager parameter: manager active
        /// </summary>
        /// <remarks>Defined in AppName.exe.config</remarks>
        public const string MGR_PARAM_MGR_ACTIVE_LOCAL = "MgrActive_Local";

        /// <summary>
        /// Manager parameter: manager name
        /// </summary>
        public const string MGR_PARAM_MGR_NAME = "MgrName";

        /// <summary>
        /// Manager parameter: using defaults flag
        /// </summary>
        public const string MGR_PARAM_USING_DEFAULTS = "UsingDefaults";

        #endregion

        #region "Properties"

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrMsg { get; protected set; } = "";

        /// <summary>
        /// Manager name
        /// </summary>
        public string ManagerName => GetParam(MGR_PARAM_MGR_NAME, Environment.MachineName + "_Undefined-Manager");

        /// <summary>
        /// This will be true after the parameter have been successfully loaded from the database
        /// </summary>
        public bool ParamsLoadedFromDB { get; private set; }

        /// <summary>
        /// Dictionary of manager parameters
        /// </summary>
        public Dictionary<string, string> MgrParams { get; }

        #endregion

        #region "Events"

        /// <summary>
        /// Important error event (only raised if ParamsLoadedFromDB is false)
        /// </summary>
        public ErrorEventEventHandler CriticalErrorEvent;

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Call LoadSettings after instantiating this class</remarks>
        public MgrSettings()
        {
            ParamsLoadedFromDB = false;
            MgrParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Specifies the full name and path for the application config file
        /// </summary>
        /// <returns>String containing full name and path</returns>
        private string GetConfigFileName()
        {
            return Path.GetFileName(GetConfigFilePath());
        }

        /// <summary>
        /// Specifies the full name and path for the application config file
        /// </summary>
        /// <returns>String containing full name and path</returns>
        protected string GetConfigFilePath()
        {
            var configFilePath = PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppPath() + ".config";
            return configFilePath;
        }

        /// <summary>
        /// Initialize manager settings using the local settings, then load additional settings from the database
        /// </summary>
        /// <param name="localSettings">Manager settings from the AppName.exe.config file or from Properties.Settings.Default</param>
        /// <returns></returns>
        public bool LoadSettings(Dictionary<string, string> localSettings)
        {
            ErrMsg = string.Empty;

            MgrParams.Clear();

            // Copy the settings from localSettings to configFileSettings
            // Assures that the MgrName setting is defined and auto-updates it if it contains $ComputerName$
            var mgrSettingsFromFile = InitializeMgrSettings(localSettings);

            foreach (var item in mgrSettingsFromFile)
            {
                MgrParams.Add(item.Key, item.Value);
            }

            // Auto-add setting ApplicationPath, which is the directory with this applications .exe
            var appPath = PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppPath();
            var appFile = new FileInfo(appPath);
            SetParam("ApplicationPath", appFile.DirectoryName);

            // Test the settings retrieved from the config file
            if (!CheckInitialSettings(MgrParams))
            {
                // Error logging handled by CheckInitialSettings
                return false;
            }

            // Assure that MgrActive_Local is defined
            if (!MgrParams.TryGetValue(MGR_PARAM_MGR_ACTIVE_LOCAL, out _))
            {
                // MgrActive_Local parameter not defined defined in the AppName.exe.config file
                HandleParameterNotDefined(MGR_PARAM_MGR_ACTIVE_LOCAL);
            }

            // Get remaining settings from database
            if (!LoadMgrSettingsFromDB())
            {
                // Error logging handled by LoadMgrSettingsFromDB
                return false;
            }

            // Set flag indicating params have been loaded from the manager control database
            ParamsLoadedFromDB = true;

            // No problems found
            return true;
        }

        private Dictionary<string, string> InitializeMgrSettings(Dictionary<string, string> localSettings)
        {
            var mgrSettingsFromFile = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var item in localSettings)
            {
                mgrSettingsFromFile.Add(item.Key, item.Value);
            }

            // If MgrName value contains the text $ComputerName$, replace it with computer's name
            // This is a case-sensitive comparison
            if (mgrSettingsFromFile.TryGetValue(MGR_PARAM_MGR_NAME, out var mgrName))
            {
                if (mgrName.Contains("$ComputerName$"))
                {
                    mgrSettingsFromFile[MGR_PARAM_MGR_NAME] = mgrName.Replace("$ComputerName$", Environment.MachineName);
                }
            }
            else
            {
                mgrSettingsFromFile.Add(MGR_PARAM_MGR_NAME, Environment.MachineName + "_Undefined-Manager");
            }

            return mgrSettingsFromFile;
        }

        /// <summary>
        /// Tests initial settings retrieved from config file
        /// </summary>
        /// <param name="paramDictionary"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool CheckInitialSettings(IReadOnlyDictionary<string, string> paramDictionary)
        {

            // Verify that UsingDefaults is false
            if (!paramDictionary.TryGetValue(MGR_PARAM_USING_DEFAULTS, out var usingDefaultsText))
            {
                HandleParameterNotDefined(MGR_PARAM_USING_DEFAULTS);
            }
            else
            {
                if (bool.TryParse(usingDefaultsText, out var usingDefaults) && usingDefaults)
                {
                    ErrMsg = string.Format("MgrSettings.CheckInitialSettings; Config file problem, {0} contains UsingDefaults=True",
                                           GetConfigFileName());
                    ReportError(ErrMsg);
                    return false;
                }
            }

            // Determine if manager is deactivated locally
            if (!paramDictionary.TryGetValue(MGR_PARAM_MGR_ACTIVE_LOCAL, out var activeLocalText))
            {
                // Parameter is not defined; log an error but return true
                HandleParameterNotDefined(MGR_PARAM_MGR_ACTIVE_LOCAL);
                return true;
            }

            if (!bool.TryParse(activeLocalText, out var activeLocal) || !activeLocal)
            {
                ErrMsg = DEACTIVATED_LOCALLY;
                ReportError(DEACTIVATED_LOCALLY, false);
                return false;
            }

            // No problems found
            return true;
        }

        private string GetGroupNameFromSettings(IReadOnlyDictionary<string, string> mgrSettings)
        {
            if (!mgrSettings.TryGetValue("MgrSettingGroupName", out var groupName))
                return string.Empty;

            return string.IsNullOrWhiteSpace(groupName) ? string.Empty : groupName;
        }

        private void HandleParameterNotDefined(string parameterName)
        {
            ErrMsg = string.Format("Parameter '{0}' is not defined defined in file {1}",
                                   parameterName,
                                   GetConfigFileName());
            ReportError(ErrMsg);
        }

        /// <summary>
        /// Gets manager config settings from manager control DB (Manager_Control)
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks>Performs retries if necessary.</remarks>
        protected bool LoadMgrSettingsFromDB(bool logConnectionErrors = true)
        {

            var managerName = GetParam(MGR_PARAM_MGR_NAME, string.Empty);

            if (string.IsNullOrEmpty(managerName))
            {
                // MgrName parameter not defined defined in the AppName.exe.config file
                HandleParameterNotDefined(MGR_PARAM_MGR_NAME);
                return false;
            }

            var success = LoadMgrSettingsFromDBWork(managerName, out var mgrSettingsFromDB, logConnectionErrors, returnErrorIfNoParameters: true);
            if (!success)
            {
                return false;
            }

            success = StoreParameters(mgrSettingsFromDB, managerName, skipExistingParameters: false);

            var mgrSettingsGroup = GetGroupNameFromSettings(mgrSettingsFromDB);

            while (success && !string.IsNullOrEmpty(mgrSettingsGroup))
            {
                // This manager has group-based settings defined; load them now
                success = LoadMgrSettingsFromDBWork(mgrSettingsGroup, out var mgrGroupSettingsFromDB, logConnectionErrors, returnErrorIfNoParameters: false);

                if (success)
                {
                    success = StoreParameters(mgrGroupSettingsFromDB, mgrSettingsGroup, skipExistingParameters: true);
                    mgrSettingsGroup = GetGroupNameFromSettings(mgrGroupSettingsFromDB);
                }
            }

            return success;
        }

        /// <summary>
        /// Load manager settings from the database
        /// </summary>
        /// <param name="managerName">Manager name or manager group name</param>
        /// <param name="mgrSettingsFromDB">Output: manager settings</param>
        /// <param name="logConnectionErrors">When true, log connection errors</param>
        /// <param name="returnErrorIfNoParameters">When true, return an error if no parameters defined</param>
        /// <returns></returns>
        private bool LoadMgrSettingsFromDBWork(
            string managerName,
            out Dictionary<string, string> mgrSettingsFromDB,
            bool logConnectionErrors,
            bool returnErrorIfNoParameters)
        {

            mgrSettingsFromDB = new Dictionary<string, string>();

            var dbConnectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING, "");

            if (string.IsNullOrEmpty(dbConnectionString))
            {
                // MgrCnfgDbConnectStr parameter not defined defined in the AppName.exe.config file
                HandleParameterNotDefined(MGR_PARAM_MGR_CFG_DB_CONN_STRING);
                return false;
            }

            var sqlQuery = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" + managerName + "'";

            // Query the database (retrying up to 3 times)
            var dbTools = new DBTools(dbConnectionString);

            if (logConnectionErrors)
            {
                RegisterEvents(dbTools);
            }

            short retryCount = 3;
            var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, "LoadMgrSettingsFromDBWork", retryCount);

            if (!success)
            {
                // Log the message to the DB if the monthly Windows updates are not pending
                var criticalError = !WindowsUpdateStatus.ServerUpdatesArePending();

                ErrMsg = "MgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database for manager " + managerName;
                if (logConnectionErrors)
                    ReportError(ErrMsg, criticalError);

                return false;
            }

            // Verify at least one row returned
            if (queryResults.Count < 1 && returnErrorIfNoParameters)
            {
                // Wrong number of rows returned
                ErrMsg = string.Format("MgrSettings.LoadMgrSettingsFromDB; Manager '{0}' is not defined in the manager control database; using {1}",
                                       managerName, dbConnectionString);
                ReportError(ErrMsg);
                return false;
            }

            foreach (var item in queryResults)
            {
                mgrSettingsFromDB.Add(item[0], item[1]);
            }

            return true;
        }

        /// <summary>
        /// Store manager settings, optionally skipping existing parameters
        /// </summary>
        /// <param name="mgrSettings">Manager settings</param>
        /// <param name="managerOrGroupName">Manager name or manager group name</param>
        /// <param name="skipExistingParameters">When true, skip existing parameters</param>
        /// <returns></returns>
        private bool StoreParameters(IReadOnlyDictionary<string, string> mgrSettings, string managerOrGroupName, bool skipExistingParameters)
        {
            bool success;

            try
            {
                foreach (var item in mgrSettings)
                {
                    if (MgrParams.ContainsKey(item.Key))
                    {
                        if (!skipExistingParameters)
                        {
                            MgrParams[item.Key] = item.Value;
                        }
                    }
                    else
                    {
                        MgrParams.Add(item.Key, item.Value);
                    }
                }
                success = true;
            }
            catch (Exception ex)
            {
                ErrMsg = string.Format("MgrSettings.StoreParameters; Exception storing settings for manager '{0}': {1}",
                                       managerOrGroupName, ex.Message);
                ReportError(ErrMsg);
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Gets a manager parameter
        /// </summary>
        /// <param name="itemKey"></param>
        /// <returns>Parameter value if found, otherwise an empty string</returns>
        public string GetParam(string itemKey)
        {
            return GetParam(itemKey, string.Empty);
        }

        /// <summary>
        /// Gets a manager parameter
        /// </summary>
        /// <param name="itemKey">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise valueIfMissing</returns>
        public string GetParam(string itemKey, string valueIfMissing)
        {
            if (MgrParams.TryGetValue(itemKey, out var itemValue))
            {
                return itemValue ?? string.Empty;
            }

            return valueIfMissing ?? string.Empty;
        }

        /// <summary>
        /// Gets a manager parameter
        /// </summary>
        /// <param name="itemKey">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise valueIfMissing</returns>
        public bool GetParam(string itemKey, bool valueIfMissing)
        {
            if (MgrParams.TryGetValue(itemKey, out var valueText))
            {
                if (string.IsNullOrEmpty(valueText))
                    return valueIfMissing;

                if (bool.TryParse(valueText, out var value))
                    return value;

                if (int.TryParse(valueText, out var integerValue))
                {
                    if (integerValue == 0)
                        return false;
                    if (integerValue == 1)
                        return true;
                }
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Gets a manager parameter
        /// </summary>
        /// <param name="itemKey">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise valueIfMissing</returns>
        public int GetParam(string itemKey, int valueIfMissing)
        {
            if (MgrParams.TryGetValue(itemKey, out var valueText))
            {
                if (string.IsNullOrEmpty(valueText))
                    return valueIfMissing;

                if (int.TryParse(valueText, out var value))
                    return value;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Adds or updates a manager parameter
        /// </summary>
        /// <param name="itemKey"></param>
        /// <param name="itemValue"></param>
        // ReSharper disable once UnusedMember.Global
        public void SetParam(string itemKey, string itemValue)
        {
            if (MgrParams.ContainsKey(itemKey))
            {
                MgrParams[itemKey] = itemValue;
            }
            else
            {
                MgrParams.Add(itemKey, itemValue);
            }
        }

        #endregion

        #region "Event Handlers"

        /// <summary>
        /// Report an important error
        /// </summary>
        /// <param name="message"></param>
        private void OnCriticalErrorEvent(string message)
        {
            if (CriticalErrorEvent == null && WriteToConsoleIfNoListener)
                ConsoleMsgUtils.ShowError(message, false, false, EmptyLinesBeforeErrorMessages);

            CriticalErrorEvent?.Invoke(message, null);
        }

        /// <summary>
        /// Raises a CriticalErrorEvent if criticalError is true and ParamsLoadedFromDB is false
        /// Otherwise, raises a normal error event
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="criticalError"></param>
        protected void ReportError(string errorMessage, bool criticalError = true)
        {
            if (!ParamsLoadedFromDB && criticalError)
            {
                OnCriticalErrorEvent(errorMessage);
            }
            else
            {
                OnErrorEvent(errorMessage);
            }
        }

        /// <summary>
        /// Raises an error event that includes an exception
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="ex"></param>
        protected void ReportError(string errorMessage, Exception ex)
        {
            OnErrorEvent(errorMessage, ex);
        }

        #endregion
    }
}
