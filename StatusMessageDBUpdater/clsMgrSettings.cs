
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/16/2009
//
// Last modified 06/16/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Collections.Specialized;
using System.Xml;
using System.Configuration;

namespace StatusMessageDBUpdater
{
	public class clsMgrSettings : IMgrParams
	{
		//*********************************************************************************************************
		//	Class for loading, storing and accessing manager parameters.
		//	Loads initial settings from local config file, then checks to see if remainder of settings should be
		//		loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
		//		parameters database.
		//**********************************************************************************************************

		#region "Class variables"
			StringDictionary m_ParamDictionary = null;
			string m_ErrMsg = "";
			bool m_MCParamsLoaded = false;
		#endregion

		#region "Properties"
			string ErrMsg
			{
				get
				{
					return m_ErrMsg;
				}
			}	// End property
		#endregion

		#region "Methods"
			public clsMgrSettings()
			{
				if (!LoadSettings())
				{
					throw new ApplicationException("Unable to initialize manager settings class");
				}
			}	// End sub

			public bool LoadSettings()
			{
				m_ErrMsg = "";

				// If the param dictionary exists, it needs to be cleared out
				if (m_ParamDictionary != null)
				{
					m_ParamDictionary.Clear();
					m_ParamDictionary = null;
				}

				// Get settings from config file
				m_ParamDictionary = LoadMgrSettingsFromFile();

				//Test the settings retrieved from the config file
				if ( !CheckInitialSettings(m_ParamDictionary) )
				{
					//Error logging handled by CheckInitialSettings
					return false;
				}

				//Determine if manager is deactivated locally
				if (!bool.Parse( m_ParamDictionary["MgrActive_Local"]))
				{
///					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.WARN, "Manager deactivated locally");
					m_ErrMsg = "Manager deactivated locally";
					return false;
				}

				//Get remaining settings from database
				if ( !LoadMgrSettingsFromDB(ref m_ParamDictionary) )
				{
					//Error logging handled by LoadMgrSettingsFromDB
					return false;
				}

				//Set flag indicating params have been loaded from MC db
				m_MCParamsLoaded = true;

				//No problems found
				return true;
			}	// End sub

			private StringDictionary LoadMgrSettingsFromFile()
			{
				// Load initial settings into string dictionary for return
				StringDictionary RetDict = new StringDictionary();
				string TempStr;

//				My.Settings.Reload()
				//Manager config db connection string
				TempStr = StatusMessageDBUpdater.Properties.Settings.Default.MgrCnfgDbConnectStr;
				RetDict.Add("MgrCnfgDbConnectStr", TempStr);

				//Manager active flag
				TempStr = StatusMessageDBUpdater.Properties.Settings.Default.MgrActive_Local;
				RetDict.Add("MgrActive_Local", TempStr);

				//Manager name
				TempStr = StatusMessageDBUpdater.Properties.Settings.Default.MgrName;
				RetDict.Add("MgrName", TempStr);

				//Default settings in use flag
				TempStr = StatusMessageDBUpdater.Properties.Settings.Default.UsingDefaults;
				RetDict.Add("UsingDefaults", TempStr);

				//Update check interval
				TempStr = StatusMessageDBUpdater.Properties.Settings.Default.CheckForUpdateInterval;
				RetDict.Add("CheckForUpdateInterval", TempStr);

				return RetDict;
			}	// End sub

			private bool CheckInitialSettings(StringDictionary InpDict)
			{
				string MyMsg = null;

				//Verify manager settings dictionary exists
				if (InpDict == null)
				{
					MyMsg = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found";
///					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, MyMsg);
					return false;
				}

				//Verify intact config file was found
				if (bool.Parse(InpDict["UsingDefaults"]))
				{
					MyMsg = "clsMgrSettings.CheckInitialSettings(); Config file problem, default settings being used";
///					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, MyMsg);
					return false;
				}

				//No problems found
				return true;
			}	// End sub

			private bool LoadMgrSettingsFromDB()
			{
				return LoadMgrSettingsFromDB(ref m_ParamDictionary);
			}	// End sub

			public bool LoadMgrSettingsFromDB(ref StringDictionary MgrSettingsDict)
			{
				//Requests manager parameters from database. Input string specifies view to use. Performs retries if necessary.
				short RetryCount = 3;
				string MyMsg = null;
				string ParamKey = null;
				string ParamVal = null;

				string SqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" 
											+ m_ParamDictionary["MgrName"] + "'";

				//Get a table containing data for job
				 DataTable Dt = null;

				//Get a datatable holding the parameters for one manager
				while (RetryCount > 0)
				{
					try
					{
						using (SqlConnection Cn = new SqlConnection(MgrSettingsDict["MgrCnfgDbConnectStr"]))
						{
							using (SqlDataAdapter Da = new SqlDataAdapter(SqlStr, Cn))
							{
								using (DataSet Ds = new DataSet())
								{
									Da.Fill(Ds);
									Dt = Ds.Tables[0];
									//Ds
								}
								//Da
							}
						}
						//Cn
						break;
					}
					catch (System.Exception ex)
					{
						RetryCount -= 1;
						MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception getting manager settings from database: " + ex.Message;
						MyMsg = MyMsg + ", RetryCount = " + RetryCount.ToString();
						WriteErrorMsg(MyMsg);
						//Delay for 5 seconds before trying again
						System.Threading.Thread.Sleep(5000);
					}
				}

				//If loop exited due to errors, return false
				if (RetryCount < 1)
				{
					MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database";
					WriteErrorMsg(MyMsg);
					Dt.Dispose();
					return false;
				}

				//Verify at least one row returned
				if (Dt.Rows.Count < 1)
				{
					//Wrong number of rows returned
					MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Invalid row count retrieving manager settings: RowCount = ";
					MyMsg += Dt.Rows.Count.ToString();
					WriteErrorMsg(MyMsg);
					Dt.Dispose();
					return false;
				}

				//Fill a string dictionary with the manager parameters that have been found
				try
				{
					foreach (DataRow TestRow in Dt.Rows)
					{
						//Add the column heading and value to the dictionary
						ParamKey = DbCStr(TestRow[Dt.Columns["ParameterName"]]);
						ParamVal = DbCStr(TestRow[Dt.Columns["ParameterValue"]]);
						if (m_ParamDictionary.ContainsKey(ParamKey))
						{
							m_ParamDictionary[ParamKey] = ParamVal;
						}
						else
						{
							m_ParamDictionary.Add(ParamKey, ParamVal);
						}
					}
					return true;
				}
				catch (System.Exception ex)
				{
					MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table: " + ex.Message;
					WriteErrorMsg(MyMsg);
					return false;
				}
				finally
				{
					Dt.Dispose();
				}
			}	// End sub

			public string GetParam(string ItemKey)
			{
				string RetStr = m_ParamDictionary[ItemKey];
				return RetStr;
			}

			public void SetParam(string ItemKey, string ItemValue)
			{
				m_ParamDictionary[ItemKey]=ItemValue;
			}

			private string DbCStr(object InpObj)
			{
				if (InpObj == null)
				{
					return "";
				}
				else
				{
					return InpObj.ToString();
				}
			}

			private void WriteErrorMsg(string ErrMsg)
			{
				if (m_MCParamsLoaded)
				{
///					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg);
				}
				else
				{
///					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, ErrMsg);
				}
			}
		#endregion
	}	// End class
}	// End namespace
