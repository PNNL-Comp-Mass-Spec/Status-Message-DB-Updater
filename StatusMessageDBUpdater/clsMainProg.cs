using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Windows.Forms;
using System.Xml;
using System.Collections.Specialized;
using log4net;

namespace StatusMessageDBUpdater
{
	class clsMainProg
	{
		private static readonly ILog mainLog = LogManager.GetLogger("MainLog");

		#region "Enums"
		private enum BroadcastCmdType
		{
			Shutdown,
			ReadConfig,
			Invalid
		}
		#endregion

		#region "Class variables"
		string mgrName = null;
		private DBAccess dba = null;
		private int dbUpdateInterval;
		private int dbUpdateIntervalMS;
		private bool run = true;
		bool restart = false;
		bool LogStatusToMessageQueue;
		bool mgrActive = true;
		private DateTime LastConfigCheck = DateTime.UtcNow;
		clsMgrSettings mgrSettings = null;

		private clsMessageHandler messageHandler = null;
		private bool m_MsgQueueInitSuccess = false;

		System.Collections.Queue m_SendMessageQueue = new System.Collections.Queue();
		private System.Timers.Timer m_SendMessageQueueProcessor;

		private MessageAccumulator m_ma = null;
		XmlDocument doc = null;
		#endregion

		#region "Methods"

		/// <summary>
		/// Initializes the manager
		/// </summary>
		/// <returns>TRUE for success, FALSE for failure</returns>
		public bool InitMgr()
		{
			//Get the manager settings
			mgrSettings = null;
			try
			{
				mgrSettings = new clsMgrSettings();
				mainLog.Info("Read manager settings from Manager Control Database");
			}
			catch
			{
				return false; //Failures are logged by clsMgrSettings to local emergency log file
			}

			this.mgrActive = (mgrSettings.GetParam("mgractive") != "False");

			// processor name
			this.mgrName = mgrSettings.GetParam("MgrName");
			mainLog.Info("Manager:" + this.mgrName);

			m_SendMessageQueueProcessor = new System.Timers.Timer(250);
			m_SendMessageQueueProcessor.Elapsed += new System.Timers.ElapsedEventHandler(m_SendMessageQueueProcessor_Elapsed);
			m_SendMessageQueueProcessor.Start();

			// status message skeleton
			this.doc = new XmlDocument();
			FileInfo fi = new FileInfo(Application.ExecutablePath);
			this.doc.Load(System.IO.Path.Combine(fi.DirectoryName, "status_template.xml"));
			this.doc.SelectSingleNode("//MgrName").InnerText = this.mgrName;
			this.doc.SelectSingleNode("//LastStartTime").InnerText = System.DateTime.Now.ToString();
			this.doc.SelectSingleNode("//MgrStatus").InnerText = "";

			//---- initialize the connection parameter fields ----
			string messageBrokerURL = mgrSettings.GetParam("MessageQueueURI");
			string messageTopicName = mgrSettings.GetParam("StatusMsgIncomingTopic");
			string monitorTopicName = mgrSettings.GetParam("MessageQueueTopicMgrStatus"); // topic to send
			string brodcastTopicName = mgrSettings.GetParam("BroadcastQueueTopic");
			this.LogStatusToMessageQueue = (mgrSettings.GetParam("LogStatusToMessageQueue") == "True");

			this.messageHandler = new clsMessageHandler();
			this.messageHandler.MgrName = mgrName;
			this.messageHandler.BrokerUri = messageBrokerURL;
			this.messageHandler.InputStatusTopicName = messageTopicName;
			this.messageHandler.OutputStatusTopicName = monitorTopicName;
			this.messageHandler.BroadcastTopicName = brodcastTopicName;


			// Initialize the message queue
			// Start this in a separate thread so that we can abort the initialization if necessary
			if (!InitializeMessageQueue())
			{
				return false;
			}

			// make a new processor message accumulator and start it running
			this.m_ma = new MessageAccumulator();

			this.messageHandler.InputMessageReceived += this.m_ma.subscriber_OnMessageReceived;
			this.messageHandler.BroadcastReceived += this.OnMsgHandler_BroadcastReceived;

			//---- seconds between database updates ----
			string interval = mgrSettings.GetParam("StatusMsgDBUpdateInterval");
			this.dbUpdateInterval = int.Parse(interval);
			this.dbUpdateIntervalMS = 1000 * this.dbUpdateInterval;

			// create a new database access object
			string dbConnStr = mgrSettings.GetParam("connectionstring");
			this.dba = new DBAccess(dbConnStr);

			//---- Connect message handler events ----

			//---- Everything worked ----
			return true;
		}

		private bool InitializeMessageQueue()
		{

			System.Threading.Thread worker = new System.Threading.Thread(InitializeMessageQueueWork);
			worker.Start();

			// Wait a maximum of 15 seconds
			if (!worker.Join(15000))
			{
				worker.Abort();
				m_MsgQueueInitSuccess = false;
				string errMessage = "Unable to initialize the message queue (timeout after 15 seconds); " + messageHandler.BrokerUri;
				mainLog.Error(errMessage);
				Console.WriteLine(errMessage);
			}

			return m_MsgQueueInitSuccess;
		}

		private void InitializeMessageQueueWork()
		{

			if (!this.messageHandler.Init())
			{
				Console.WriteLine("Message handler init error");
				m_MsgQueueInitSuccess = false;
			}
			else
			{
				m_MsgQueueInitSuccess = true;
			}

			return;
		}

		/// <summary>
		/// Repetitively updates database with accumulated status messages
		/// since last update
		/// </summary>
		/// <returns>TRUE for restart required, FALSE for restart not required</returns>
		public bool DoProcess()
		{
			mainLog.Info("Process started");
			this.doc.SelectSingleNode("//MgrStatus").InnerText = "Starting";
			this.doc.SelectSingleNode("//LastUpdate").InnerText = DateTime.Now.ToString();

			QueueMessageToSend(this.doc.InnerXml);

			while (this.run)
			{

				// sleep for 5 seconds, wake up and count down
				// and see if we are supposed to stop or proceed
				int timeRemaining = this.dbUpdateInterval;
				do
				{
					Thread.Sleep(5000);
					timeRemaining -= 5;
					if (!run) break;
				} while (timeRemaining > 0);
				if (!run) break;

				// are we active?
				if (this.mgrActive)
				{
					this.doc.SelectSingleNode("//MgrStatus").InnerText = "Running";
					this.doc.SelectSingleNode("//LastUpdate").InnerText = System.DateTime.Now.ToString();
				}
				else
				{
					mainLog.Info("Manager is inactive");
					this.doc.SelectSingleNode("//LastUpdate").InnerText = System.DateTime.Now.ToString();
					this.doc.SelectSingleNode("//Status").InnerText = "Inactive";
					this.doc.SelectSingleNode("//MgrStatus").InnerText = "Inactive";
					QueueMessageToSend(this.doc.InnerXml);

					//Test to determine if we need to reload config from db
					TestForConfigReload();
					continue;
				}

				// from the message accumulator, get list of processors 
				// that have received messages since the last refresh and
				// reset the list in the accumulator
				string[] Processors = m_ma.changedList.Keys.ToArray();
				m_ma.changedList.Clear();
				int msgCount = m_ma.msgCount;
				m_ma.msgCount = 0;

				string progMsg = "MsgDB program updated " + Processors.Length.ToString() + " at " + DateTime.Now.ToString();
				mainLog.Info(progMsg);

				try
				{
					dba.Connect();

					// build concatenated XML for all new status messages
					System.Text.StringBuilder concatMessages = new System.Text.StringBuilder(1024);

					foreach (string Processor in Processors)
					{
						XmlDocument doc = new XmlDocument();
						doc.LoadXml(m_ma.statusList[Processor]);
						XmlNode n = doc.SelectSingleNode("//Root");
						if (n != null)
						{
							concatMessages.Append(n.OuterXml);
						}
					}
					progMsg = "Size:" + concatMessages.Length.ToString();
					mainLog.Info(progMsg);

					// update the database
					string message = "";
					bool err = dba.UpdateDatabase(concatMessages, ref message);

					// send status
					if (this.LogStatusToMessageQueue)
					{
						this.doc.SelectSingleNode("//LastUpdate").InnerText = System.DateTime.Now.ToString();
						if (err)
						{
							mainLog.Error(message);
							this.doc.SelectSingleNode("//Status").InnerText = "Error";
							this.doc.SelectSingleNode("//ErrMsg").InnerText = message;
							QueueMessageToSend(this.doc.InnerXml);
						}
						else
						{
							mainLog.Info("Result:" + message);
							this.doc.SelectSingleNode("//Status").InnerText = "Good";
							this.doc.SelectSingleNode("//MostRecentLogMessage").InnerText = message;
							QueueMessageToSend(this.doc.InnerXml);
						}
					}
				}
				catch (Exception e)
				{
					mainLog.Error(e.Message);
					this.doc.SelectSingleNode("//Status").InnerText = "Exception";
					this.doc.SelectSingleNode("//ErrMsg").InnerText = "message";
					QueueMessageToSend(this.doc.InnerXml);
				}
				dba.Disconnect();

				//Test to determine if we need to reload config from db
				TestForConfigReload();
			}
			mainLog.Info("Process interrupted, " + "Restart:" + this.restart.ToString());
			this.doc.SelectSingleNode("//LastUpdate").InnerText = System.DateTime.Now.ToString();
			this.doc.SelectSingleNode("//Status").InnerText = "Stopped";
			this.doc.SelectSingleNode("//MgrStatus").InnerText = "Stopped";
			QueueMessageToSend(this.doc.InnerXml);

			this.messageHandler.Dispose();
			return this.restart;
		}

		/// <summary>
		/// Handles broacast messages for control of the manager
		/// </summary>
		/// <param name="cmdText">Text of received message</param>
		void OnMsgHandler_BroadcastReceived(string cmdText)
		{
			mainLog.Info("clsMainProgram.OnMsgHandler_BroadcastReceived: Broadcast message received: " + cmdText);

			// parse command XML and get command text and
			// list of machines that command applies to
			List<string> MachineList = new List<string>();
			string MachCmd;
			try
			{
				XmlDocument doc = new XmlDocument();
				doc.LoadXml(cmdText);
				foreach (XmlNode xn in doc.SelectNodes("//Managers/*"))
				{
					MachineList.Add(xn.InnerText);
				}
				MachCmd = doc.SelectSingleNode("//Message").InnerText;
			}
			catch (Exception Ex)
			{
				mainLog.Error("Exception while parsing broadcast string:" + Ex.Message);
				return;
			}

			// Determine if the message applies to this machine
			if (!MachineList.Contains(this.mgrName))
			{
				// Received command doesn't apply to this manager
				mainLog.Debug("Received command not applicable to this manager instance");
				return;
			}

			// Get the command and take appropriate action
			switch (MachCmd.ToLower())
			{
				case "shutdown":
					mainLog.Info("Shutdown message received");
					this.run = false;
					this.restart = false;
					break;
				case "readconfig":
					mainLog.Info("Reload config message received");
					this.run = false;
					this.restart = true;
					break;
				default:
					mainLog.Warn("Invalid broadcast command received: " + cmdText);
					break;
			}
		}	// End sub

		private void QueueMessageToSend(string message)
		{
			System.Collections.Queue.Synchronized(m_SendMessageQueue).Enqueue(message);
		}

		private void TestForConfigReload()
		{
			DateTime testTime = LastConfigCheck.AddMinutes(double.Parse(mgrSettings.GetParam("CheckForUpdateInterval")));	//Interval is in minutes
			DateTime currTime = DateTime.UtcNow;
			if (currTime.CompareTo(testTime) > 0)
			{
				//Time to reload the config
				mainLog.Info("Reloading config from MC database");
				this.run = false;
				this.restart = true;
				LastConfigCheck = DateTime.UtcNow;
			}
		}	// End sub

		void m_SendMessageQueueProcessor_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
		{

			if (System.Collections.Queue.Synchronized(m_SendMessageQueue).Count > 0)
			{
				try
				{
					string message;
					message = (string)System.Collections.Queue.Synchronized(m_SendMessageQueue).Dequeue();

					if (!string.IsNullOrEmpty(message))
					{
						// Send the message on a separate thread
						System.Threading.Thread worker = new System.Threading.Thread(() => SendQueuedMessageWork(message));

						worker.Start();
						// Wait up to 15 seconds
						if (!worker.Join(15000))
						{
							mainLog.Error("Unable to send queued message (timeout after 15 seconds); aborting");
							worker.Abort();
						}
					}
				}
				catch
				{
					// Ignore this; likely was handled by a different thread
				}

			}

		}


		private void SendQueuedMessageWork(string message)
		{
			if (this.messageHandler != null)
				this.messageHandler.SendMessage(this.mgrName, message);

			return;
		}
		#endregion
	} // end class
} // end namespace
