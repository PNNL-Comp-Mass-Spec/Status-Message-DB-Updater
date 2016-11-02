using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.IO;
using System.Windows.Forms;
using System.Xml;
using log4net;

namespace StatusMessageDBUpdater
{
    class clsMainProg
    {
        private static readonly ILog mainLog = LogManager.GetLogger("MainLog");

        #region "Constants"
        
        private const int TIMER_UPDATE_INTERVAL_MSEC = 1000;

        #endregion		

        #region "Class variables"
        
        string mMgrName;
        private DBAccess mDba;
        private int mDBUpdateIntervalSeconds;
        private bool mKeepRunning = true;
        bool mRestartAfterShutdown;
        bool mLogStatusToMessageQueue;
        bool mMgrActive = true;

        DateTime mStartTime;
        int mMaxRuntimeHours = 1000;

        private DateTime mLastUpdate = DateTime.UtcNow;

        clsMgrSettings mMgrSettings;

        private clsMessageHandler mMessageHandler;
        private bool m_MsgQueueInitSuccess;

        readonly System.Collections.Queue m_SendMessageQueue = new System.Collections.Queue();
        private System.Timers.Timer m_SendMessageQueueProcessor;

        private MessageAccumulator mMsgAccumulator;
        XmlDocument mXmlStatusDocument;

        #endregion

        #region "Methods"

        /// <summary>
        /// Initializes the manager
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        public bool InitMgr(int maxRunTimeHours)
        {
            //Get the manager settings
            mMgrSettings = null;
            try
            {
                mMgrSettings = new clsMgrSettings(mainLog);
                mainLog.Info("Read manager settings from Manager Control Database");
            }
            catch
            {
                return false; //Failures are logged by clsMgrSettings to local emergency log file
            }

            mMgrActive = (mMgrSettings.GetParam("mgractive") != "False");

            mStartTime = DateTime.UtcNow;
            if (maxRunTimeHours < 1)
                maxRunTimeHours = 1;

            mMaxRuntimeHours = maxRunTimeHours;

            // processor name
            mMgrName = mMgrSettings.GetParam("MgrName");
            mainLog.Info("Manager:" + mMgrName);

            m_SendMessageQueueProcessor = new System.Timers.Timer(TIMER_UPDATE_INTERVAL_MSEC);
            m_SendMessageQueueProcessor.Elapsed += m_SendMessageQueueProcessor_Elapsed;
            m_SendMessageQueueProcessor.Start();

            // status message skeleton
            mXmlStatusDocument = new XmlDocument();
            var fi = new FileInfo(Application.ExecutablePath);
            mXmlStatusDocument.Load(Path.Combine(fi.DirectoryName, "status_template.xml"));
            mXmlStatusDocument.SelectSingleNode("//MgrName").InnerText = mMgrName;
            mXmlStatusDocument.SelectSingleNode("//LastStartTime").InnerText = System.DateTime.Now.ToString(CultureInfo.InvariantCulture);
            mXmlStatusDocument.SelectSingleNode("//MgrStatus").InnerText = "";

            //---- initialize the connection parameter fields ----
            var messageBrokerURL = mMgrSettings.GetParam("MessageQueueURI");
            var messageTopicName = mMgrSettings.GetParam("StatusMsgIncomingTopic");
            var monitorTopicName = mMgrSettings.GetParam("MessageQueueTopicMgrStatus"); // topic to send
            var brodcastTopicName = mMgrSettings.GetParam("BroadcastQueueTopic");
            mLogStatusToMessageQueue = (mMgrSettings.GetParam("LogStatusToMessageQueue") == "True");

            mMessageHandler = new clsMessageHandler
            {
                MgrName = mMgrName,
                BrokerUri = messageBrokerURL,
                InputStatusTopicName = messageTopicName,
                OutputStatusTopicName = monitorTopicName,
                BroadcastTopicName = brodcastTopicName
            };


            // Initialize the message queue
            // Start this in a separate thread so that we can abort the initialization if necessary
            if (!InitializeMessageQueue())
            {
                return false;
            }

            // make a new processor message accumulator and start it running
            mMsgAccumulator = new MessageAccumulator();

            mMessageHandler.InputMessageReceived += mMsgAccumulator.subscriber_OnMessageReceived;
            mMessageHandler.BroadcastReceived += OnMsgHandler_BroadcastReceived;

            //---- seconds between database updates ----
            var interval = mMgrSettings.GetParam("StatusMsgDBUpdateInterval", "30");
            mDBUpdateIntervalSeconds = int.Parse(interval);

            // create a new database access object
            var dbConnStr = mMgrSettings.GetParam("connectionstring");
            mDba = new DBAccess(dbConnStr);

            //---- Connect message handler events ----

            //---- Everything worked ----
            return true;
        }

        private bool InitializeMessageQueue()
        {

            var worker = new System.Threading.Thread(InitializeMessageQueueWork);
            worker.Start();

            // Wait a maximum of 15 seconds
            if (!worker.Join(15000))
            {
                worker.Abort();
                m_MsgQueueInitSuccess = false;
                var errMessage = "Unable to initialize the message queue (timeout after 15 seconds); " + mMessageHandler.BrokerUri;
                mainLog.Error(errMessage);
                Console.WriteLine(errMessage);
            }

            return m_MsgQueueInitSuccess;
        }

        private void InitializeMessageQueueWork()
        {

            if (!mMessageHandler.Init())
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

            UpdateManagerStatus("Starting");

            QueueMessageToSend(mXmlStatusDocument.InnerXml);

            while (mKeepRunning)
            {

                // sleep for 5 seconds, wake up and count down
                // and see if we are supposed to stop or proceed
                var timeRemaining = mDBUpdateIntervalSeconds;
                do
                {
                    Thread.Sleep(5000);
                    timeRemaining -= 5;
                    if (!mKeepRunning) 
                        break;
                } while (timeRemaining > 0);

                if (System.DateTime.UtcNow.Subtract(mStartTime).TotalHours >= mMaxRuntimeHours)
                    break;

                if (!mKeepRunning) 
                    break;

                // are we active?
                if (mMgrActive)
                {
                    UpdateManagerStatus("Running");
                }
                else
                {
                    mainLog.Info("Manager is inactive");
                    UpdateManagerStatus("Inactive");

                    QueueMessageToSend(mXmlStatusDocument.InnerXml);

                    //Test to determine if we need to reload config from db
                    TestForConfigReload();
                    continue;
                }

                // from the message accumulator, get list of processors 
                // that have received messages since the last refresh and
                // reset the list in the accumulator
                var Processors = mMsgAccumulator.changedList.Keys.ToArray();
                mMsgAccumulator.changedList.Clear();
                mMsgAccumulator.msgCount = 0;

                var progMsg = "MsgDB program updated " + Processors.Length + " at " + DateTime.Now;
                mainLog.Info(progMsg);

                try
                {
                    mDba.Connect();

                    // build concatenated XML for all new status messages
                    var concatMessages = new System.Text.StringBuilder(1024);

                    foreach (var Processor in Processors)
                    {
                        var doc = new XmlDocument();
                        doc.LoadXml(mMsgAccumulator.statusList[Processor]);
                        var n = doc.SelectSingleNode("//Root");
                        if (n != null)
                        {
                            concatMessages.Append(n.OuterXml);
                        }
                    }
                    progMsg = "Size:" + concatMessages.Length.ToString();
                    mainLog.Info(progMsg);

                    // update the database
                    var message = "";
                    var err = mDba.UpdateDatabase(concatMessages, ref message);

                    // send status
                    if (mLogStatusToMessageQueue)
                    {
                        UpdateXmlNode(mXmlStatusDocument, "//LastUpdate", DateTime.Now.ToString(CultureInfo.InvariantCulture));
                        if (err)
                        {
                            mainLog.Error(message);
                            UpdateXmlNode(mXmlStatusDocument, "//Status", "Error");
                            UpdateXmlNode(mXmlStatusDocument, "//ErrMsg", message);
                            QueueMessageToSend(mXmlStatusDocument.InnerXml);
                        }
                        else
                        {
                            mainLog.Info("Result: " + message);
                            UpdateXmlNode(mXmlStatusDocument, "//Status", "Good");
                            UpdateXmlNode(mXmlStatusDocument, "//MostRecentLogMessage", message);                            
                            QueueMessageToSend(mXmlStatusDocument.InnerXml);
                        }
                    }
                }
                catch (Exception e)
                {
                    mainLog.Error(e.Message);
                    UpdateXmlNode(mXmlStatusDocument, "//Status", "Exception");
                    UpdateXmlNode(mXmlStatusDocument, "//ErrMsg", e.Message);
                    QueueMessageToSend(mXmlStatusDocument.InnerXml);
                }
                mDba.Disconnect();

                //Test to determine if we need to reload config from db
                TestForConfigReload();
            }

            if (!mRestartAfterShutdown)
                mainLog.Info("Process interrupted, " + "Restart:" + mRestartAfterShutdown);

            UpdateManagerStatus("Stopped");
            QueueMessageToSend(mXmlStatusDocument.InnerXml);

            // Sleep for 5 seconds to allow the message to be sent
            var dtContinueTime = System.DateTime.UtcNow.AddMilliseconds(5 * TIMER_UPDATE_INTERVAL_MSEC);
            while (System.DateTime.UtcNow < dtContinueTime)
                Thread.Sleep(500);

            mMessageHandler.Dispose();
            return mRestartAfterShutdown;
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
            var machineList = new List<string>();
            var machineCommand = "<Undefined>";

            try
            {
                var doc = new XmlDocument();
                doc.LoadXml(cmdText);
                var managerNodes = doc.SelectNodes("//Managers/*");
                if (managerNodes != null)
                {
                    foreach (XmlNode xn in managerNodes)
                    {
                        machineList.Add(xn.InnerText);
                    }
                }
                var messageNode = doc.SelectSingleNode("//Message");
                if (messageNode != null)
                    machineCommand = messageNode.InnerText;
            }
            catch (Exception ex)
            {
                mainLog.Error("Exception while parsing broadcast string:" + ex.Message);
                return;
            }

            // Determine if the message applies to this machine
            if (!machineList.Contains(mMgrName))
            {
                // Received command doesn't apply to this manager
                mainLog.Debug("Received command not applicable to this manager instance");
                return;
            }

            // Get the command and take appropriate action
            switch (machineCommand.ToLower())
            {
                case "shutdown":
                    mainLog.Info("Shutdown message received");
                    mKeepRunning = false;
                    mRestartAfterShutdown = false;
                    break;
                case "readconfig":
                    mainLog.Info("Reload config message received");
                    mKeepRunning = false;
                    mRestartAfterShutdown = true;
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
            // The update interval comes from file StatusMessageDBUpdater.exe.config
            // The default is 10 minutes
            var updateIntervalText = mMgrSettings.GetParam("CheckForUpdateInterval", "10");
            double updateIntervalMinutes;
            if (!double.TryParse(updateIntervalText, out updateIntervalMinutes))
                updateIntervalMinutes = 10;

            var testTime = mLastUpdate.AddMinutes(updateIntervalMinutes);
            var currTime = DateTime.UtcNow;

            if (currTime.CompareTo(testTime) <= 0)
            {
                return;
            }

            // Time to reload the config
            mainLog.Info("Reloading config from MC database");
            mKeepRunning = false;
            mRestartAfterShutdown = true;
            mLastUpdate = DateTime.UtcNow;
        }

        void m_SendMessageQueueProcessor_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {

            while (System.Collections.Queue.Synchronized(m_SendMessageQueue).Count > 0)
            {
                try
                {
                    var message = (string)System.Collections.Queue.Synchronized(m_SendMessageQueue).Dequeue();

                    if (string.IsNullOrEmpty(message))
                    {
                        continue;
                    }

                    if (mMessageHandler == null)
                    {
                        mainLog.Warn("MessageHandler is null; unable to send queued message");
                    }
                    else
                    {
                        // Send the message on a separate thread
                        var worker = new System.Threading.Thread(() => SendQueuedMessageWork(message));

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

            } // End While

        }


        private void SendQueuedMessageWork(string message)
        {
            if (mMessageHandler != null)
                mMessageHandler.SendMessage(mMgrName, message);
        }

        private void UpdateManagerStatus(string managerStatus)
        {
            UpdateXmlNode(mXmlStatusDocument, "//MgrStatus", managerStatus);
            UpdateXmlNode(mXmlStatusDocument, "//Status", managerStatus);
            UpdateXmlNode(mXmlStatusDocument, "//LastUpdate", DateTime.Now.ToString(CultureInfo.InvariantCulture));
        }

        private void UpdateXmlNode(XmlDocument statusDocument, string nodeXPath, string newValue)
        {
            var selectedNode = statusDocument.SelectSingleNode(nodeXPath);
            if (selectedNode != null)
                selectedNode.InnerText = newValue;
        }

        #endregion
    }
}
