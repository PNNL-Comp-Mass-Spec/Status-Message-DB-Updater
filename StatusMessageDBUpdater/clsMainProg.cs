using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using PRISM;

namespace StatusMessageDBUpdater
{
    public class clsMainProg : EventNotifier
    {

        #region "Constants"

        private const int TIMER_UPDATE_INTERVAL_MSEC = 1000;

        private const int RECENT_STATUS_FILES_TO_KEEP = 30;

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

        private bool mMsgQueueInitSuccess;

        readonly Queue mSendMessageQueue = new Queue();

        private System.Timers.Timer mSendMessageQueueProcessor;

        private MessageAccumulator mMsgAccumulator;

        XmlDocument mXmlStatusDocument;

        #endregion

        #region "Methods"

        /// <summary>
        /// Initializes the manager
        /// </summary>
        /// <returns>True for success, false if an error</returns>
        public bool InitMgr(int maxRunTimeHours)
        {
            mMgrSettings = null;
            try
            {
                mMgrSettings = new clsMgrSettings();
                RegisterEvents(mMgrSettings);

                if (!mMgrSettings.LoadSettings())
                {
                    if (string.Equals(mMgrSettings.ErrMsg, clsMgrSettings.DEACTIVATED_LOCALLY))
                        throw new ApplicationException(clsMgrSettings.DEACTIVATED_LOCALLY);

                    throw new ApplicationException("Unable to initialize manager settings class: " + mMgrSettings.ErrMsg);
                }

                OnStatusEvent("Loaded manager settings from Manager Control Database");
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error loading manager settings", ex);
                return false;
            }

            mMgrActive = (mMgrSettings.GetParam("mgractive").ToLower() != "false");

            mStartTime = DateTime.UtcNow;
            if (maxRunTimeHours < 1)
                maxRunTimeHours = 1;

            mMaxRuntimeHours = maxRunTimeHours;

            // Manager name
            mMgrName = mMgrSettings.GetParam("MgrName");
            OnStatusEvent("Manager: " + mMgrName);

            mSendMessageQueueProcessor = new System.Timers.Timer(TIMER_UPDATE_INTERVAL_MSEC);
            mSendMessageQueueProcessor.Elapsed += SendMessageQueueProcessor_Elapsed;
            mSendMessageQueueProcessor.Start();

            // Status message skeleton
            mXmlStatusDocument = new XmlDocument();
            var exePath = new FileInfo(PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppPath());
            if (exePath.DirectoryName == null)
            {
                OnErrorEvent("Parent directory for the .exe is null: " + exePath.FullName);
                return false;
            }

            var templatePath = new FileInfo(Path.Combine(exePath.DirectoryName, "status_template.xml"));
            if (!templatePath.Exists)
            {
                OnErrorEvent("Status template file not found: " + templatePath.FullName);
                return false;
            }

            mXmlStatusDocument.Load(templatePath.FullName);

            UpdateXmlNode(mXmlStatusDocument, "//MgrName", mMgrName);
            UpdateXmlNode(mXmlStatusDocument, "//LastStartTime", DateTime.Now.ToString(CultureInfo.InvariantCulture));
            UpdateXmlNode(mXmlStatusDocument, "//MgrStatus", "");

            // Initialize the connection parameter fields
            var messageBrokerURL = mMgrSettings.GetParam("MessageQueueURI");
            var messageTopicName = mMgrSettings.GetParam("StatusMsgIncomingTopic");
            var monitorTopicName = mMgrSettings.GetParam("MessageQueueTopicMgrStatus"); // Topic to send
            var brodcastTopicName = mMgrSettings.GetParam("BroadcastQueueTopic");
            mLogStatusToMessageQueue = (mMgrSettings.GetParam("LogStatusToMessageQueue") == "True");

            mMessageHandler = new clsMessageHandler()
            {
                BrokerUri = messageBrokerURL,
                InputStatusTopicName = messageTopicName,
                OutputStatusTopicName = monitorTopicName,
                BroadcastTopicName = brodcastTopicName
            };
            RegisterEvents(mMessageHandler);

            // Initialize the message queue
            // Start this in a separate thread so that we can abort the initialization if necessary
            if (!InitializeMessageQueue())
            {
                return false;
            }

            // Make a new processor message accumulator and start it running
            mMsgAccumulator = new MessageAccumulator();

            mMessageHandler.InputMessageReceived += mMsgAccumulator.Subscriber_OnMessageReceived;
            mMessageHandler.BroadcastReceived += OnMsgHandler_BroadcastReceived;

            // Seconds between database updates
            var interval = mMgrSettings.GetParam("StatusMsgDBUpdateInterval", "30");
            mDBUpdateIntervalSeconds = int.Parse(interval);

            // Create a new database access object
            var dbConnStr = mMgrSettings.GetParam("connectionstring");
            mDba = new DBAccess(dbConnStr);

            return true;
        }

        private bool InitializeMessageQueue()
        {

            var worker = new Thread(InitializeMessageQueueWork);
            worker.Start();

            // Wait a maximum of 15 seconds
            if (!worker.Join(15000))
            {
                worker.Abort();
                mMsgQueueInitSuccess = false;
                OnErrorEvent("Unable to initialize the message queue (timeout after 15 seconds); " + mMessageHandler.BrokerUri);
            }

            return mMsgQueueInitSuccess;
        }

        private void InitializeMessageQueueWork()
        {

            if (!mMessageHandler.Init())
            {
                mMsgQueueInitSuccess = false;
            }
            else
            {
                mMsgQueueInitSuccess = true;
            }

        }

        /// <summary>
        /// Repetitively updates database with accumulated status messages
        /// since last update
        /// </summary>
        /// <returns>TRUE for restart required, FALSE for restart not required</returns>
        public bool DoProcess()
        {
            OnStatusEvent("Process started");

            UpdateManagerStatus("Starting");

            QueueMessageToSend(mXmlStatusDocument.InnerXml);

            while (mKeepRunning)
            {

                // Sleep for 5 seconds, wake up and count down
                // and see if we are supposed to stop or proceed
                var timeRemaining = mDBUpdateIntervalSeconds;
                do
                {
                    Thread.Sleep(5000);
                    timeRemaining -= 5;
                    if (!mKeepRunning)
                        break;
                } while (timeRemaining > 0);

                if (DateTime.UtcNow.Subtract(mStartTime).TotalHours >= mMaxRuntimeHours)
                    break;

                if (!mKeepRunning)
                    break;

                // Are we active?
                if (mMgrActive)
                {
                    UpdateManagerStatus("Running");
                }
                else
                {
                    OnStatusEvent("Manager is inactive");
                    UpdateManagerStatus("Inactive");

                    QueueMessageToSend(mXmlStatusDocument.InnerXml);

                    // Test to determine if we need to reload config from db
                    TestForConfigReload();
                    continue;
                }

                // From the message accumulator, get list of processors
                // that have received messages since the last refresh and
                // reset the list in the accumulator
                var processors = mMsgAccumulator.ChangedList.ToList();
                mMsgAccumulator.ChangedList.Clear();

                OnStatusEvent("Updated status for " + processors.Count + " processors");

                try
                {
                    mDba.Connect();

                    // Build concatenated XML for all new status messages
                    var concatMessages = new StringBuilder(1024);

                    foreach (var processor in processors)
                    {
                        var doc = new XmlDocument();
                        if (!mMsgAccumulator.StatusList.TryGetValue(processor, out var statusXML))
                            continue;

                        doc.LoadXml(statusXML);
                        var rootNode = doc.SelectSingleNode("//Root");
                        if (rootNode != null)
                        {
                            concatMessages.Append(rootNode.OuterXml);
                        }
                    }
                    OnDebugEvent("Size: " + concatMessages.Length);

                    // Update the database by calling stored procedure UpdateManagerAndTaskStatusXML
                    var success = mDba.UpdateDatabase(concatMessages, out var message);

                    // Save the XML to disk, keeping the 30 most recent status files
                    WriteStatusXmlToDisk(concatMessages);

                    // Send status
                    if (mLogStatusToMessageQueue)
                    {
                        UpdateXmlNode(mXmlStatusDocument, "//LastUpdate", DateTime.Now.ToString(CultureInfo.InvariantCulture));
                        if (!success)
                        {
                            OnErrorEvent(message);
                            UpdateXmlNode(mXmlStatusDocument, "//Status", "Error");
                            UpdateXmlNode(mXmlStatusDocument, "//ErrMsg", message);
                            QueueMessageToSend(mXmlStatusDocument.InnerXml);
                        }
                        else
                        {
                            // Example message:
                            // Messages:66, PreservedA:0, PreservedB:66, InsertedA:0, InsertedB:0, INFO
                            OnDebugEvent("Result: " + message);
                            UpdateXmlNode(mXmlStatusDocument, "//Status", "Good");
                            UpdateXmlNode(mXmlStatusDocument, "//MostRecentLogMessage", message);
                            QueueMessageToSend(mXmlStatusDocument.InnerXml);
                        }
                    }
                }
                catch (Exception e)
                {
                    OnErrorEvent(e.Message);
                    UpdateXmlNode(mXmlStatusDocument, "//Status", "Exception");
                    UpdateXmlNode(mXmlStatusDocument, "//ErrMsg", e.Message);
                    QueueMessageToSend(mXmlStatusDocument.InnerXml);
                }
                mDba.Disconnect();

                // Test to determine if we need to reload config from db
                TestForConfigReload();

            }

            if (!mRestartAfterShutdown)
                OnStatusEvent("Process interrupted, " + "Restart:" + mRestartAfterShutdown);

            UpdateManagerStatus("Stopped");
            QueueMessageToSend(mXmlStatusDocument.InnerXml);

            // Sleep for 5 seconds to allow the message to be sent
            var dtContinueTime = DateTime.UtcNow.AddMilliseconds(5000);
            while (DateTime.UtcNow < dtContinueTime)
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
            OnStatusEvent("clsMainProgram.OnMsgHandler_BroadcastReceived: Broadcast message received: " + cmdText);

            // Parse command XML and get command text and
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
                OnErrorEvent("Exception while parsing broadcast string", ex);
                return;
            }

            // Determine if the message applies to this machine
            if (!machineList.Contains(mMgrName))
            {
                // Received command doesn't apply to this manager
                OnDebugEvent("Received command not applicable to this manager instance");
                return;
            }

            // Get the command and take appropriate action
            switch (machineCommand.ToLower())
            {
                case "shutdown":
                    OnStatusEvent("Shutdown message received");
                    mKeepRunning = false;
                    mRestartAfterShutdown = false;
                    break;
                case "readconfig":
                    OnStatusEvent("Reload config message received");
                    mKeepRunning = false;
                    mRestartAfterShutdown = true;
                    break;
                default:
                    OnWarningEvent("Invalid broadcast command received: " + cmdText);
                    break;
            }
        }

        private void QueueMessageToSend(string message)
        {
            Queue.Synchronized(mSendMessageQueue).Enqueue(message);
        }

        private void TestForConfigReload()
        {
            // The update interval comes from file StatusMessageDBUpdater.exe.config
            // The default is 60 minutes
            var updateIntervalText = mMgrSettings.GetParam("CheckForUpdateInterval", "60");
            if (!double.TryParse(updateIntervalText, out var updateIntervalMinutes))
                updateIntervalMinutes = 60;

            var testTime = mLastUpdate.AddMinutes(updateIntervalMinutes);
            var currTime = DateTime.UtcNow;

            if (currTime.CompareTo(testTime) <= 0)
            {
                return;
            }

            // Time to reload the config
            OnStatusEvent("Reloading config from MC database");
            mKeepRunning = false;
            mRestartAfterShutdown = true;
            mLastUpdate = DateTime.UtcNow;
        }

        void SendMessageQueueProcessor_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {

            while (Queue.Synchronized(mSendMessageQueue).Count > 0)
            {
                try
                {
                    var message = (string)Queue.Synchronized(mSendMessageQueue).Dequeue();

                    if (string.IsNullOrEmpty(message))
                    {
                        continue;
                    }

                    if (mMessageHandler == null)
                    {
                        OnWarningEvent("MessageHandler is null; unable to send queued message");
                    }
                    else
                    {
                        // Send the message on a separate thread
                        var worker = new Thread(() => SendQueuedMessageWork(message));

                        worker.Start();

                        // Wait up to 15 seconds
                        if (!worker.Join(15000))
                        {
                            OnErrorEvent("Unable to send queued message (timeout after 15 seconds); aborting");
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
            mMessageHandler?.SendMessage(mMgrName, message);
        }

        private void UpdateManagerStatus(string managerStatus)
        {
            UpdateXmlNode(mXmlStatusDocument, "//MgrStatus", managerStatus);
            UpdateXmlNode(mXmlStatusDocument, "//Status", managerStatus);
            UpdateXmlNode(mXmlStatusDocument, "//LastUpdate", DateTime.Now.ToString(CultureInfo.InvariantCulture));
        }

        private void UpdateXmlNode(XmlNode statusDocument, string nodeXPath, string newValue)
        {
            var selectedNode = statusDocument.SelectSingleNode(nodeXPath);
            if (selectedNode != null)
                selectedNode.InnerText = newValue;
        }

        /// <summary>
        /// Write the status message XML to a text file
        /// Deletes old text files so that only 30 files are present in the RecentStatusMsgs directory
        /// </summary>
        /// <param name="statusMessages"></param>
        private void WriteStatusXmlToDisk(StringBuilder statusMessages)
        {
            const string STATUS_MSG_FILE_PREFIX = "StatusMsgs_";

            try
            {
                var appDirPath = PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppDirectoryPath();
                var recentStatusMsgDir = new DirectoryInfo(Path.Combine(appDirPath, "RecentStatusMsgs"));

                if (!recentStatusMsgDir.Exists)
                    recentStatusMsgDir.Create();

                // Example filename: StatusMsgs_2018-04-19_12.34.26.xml
                var statusMsgFileName = string.Format("{0}{1:yyyy-MM-dd_hh.mm.ss}.xml", STATUS_MSG_FILE_PREFIX, DateTime.Now);
                var statusMsgFilePath = Path.Combine(recentStatusMsgDir.FullName, statusMsgFileName);

                var doc = XDocument.Parse("<StatusMessages>" + statusMessages + "</StatusMessages>");
                using (var writer = new StreamWriter(new FileStream(statusMsgFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    writer.WriteLine(doc.ToString());
                }

                // Look for old files to delete
                var statusMsgFiles = recentStatusMsgDir.GetFileSystemInfos(STATUS_MSG_FILE_PREFIX + "*.xml").ToList();
                var numToDelete = statusMsgFiles.Count - RECENT_STATUS_FILES_TO_KEEP;

                if (numToDelete < 1)
                    return;

                foreach (var fileToDelete in (from item in statusMsgFiles orderby item.LastWriteTimeUtc select item).Take(numToDelete))
                {
                    fileToDelete.Delete();
                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error writing status XML to disk", ex);
            }

        }

        #endregion
    }
}
