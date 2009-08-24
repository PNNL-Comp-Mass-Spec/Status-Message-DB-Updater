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

namespace StatusMessageDBUpdater {
    class clsMainProg {
        private static readonly ILog mainLog = LogManager.GetLogger("MainLog");

        #region "Enums"
        private enum BroadcastCmdType {
            Shutdown,
            ReadConfig,
            Invalid
        }
        #endregion

        #region "Class variables"
        string mgrName = null;
        private DBAccess dba = null;
        private int dbUpdateIntervalMS;
        private bool run = true;
        bool restart = false;

        private clsMessageHandler messageHandler = null;
        private MessageAccumulator m_ma = null;
        #endregion

        #region "Methods"

        /// <summary>
        /// Initializes the manager
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        public bool InitMgr() {
            //Get the manager settings
            clsMgrSettings mgrSettings = null;
            try {
                mgrSettings = new clsMgrSettings();
                mainLog.Info("Read manager settings from Manager Control Database");
            }
            catch {
                return false; //Failures are logged by clsMgrSettings to local emergency log file
            }

            // processor name
            this.mgrName = mgrSettings.GetParam("MgrName");
            mainLog.Info("Manager:" + this.mgrName);

            //---- initialize the connection parameter fields ----
            string messageBrokerURL = mgrSettings.GetParam("MessageQueueURI");
            string messageTopicName = mgrSettings.GetParam("StatusMsgIncomingTopic");
            string monitorTopicName = mgrSettings.GetParam("MessageQueueTopicMgrStatus"); // topic to send
            string brodcastTopicName = mgrSettings.GetParam("BroadcastQueueTopic");
            mgrSettings.GetParam("LogStatusToMessageQueue");

            this.messageHandler = new clsMessageHandler();
            this.messageHandler.MgrName = mgrName;
            this.messageHandler.BrokerUri = messageBrokerURL;
            this.messageHandler.InputStatusTopicName = messageTopicName;
            this.messageHandler.OutputStatusTopicName = monitorTopicName;
            this.messageHandler.BroadcastTopicName = brodcastTopicName;
            this.messageHandler.Init();

            // make a new processor message accumulator and start it running
            this.m_ma = new MessageAccumulator();

            this.messageHandler.InputMessageReceived += this.m_ma.subscriber_OnMessageReceived;
            this.messageHandler.BroadcastReceived += this.OnMsgHandler_BroadcastReceived;

            //---- seconds between database updates ----
            string interval = mgrSettings.GetParam("StatusMsgDBUpdateInterval");
            this.dbUpdateIntervalMS = 1000 * int.Parse(interval);

            // create a new database access object
            string dbConnStr = mgrSettings.GetParam("connectionstring");
            this.dba = new DBAccess(dbConnStr);

            //---- Connect message handler events ----

            //---- Everything worked ----
            return true;
        }

        public bool DoProcess() {
            mainLog.Info("Process started");
            while (this.run) {
                // wait a minute
                Thread.Sleep(this.dbUpdateIntervalMS);
                if (!run) {
                    break;
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

                try {
                    dba.Connect();

                    // build concatenated XML for all new status messages
                    string concatMessages = "";
                    foreach (string Processor in Processors) {
                        XmlDocument doc = new XmlDocument();
                        doc.LoadXml(m_ma.statusList[Processor]);
                        XmlNode n = doc.SelectSingleNode("//Root");
                        if (n != null) {
                            concatMessages += n.OuterXml;
                        }
                    }
                    progMsg = "Size:" + concatMessages.Length.ToString();
                    mainLog.Info(progMsg);

                    // update the database
                    string message = "";
                    bool err = dba.UpdateDatabase(concatMessages, ref message);
                    //                    if(message != "") progMsg += "msg:" + message + Environment.NewLine;
                    //                    if (monitorTopicName != "") m_ts.SendMessage(monitorTopicName, progMsg);
                    mainLog.Info("Result:" + message);
                }
                catch (Exception e) {
                    mainLog.Error(e.Message);
                }
                dba.Disconnect();
            }
            mainLog.Info("Process interrupted, " + "Restart:" + this.restart.ToString());
            this.messageHandler.Dispose();
            return this.restart;
        }

        /*
        m_MgrSettings.GetParam("machname");
        m_MgrSettings.GetParam("debuglevel");
        m_MgrSettings.GetParam("mgractive");
        m_MgrSettings.GetParam("statusfilelocation");
        m_MgrSettings.GetParam("configfilename");
        m_MgrSettings.GetParam("localmgrpath");
        m_MgrSettings.GetParam("remotemgrpath");
        m_MgrSettings.GetParam("programfoldername");
        m_MgrSettings.GetParam("modulename");
        m_MgrSettings.GetParam("showinmgrctrl");
        m_MgrSettings.GetParam("showinanmonitor");
        m_MgrSettings.GetParam("logfilename");
        m_MgrSettings.GetParam("cmdtimeout");
        m_MgrSettings.GetParam("LogStatusToMessageQueue");
        m_MgrSettings.GetParam("BroadcastQueueTopic");
        */

        /// <summary>
        /// Handles broacast messages for control of the manager
        /// </summary>
        /// <param name="cmdText">Text of received message</param>
        void OnMsgHandler_BroadcastReceived(string cmdText) {
            mainLog.Info("clsMainProgram.OnMsgHandler_BroadcastReceived: Broadcast message received: " + cmdText);

            // parse command XML and get command text and
            // list of machines that command applies to
            List<string> MachineList = new List<string>();
            string MachCmd;
            try {
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(cmdText);
                foreach (XmlNode xn in doc.SelectNodes("//Managers/*")) {
                    MachineList.Add(xn.InnerText);
                }
                MachCmd = doc.SelectSingleNode("//Message").InnerText;
            }
            catch (Exception Ex) {
                mainLog.Error("Exception while parsing broadcast string:" + Ex.Message);
                return;
            }

            // Determine if the message applies to this machine
            if (!MachineList.Contains(this.mgrName)) {
                // Received command doesn't apply to this manager
                mainLog.Debug("Received command not applicable to this manager instance");
                return;
            }

            // Get the command and take appropriate action
            switch (MachCmd.ToLower()) {
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
        #endregion
    } // end class
} // end namespace
