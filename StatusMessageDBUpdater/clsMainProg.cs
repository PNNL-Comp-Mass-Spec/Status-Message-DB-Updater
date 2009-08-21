using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Windows.Forms;
using System.Xml;
using System.Collections.Specialized;

namespace StatusMessageDBUpdater {
    class clsMainProg {

        #region "Enums"
        private enum BroadcastCmdType {
            Shutdown,
            ReadConfig,
            Invalid
        }
        #endregion

        #region "Class variables"
        private clsMgrSettings m_MgrSettings;
//        private IStatusFile m_StatusFile;
//        private clsMessageHandler m_MsgHandler;
//        private bool m_Running = false;
//        private bool m_MgrActive = false;
//        private BroadcastCmdType m_BroadcastCmdType;
        #endregion

        #region "Methods"

        /// <summary>
        /// Initializes the manager
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        public bool InitMgr() {
            //Get the manager settings
            try {
                m_MgrSettings = new clsMgrSettings();
            }
            catch {
                //Failures are logged by clsMgrSettings to local emergency log file
                return false;
            }

            //Setup the logger

            //Make the initial log entry

            //Setup the message queue

            //Connect message handler events

            //Everything worked
            return true;
        }



        public void DoProcess() {
            // seconds between database updates
            string interval = m_MgrSettings.GetParam("StatusMsgDBUpdateInterval");
            int dbUpdateIntervalMS = 1000 * int.Parse(interval); 

            // initialize the connection parameter fields
            string messageBrokerURL = m_MgrSettings.GetParam("MessageQueueURI");
            string messageTopicName = m_MgrSettings.GetParam("StatusMsgIncomingTopic");

            // get a unique name for the message client
            DateTime tn = DateTime.Now; // Alternative: System.Guid.NewGuid().ToString();
            string clientID = System.Net.Dns.GetHostEntry("localhost").HostName + '_' + tn.Ticks.ToString();

            // get topic to send
            string monitorTopicName = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus");

            // make a new processor message accumulator and start it running
            MessageAccumulator m_ma = new MessageAccumulator();

            // object that connects to the message broker and gets messages from it
            string cid = clientID;
            SimpleTopicSubscriber m_ts = new SimpleTopicSubscriber(messageTopicName, messageBrokerURL, ref cid);
            m_ts.OnMessageReceived += m_ma.subscriber_OnMessageReceived;

            // create a new database access object
            string dbConnStr = m_MgrSettings.GetParam("connectionstring");
            DBAccess dba = new DBAccess(dbConnStr);

            while (true) {
                // wait a minute
                Thread.Sleep(dbUpdateIntervalMS);

                // from the message accumulator, get list of processors 
                // that have received messages since the last refresh and
                // reset the list in the accumulator
                string[] Processors = m_ma.changedList.Keys.ToArray();
                m_ma.changedList.Clear();
                int msgCount = m_ma.msgCount;
                m_ma.msgCount = 0;

                string progMsg = "MsgDB program updated " + Processors.Length.ToString() + " at " + DateTime.Now.ToString() + Environment.NewLine;
                System.Diagnostics.Debug.WriteLine("----");
                System.Diagnostics.Debug.Write(progMsg);

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
                    System.Diagnostics.Debug.WriteLine("Size:" + concatMessages.Length.ToString());

                    // update the database
                    string message = "";
                    bool err = dba.UpdateDatabase(concatMessages, ref message);
                    //                    if(message != "") progMsg += "msg:" + message + Environment.NewLine;
                    //                    if (monitorTopicName != "") m_ts.SendMessage(monitorTopicName, progMsg);
                    System.Diagnostics.Debug.WriteLine(message);
                }
                catch (Exception e) {
                    System.Diagnostics.Debug.WriteLine(e.Message);
                }
                dba.Disconnect();
            }
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
        #endregion
    } // end class
} // end namespace
