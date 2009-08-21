using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;

namespace StatusMessageDBUpdater {
    class Program {
        static void Main(string[] args) {
            // seconds between database updates
            int dbUpdateIntervalMS = 1000 * Properties.Settings.Default.DBUpdateInterval;

            // initialize the connection parameter fields
            string messageBrokerURL = Properties.Settings.Default.BrokerURL;
            string messageTopicName = Properties.Settings.Default.TopicName;

            // get a unique name for the message client
            DateTime tn = DateTime.Now; // Alternative: System.Guid.NewGuid().ToString();
            string clientID = System.Net.Dns.GetHostEntry("localhost").HostName + '_' + tn.Ticks.ToString();

            // get topic to send
            string monitorTopicName = Properties.Settings.Default.MonitorTopicName;

            // make a new processor message accumulator and start it running
            MessageAccumulator m_ma = new MessageAccumulator();

            // object that connects to the message broker and gets messages from it
            string cid = clientID;
            SimpleTopicSubscriber m_ts = new SimpleTopicSubscriber(messageTopicName, messageBrokerURL, ref cid);
            m_ts.OnMessageReceived += m_ma.subscriber_OnMessageReceived;

            // create a new database access object
            string dbConnStr = Properties.Settings.Default.DBConnectionStr;
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
    }
}
