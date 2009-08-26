using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;
using log4net;
using log4net.Config;

[assembly: log4net.Config.XmlConfigurator(ConfigFile = "Logging.config", Watch = true)]

namespace StatusMessageDBUpdater {
    class Program {
        private static readonly ILog mainLog = LogManager.GetLogger("MainLog");

        #region "Class variables"
        static clsMainProg m_MainProcess = null;
        static string ErrMsg;
        #endregion

        #region "Methods"

        static void Main(string[] args) {
            // setup the logger
//            XmlConfigurator.Configure(new System.IO.FileInfo("Logging.config"));
            mainLog.Info("Started");

            bool restart = false;
            do {
                // Start the main program running
                try {
                    if (m_MainProcess == null) {
                        m_MainProcess = new clsMainProg();
                        if (!m_MainProcess.InitMgr()) {
                            return;
                        }
                        restart = m_MainProcess.DoProcess();
                        m_MainProcess = null;
                    }
                }
                catch (Exception Err) {
                    // Report any exceptions not handled at a lover leve to the system application log
                    ErrMsg = "Critical exception starting application: " + Err.Message;
//                    System.Diagnostics.EventLog ev = new System.Diagnostics.EventLog("Application", ".", "DMS_StatusMsgDBUpdater");
//                    System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.EventLogTraceListener("DMS_StatusMsgDBUpdater"));
//                    System.Diagnostics.Trace.WriteLine(ErrMsg);
//                    ev.Close();
                    System.Diagnostics.Debug.WriteLine(ErrMsg);
                }
            } while(restart);

        }

        #endregion
    }
}
