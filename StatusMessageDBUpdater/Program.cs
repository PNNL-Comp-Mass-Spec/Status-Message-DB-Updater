using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;

namespace StatusMessageDBUpdater {
    class Program {

        #region "Class variables"
        static clsMainProg m_MainProcess = null;
        static string ErrMsg;
        #endregion

        #region "Methods"

        static void Main(string[] args) {
            // Start the main program running
            try {
                if (m_MainProcess == null) {
                    m_MainProcess = new clsMainProg();
                    if (!m_MainProcess.InitMgr()) {
                        return;
                    }
                    m_MainProcess.DoProcess();
                }
            }
            catch (Exception Err) {
                // Report any exceptions not handled at a lover leve to the system application log
                ErrMsg = "Critical exception starting application: " + Err.Message;
                System.Diagnostics.EventLog ev = new System.Diagnostics.EventLog("Application", ".", "DMS_StatusMsgDBUpdater");
                System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.EventLogTraceListener("DMS_StatusMsgDBUpdater"));
                System.Diagnostics.Trace.WriteLine(ErrMsg);
                ev.Close();
            }

        }

        #endregion
    }
}
