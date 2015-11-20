using System;
using log4net;

[assembly: log4net.Config.XmlConfigurator(ConfigFile = "Logging.config", Watch = true)]

namespace StatusMessageDBUpdater {
    class Program {
        private static readonly ILog mainLog = LogManager.GetLogger("MainLog");

        #region "Class variables"
		const int MAX_RUNTIME_HOURS = 24;

        static clsMainProg m_MainProcess;
        static string ErrMsg;
        #endregion

        #region "Methods"

        static void Main(string[] args) {

            mainLog.Info("Started");

            var restart = false;
			var dtStartTime = DateTime.UtcNow;

            do {
                // Start the main program running
                try {
                    if (m_MainProcess == null) {
                        m_MainProcess = new clsMainProg();
						if (!m_MainProcess.InitMgr(MAX_RUNTIME_HOURS))
						{
                            return;
                        }
                        restart = m_MainProcess.DoProcess();
                        m_MainProcess = null;
                    }
                }
                catch (Exception Err) {
                    // Report any exceptions not handled at a lower level to the system application log
                    ErrMsg = "Critical exception starting application: " + Err.Message;
//                    System.Diagnostics.EventLog ev = new System.Diagnostics.EventLog("Application", ".", "DMS_StatusMsgDBUpdater");
//                    System.Diagnostics.Trace.Listeners.Add(new System.Diagnostics.EventLogTraceListener("DMS_StatusMsgDBUpdater"));
//                    System.Diagnostics.Trace.WriteLine(ErrMsg);
//                    ev.Close();
                    System.Diagnostics.Debug.WriteLine(ErrMsg);
                }

				if (DateTime.UtcNow.Subtract(dtStartTime).TotalHours >= MAX_RUNTIME_HOURS)
				{
					var message = "Over " + MAX_RUNTIME_HOURS.ToString() + " hours have elapsed; exiting program (helps mitigate a memory leak)";
					System.Diagnostics.Debug.WriteLine(message);
					Console.WriteLine(message);
					break;
				}

            } while(restart);

        }

        #endregion
    }
}
