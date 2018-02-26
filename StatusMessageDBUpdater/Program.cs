using PRISM.Logging;
using System;
using PRISM;

namespace StatusMessageDBUpdater
{

    public class Program
    {

        const int MAX_RUNTIME_HOURS = 24;

        static FileLogger m_Logger;

        /// <summary>
        /// Entry method
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            try
            {
                m_Logger = new FileLogger(@"Logs\StatusMsgDBUpdater", BaseLogger.LogLevels.INFO);

                var restart = true;

                do
                {
                    // Start the main program running
                    try
                    {
                        var mainProcess = new clsMainProg();
                        mainProcess.DebugEvent += MainProcess_DebugEvent;
                        mainProcess.ErrorEvent += MainProcess_ErrorEvent;
                        mainProcess.WarningEvent += MainProcess_WarningEvent;
                        mainProcess.StatusEvent += MainProcess_StatusEvent;

                        if (!mainProcess.InitMgr(MAX_RUNTIME_HOURS))
                        {
                            clsProgRunner.SleepMilliseconds(1500);
                            return;
                        }

                        // Start the main process
                        // If it receives the ReadConfig command, DoProcess will return true
                        restart = mainProcess.DoProcess();
                    }
                    catch (Exception ex2)
                    {
                        ShowErrorMessage("Error running the main process", ex2);
                        clsProgRunner.SleepMilliseconds(1500);
                    }
                } while (restart);

                FileLogger.FlushPendingMessages();

            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error starting application", ex);
            }

            clsProgRunner.SleepMilliseconds(1500);

        }

        private static void MainProcess_DebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebug(message);
            m_Logger?.Debug(message);
        }

        private static void MainProcess_ErrorEvent(string message, Exception ex)
        {
            ConsoleMsgUtils.ShowError(message, ex, false);
            m_Logger?.Error(message, ex);
        }

        private static void MainProcess_StatusEvent(string message)
        {
            Console.WriteLine(message);
            m_Logger?.Info(message);
        }

        private static void MainProcess_WarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
            m_Logger?.Warn(message);
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
            m_Logger?.Error(message, ex);
        }

    }
}
