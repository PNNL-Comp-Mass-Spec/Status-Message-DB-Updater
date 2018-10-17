using PRISM.Logging;
using System;
using PRISM;
using PRISM.FileProcessor;

namespace StatusMessageDBUpdater
{

    public class Program
    {

        const int MAX_RUNTIME_HOURS = 24;

        static FileLogger mLogger;

        /// <summary>
        /// Entry method
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            try
            {
                mLogger = new FileLogger(@"Logs\StatusMsgDBUpdater", BaseLogger.LogLevels.INFO);

                var appVersion = ProcessFilesOrDirectoriesBase.GetEntryOrExecutingAssembly().GetName().Version.ToString();
                mLogger.Info("=== Started StatusMessageDBUpdater V" + appVersion + " =====");

                var restart = true;

                do
                {
                    // Start the main program running
                    try
                    {
                        var mainProcess = new MainProgram();
                        mainProcess.DebugEvent += MainProcess_DebugEvent;
                        mainProcess.ErrorEvent += MainProcess_ErrorEvent;
                        mainProcess.WarningEvent += MainProcess_WarningEvent;
                        mainProcess.StatusEvent += MainProcess_StatusEvent;

                        if (!mainProcess.InitMgr(MAX_RUNTIME_HOURS))
                        {
                            ProgRunner.SleepMilliseconds(1500);
                            return;
                        }

                        // Start the main process
                        // If it receives the ReadConfig command, DoProcess will return true
                        restart = mainProcess.DoProcess();
                    }
                    catch (Exception ex2)
                    {
                        ShowErrorMessage("Error running the main process", ex2);
                        ProgRunner.SleepMilliseconds(1500);
                    }
                } while (restart);

                FileLogger.FlushPendingMessages();

            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error starting application", ex);
            }

            ProgRunner.SleepMilliseconds(1500);

        }

        private static void MainProcess_DebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebug(message);
            mLogger?.Debug(message);
        }

        private static void MainProcess_ErrorEvent(string message, Exception ex)
        {
            ConsoleMsgUtils.ShowError(message, ex, false);
            mLogger?.Error(message, ex);
        }

        private static void MainProcess_StatusEvent(string message)
        {
            Console.WriteLine(message);
            mLogger?.Info(message);
        }

        private static void MainProcess_WarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
            mLogger?.Warn(message);
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
            mLogger?.Error(message, ex);
        }

    }
}
