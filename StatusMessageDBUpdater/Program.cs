using PRISM.Logging;
using System;
using PRISM;
using PRISM.FileProcessor;

namespace StatusMessageDBUpdater
{
    public static class Program
    {
        private static FileLogger mLogger;

        /// <summary>
        /// Entry method
        /// </summary>
        /// <param name="args"></param>
        private static void Main(string[] args)
        {
            try
            {
                mLogger = new FileLogger(@"Logs\StatusMsgDBUpdater", BaseLogger.LogLevels.INFO);

                var appVersion = ProcessFilesOrDirectoriesBase.GetEntryOrExecutingAssembly().GetName().Version.ToString();
                mLogger.Info("=== Started StatusMessageDBUpdater V" + appVersion + " =====");

                var restart = true;
                var runFailureCount = 0;
                var startTime = DateTime.UtcNow;

                while (restart)
                {
                    // Start the main program running
                    try
                    {
                        var mainProcess = new MainProgram();
                        mainProcess.DebugEvent += MainProcess_DebugEvent;
                        mainProcess.ErrorEvent += MainProcess_ErrorEvent;
                        mainProcess.WarningEvent += MainProcess_WarningEvent;
                        mainProcess.StatusEvent += MainProcess_StatusEvent;

                        if (!mainProcess.InitMgr(startTime))
                        {
                            ProgRunner.SleepMilliseconds(1500);
                            return;
                        }

                        // Start the main process
                        // If it receives the ReadConfig command, DoProcess will return true
                        restart = mainProcess.DoProcess();
                        runFailureCount = 0;
                    }
                    catch (Exception ex2)
                    {
                        ShowErrorMessage("Error running the main process", ex2);
                        runFailureCount++;
                        var sleepSeconds = 1.5 * runFailureCount;
                        if (sleepSeconds > 30)
                        {
                            sleepSeconds = 30;
                        }
                        ProgRunner.SleepMilliseconds((int)(sleepSeconds * 1000));
                    }
                }

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
            ConsoleMsgUtils.ShowErrorCustom(message, ex, false);
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
