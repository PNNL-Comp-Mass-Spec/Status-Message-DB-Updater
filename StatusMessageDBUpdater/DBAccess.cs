using System;
using System.Data;
using System.Text;
using PRISMDatabaseUtils;

namespace StatusMessageDBUpdater
{
    internal class DBAccess
    {
        private readonly IDBTools mDBTools;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString"></param>
        public DBAccess(string connectionString)
        {
            const int DB_TIMEOUT_SECONDS = 90;
            mDBTools = DbToolsFactory.GetDBTools(connectionString, DB_TIMEOUT_SECONDS);
        }

        /// <summary>
        /// Post message to database using stored procedure update_manager_and_task_status_xml or update_capture_task_manager_and_task_status_xml
        /// </summary>
        /// <remarks>
        /// The stored procedure name is defined in file StatusMessageDBUpdater.exe.local.config
        /// </remarks>
        /// <param name="statusMessages"></param>
        /// <param name="procedureName">Procedure to call</param>
        /// <param name="result"></param>
        /// <returns>True if success, false if an error</returns>
        public bool UpdateDatabase(StringBuilder statusMessages, string procedureName, out string result)
        {
            try
            {
                var cmd = mDBTools.CreateCommand(procedureName, CommandType.StoredProcedure);

                // Define parameter for procedure's return value
                // If querying a Postgres DB, mDBTools will auto-change "@return" to "_returnCode"
                var returnParam = mDBTools.AddParameter(cmd, "@return", SqlType.Int, ParameterDirection.ReturnValue);

                mDBTools.AddParameter(cmd, "@managerStatusXML", SqlType.Text).Value = statusMessages.ToString();
                mDBTools.AddParameter(cmd, "@infoLevel", SqlType.TinyInt).Value = 0;

                var messageParam = mDBTools.AddParameter(cmd, "@message", SqlType.VarChar, 4096, ParameterDirection.InputOutput);

                // Execute the stored procedure
                mDBTools.ExecuteSP(cmd);

                // The return code is an integer on SQL Server, but is text on Postgres
                // Use GetReturnCode to obtain the integer, or find the first integer in the text-based return code
                var returnValue = DBToolsBase.GetReturnCode(returnParam);

                // Get output parameter value
                result = mDBTools.GetString(messageParam.Value);

                if (returnValue == 0)
                {
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                result = ex.Message;
            }

            return false;
        }
    }
}
