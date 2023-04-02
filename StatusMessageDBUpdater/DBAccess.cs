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
        /// <param name="statusMessages"></param>
        /// <param name="procedureName">Procedure to call</param>
        /// <param name="result"></param>
        /// <returns>True if success, false if an error</returns>
        public bool UpdateDatabase(StringBuilder statusMessages, string procedureName, out string result)
        {
            try
            {
                var cmd = mDBTools.CreateCommand(procedureName, CommandType.StoredProcedure);

                mDBTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                mDBTools.AddParameter(cmd, "@parameters", SqlType.Text).Value = statusMessages.ToString();
                var resultParam = mDBTools.AddParameter(cmd, "@result", SqlType.VarChar, 4096, ParameterDirection.Output);

                var returnCode = mDBTools.ExecuteSP(cmd);

                // Get values for output parameters
                result = mDBTools.GetString(resultParam.Value);

                if (returnCode == 0)
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
