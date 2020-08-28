using System;
using System.Text;
using System.Data;
using PRISMDatabaseUtils;

namespace StatusMessageDBUpdater
{
    class DBAccess
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
        /// Post message to database using stored procedure UpdateManagerAndTaskStatusXML
        /// </summary>
        /// <param name="statusMessages"></param>
        /// <param name="result"></param>
        /// <returns>True if success, false if an error</returns>
        public bool UpdateDatabase(StringBuilder statusMessages, out string result)
        {
            try
            {
                var cmd = mDBTools.CreateCommand("UpdateManagerAndTaskStatusXML", CommandType.StoredProcedure);

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
