using System;
using System.Text;
using System.Data;
using PRISMDatabaseUtils;

namespace StatusMessageDBUpdater
{
    class DBAccess
    {
        readonly string mConnectionString;

        private IDBTools mDBTools;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString"></param>
        public DBAccess(string connectionString)
        {
            const int DB_TIMEOUT_SECONDS = 90;
            mConnectionString = connectionString;
            mDBTools = DbToolsFactory.GetDBTools(mConnectionString, DB_TIMEOUT_SECONDS);
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

                var returnParam = mDBTools.AddParameter(cmd, "@Return", SqlType.Int, direction: ParameterDirection.ReturnValue);
                mDBTools.AddParameter(cmd, "@parameters", SqlType.Text, value: statusMessages.ToString());
                var resultParam = mDBTools.AddParameter(cmd, "@result", SqlType.VarChar, 4096, direction: ParameterDirection.Output);

                mDBTools.ExecuteSP(cmd);

                // Get return value
                var ret = (int)returnParam.Value;

                // Get values for output parameters
                result = (string)resultParam.Value;

                if (ret == 0)
                    return true;
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
