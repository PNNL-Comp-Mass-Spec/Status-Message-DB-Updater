using System;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace StatusMessageDBUpdater
{
    class DBAccess
    {
        readonly string m_cnStr;

        SqlConnection m_dbCn;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString"></param>
        public DBAccess(string connectionString)
        {
            m_cnStr = connectionString;
            m_dbCn = new SqlConnection(m_cnStr);
        }

        /// <summary>
        /// Open the database connection
        /// </summary>
        /// <returns></returns>
        public bool Connect()
        {
            var outcome = false;
            try
            {
                m_dbCn = new SqlConnection(m_cnStr);
                m_dbCn.Open();
                outcome = true;
            }
            catch (Exception ex)
            {
                // Connection error
                Console.WriteLine(ex.Message);
            }
            return outcome;
        }

        /// <summary>
        /// Post message to database using stored procedure UpdateManagerAndTaskStatusXML
        /// </summary>
        /// <param name="statusMessages"></param>
        /// <param name="result"></param>
        /// <returns>True if success, false if an error</returns>
        public bool UpdateDatabase(StringBuilder statusMessages, out string result)
        {
            const int DB_TIMEOUT_SECONDS = 90;

            try
            {
                var cmd = new SqlCommand("UpdateManagerAndTaskStatusXML", m_dbCn)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = DB_TIMEOUT_SECONDS
                };

                cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                cmd.Parameters.Add(new SqlParameter("@parameters", SqlDbType.Text)).Value = statusMessages.ToString();
                cmd.Parameters.Add(new SqlParameter("@result", SqlDbType.VarChar, 4096)).Direction = ParameterDirection.Output;

                cmd.ExecuteNonQuery();

                // Get return value
                var ret = (int)cmd.Parameters["@Return"].Value;

                // Get values for output parameters
                result = (string)cmd.Parameters["@result"].Value;

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

        public void Disconnect()
        {
            if (m_dbCn != null)
            {
                m_dbCn.Close();
                m_dbCn.Dispose();
            }
        }

        /// <summary>
        /// Disconnect from the database
        /// </summary>
        public void Dispose()
        {
            Disconnect();
        }
    }
}
