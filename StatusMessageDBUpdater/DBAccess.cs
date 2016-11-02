using System;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace StatusMessageDBUpdater {
    class DBAccess {
        readonly string m_cnStr;
        SqlConnection m_dbCn;

        // ctor
        public DBAccess(string connectionString) {
            m_cnStr = connectionString;
            m_dbCn = new SqlConnection(m_cnStr);
        }

        // open connection
        public bool Connect() {
            var outcome = false;
            try {
                m_dbCn = new SqlConnection(m_cnStr);
                m_dbCn.Open();
                outcome = true;
            }
            catch (Exception ex) {
                // Connection error
                Console.WriteLine(ex.Message);
            }
            return outcome;
        }

        // post message to database
        public bool UpdateDatabase(StringBuilder statusMessages, out string result)
        {
            const int DB_TIMEOUT_SECONDS = 90;

            try {
                // create the command object
                //
                var sc = new SqlCommand("UpdateManagerAndTaskStatusXML", m_dbCn)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = DB_TIMEOUT_SECONDS
                };


                //
                // define parameter for stored procedure's return value
                //
                //
                // define parameter for stored procedure's return value
                //
                var myParm = sc.Parameters.Add("@Return", SqlDbType.Int);
                myParm.Direction = ParameterDirection.ReturnValue;
                //
                // define parameters for the stored procedure's arguments
                //
                myParm = sc.Parameters.Add("@parameters", SqlDbType.Text);
                myParm.Direction = ParameterDirection.Input;
                myParm.Value = statusMessages.ToString();

                myParm = sc.Parameters.Add("@result", SqlDbType.VarChar, 4096);
                myParm.Direction = ParameterDirection.Output;


                // execute the stored procedure
                //
                sc.ExecuteNonQuery();

                // get return value
                //
                var ret = (int)sc.Parameters["@Return"].Value;

                // get values for output parameters
                //
                result = (string)sc.Parameters["@result"].Value;

                // if we made it this far, we succeeded
                //
                if (ret == 0)
                    return true;;
            }
            catch (Exception ex) {
                Console.WriteLine(ex.Message);
                result = ex.Message;
            }
            
            return false;
        }

        public void Disconnect() {
            if (m_dbCn != null) {
                m_dbCn.Close();
                m_dbCn.Dispose();
            }
        }

        // clean up
        public void Dispose() {
            Disconnect();
        }
    }
}
