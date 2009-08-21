using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace StatusMessageDBUpdater {
    class DBAccess {
        string m_cnStr;
        SqlConnection m_dbCn;

        // ctor
        public DBAccess(string connectionString) {
            m_cnStr = connectionString;
            m_dbCn = new SqlConnection(m_cnStr);
        }

        // open connection
        public bool Connect() {
            bool outcome = false;
            try {
                m_dbCn = new SqlConnection(m_cnStr);
                m_dbCn.Open();
                outcome = true;
            }
            catch (Exception) {
            }
            return outcome;
        }

        // post message to database
        public bool UpdateDatabase(string statusMessages, ref string result) {
            SqlCommand sc;
            bool Outcome = false;

            try {
                // create the command object
                //
                sc = new SqlCommand("UpdateManagerAndTaskStatusXML", m_dbCn);
                sc.CommandType = CommandType.StoredProcedure;

                // define parameters for command object
                //
                SqlParameter myParm;
                //
                // define parameter for stored procedure's return value
                //
                //
                // define parameter for stored procedure's return value
                //
                myParm = sc.Parameters.Add("@Return", SqlDbType.Int);
                myParm.Direction = ParameterDirection.ReturnValue;
                //
                // define parameters for the stored procedure's arguments
                //
                myParm = sc.Parameters.Add("@parameters", SqlDbType.Text);
                myParm.Direction = ParameterDirection.Input;
                myParm.Value = statusMessages;

                myParm = sc.Parameters.Add("@result", SqlDbType.VarChar, 4096);
                myParm.Direction = ParameterDirection.Output;


                // execute the stored procedure
                //
                sc.ExecuteNonQuery();

                // get return value
                //
                int ret = -1;
                ret = (int)sc.Parameters["@Return"].Value;

                // get values for output parameters
                //
                result = (string)sc.Parameters["@result"].Value;

                // if we made it this far, we succeeded
                //
                Outcome = (ret == 0);
            }
            catch (Exception e) {
                Console.WriteLine(e.Message);
                result = e.Message;
            }
            finally {
            }
            return Outcome;
        }

        public void Disconnect() {
            if (m_dbCn != null) {
                m_dbCn.Close();
                m_dbCn.Dispose();
            }
        }

        // clean up
        public void Dispose() {
            this.Disconnect();
        }
    }
}
