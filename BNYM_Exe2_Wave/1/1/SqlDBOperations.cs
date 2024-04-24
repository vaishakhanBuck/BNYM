using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Text;


namespace BNYM_Process2
{
    class SqlDBOperations
    {
        public static string GetConnection()
        {
            // string conString = ConfigurationManager.ConnectionStrings["SiteSqlServer"].ConnectionString;
            //return conString;                      
            string conString = string.Empty;
            switch (Program.schedEnvironment.ToLower())
            {
                case "i":
                    conString = ConfigurationManager.ConnectionStrings["sqlCS_IST"].ConnectionString;
                    break;
                case "u":
                    conString = ConfigurationManager.ConnectionStrings["sqlCS_UAT"].ConnectionString;
                    break;
                case "p":
                    conString = ConfigurationManager.ConnectionStrings["SqlCS_PROD"].ConnectionString;
                    break;
                default:
                    conString = ConfigurationManager.ConnectionStrings["sqlCS_IST"].ConnectionString;
                    break;
            }
            return conString;

        }


        public DataTable GetSqlDatatable(string sqlStr)
        {

            DataTable dt = new DataTable();
            using (SqlConnection cn = new SqlConnection(GetConnection()))
            {
                SqlDataAdapter da = new SqlDataAdapter();
                SqlCommand cmd = new SqlCommand(sqlStr, cn);
                da.SelectCommand = cmd;
                da.Fill(dt);
            }
            return dt;
        }
        public DataTable GetSqlDatatable(string sqlStr, Hashtable paramList)
        {

            DataTable dt = new DataTable();
            using (SqlConnection cn = new SqlConnection(GetConnection()))
            {
                SqlDataAdapter da = new SqlDataAdapter();
                SqlCommand cmd = new SqlCommand(sqlStr, cn);
                cn.Open();
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = sqlStr;
                cmd.Connection = cn;
                da.SelectCommand = cmd;
                foreach (DictionaryEntry entry in paramList)
                {
                    cmd.Parameters.AddWithValue(entry.Key.ToString(), entry.Value);
                }

                da.Fill(dt);
            }
            return dt;

        }

        public void ExecuteQueryforSql(string sqlStr)
        {
            using (SqlConnection cn = new SqlConnection(GetConnection()))
            {
                if (cn.State != ConnectionState.Open)
                    cn.Open();

                SqlCommand cmd = new SqlCommand(sqlStr, cn);
                cmd.ExecuteNonQuery();

                if (cn.State != ConnectionState.Closed)
                    cn.Close();
            }
        }

        public void ExecuteQueryforSql(string sqlStr, Hashtable paramList)
        {
            using (SqlConnection cn = new SqlConnection(GetConnection()))
            {
                SqlCommand cmd = new SqlCommand();
                if (cn.State != ConnectionState.Open)
                    cn.Open();
                cmd.CommandText = sqlStr;
                cmd.Connection = cn;
                foreach (DictionaryEntry entry in paramList)
                {
                    cmd.Parameters.AddWithValue(entry.Key.ToString(), entry.Value);
                }
                cmd.ExecuteNonQuery();
                if (cn.State != ConnectionState.Closed)
                    cn.Close();
            }
        }

        public string GetSqlResult(string sqlStr)
        {
            string retVal = string.Empty;
            SqlCommand cmd = new SqlCommand();
            SqlDataAdapter da = new SqlDataAdapter();
            DataSet ds;
            using (SqlConnection cn = new SqlConnection(GetConnection()))
            {
                cn.Open();
                cmd.CommandText = sqlStr;
                cmd.Connection = cn;
                da.SelectCommand = cmd;
                ds = new DataSet();
                da.Fill(ds);
                cn.Close();
            }
            if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
            {
                retVal = ds.Tables[0].Rows[0][0].ToString();
            }
            return retVal;
        }
        public string GetSqlResult(string sqlStr, Hashtable paramList)
        {
            string retVal = string.Empty;
            SqlCommand cmd = new SqlCommand();
            SqlDataAdapter da = new SqlDataAdapter();
            DataSet ds;
            using (SqlConnection cn = new SqlConnection(GetConnection()))
            {
                cn.Open();
                cmd.CommandText = sqlStr;
                cmd.Connection = cn;
                da.SelectCommand = cmd;
                foreach (DictionaryEntry entry in paramList)
                {
                    cmd.Parameters.AddWithValue(entry.Key.ToString(), entry.Value);
                }
                ds = new DataSet();
                da.Fill(ds);
            }
            if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
            {
                retVal = ds.Tables[0].Rows[0][0].ToString();
            }
            return retVal;
        }

        public void ExecuteQueryProc(string procName, Hashtable paramList)
        {

            using (SqlConnection conn = new SqlConnection(GetConnection()))
            {
                var cmd = new SqlCommand();
                cmd.Connection = conn;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = procName;
                foreach (DictionaryEntry entry in paramList)
                {
                    cmd.Parameters.AddWithValue(entry.Key.ToString(), entry.Value);
                }
                conn.Open();
                cmd.ExecuteNonQuery();
                conn.Close();
            };

        }

        public string ExecuteScalarProc(string procName, Hashtable paramList)
        {
            string retVal = string.Empty;
            using (SqlConnection conn = new SqlConnection(GetConnection()))
            {
                var cmd = new SqlCommand();
                cmd.Connection = conn;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = procName;
                foreach (DictionaryEntry entry in paramList)
                {
                    cmd.Parameters.AddWithValue(entry.Key.ToString(), entry.Value);
                }
                conn.Open();
                retVal = cmd.ExecuteScalar().ToString();
                conn.Close();
            };

            return retVal;

        }

    }
}
