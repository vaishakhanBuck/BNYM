using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Data.OracleClient;


namespace  BNYM_Process2
{
    class OracleDBOperations
    {
        public static string GetConnection()
        {
            string conString = UtilityClass.PrepareConnectionString(ConfigurationManager.ConnectionStrings["oraConn"].ConnectionString);
            return conString;
        }

        public DataSet GetDSResult(string sqlStr)
        {
            DataSet ds = new DataSet();
            using (OracleConnection cn = new OracleConnection(GetConnection()))
            {
                OracleCommand cmd = new OracleCommand();
                OracleDataAdapter da = new OracleDataAdapter();

                cn.Open();
                cmd.CommandText = sqlStr;
                cmd.Connection = cn;
                da.SelectCommand = cmd;
              
                da.Fill(ds);
                cn.Close();
            }
            return ds;
        }
        public DataTable GetOracleDatatable(string sqlStr)
        {

            DataTable dt = new DataTable();
            using (OracleConnection cn = new OracleConnection(GetConnection()))
            {
                OracleDataAdapter da = new OracleDataAdapter();
                OracleCommand cmd = new OracleCommand(sqlStr, cn);

                da.SelectCommand = cmd;

                da.Fill(dt);
            }
            return dt;
        }
        public DataTable GetOracleDatatable(string sqlStr, Hashtable paramList)
        {

            DataTable dt = new DataTable();
            using (OracleConnection cn = new OracleConnection(GetConnection()))
            {
                OracleDataAdapter da = new OracleDataAdapter();
                OracleCommand cmd = new OracleCommand(sqlStr, cn);
                cn.Open();
                cmd.CommandText = sqlStr;
                cmd.Connection = cn;
                da.SelectCommand = cmd;
                foreach (DictionaryEntry entry in paramList)
                {
                    cmd.Parameters.AddWithValue(entry.Key.ToString(),entry.Value);
                }

                da.Fill(dt);
            }
            return dt;

        }

        public DataTable GetOracleDatatableUsingConStr(string paramConString, string sqlStr)
        {
            DataTable dt = new DataTable();
            using (OracleConnection cn = new OracleConnection(paramConString))
            {
                OracleDataAdapter da = new OracleDataAdapter();
                OracleCommand cmd = new OracleCommand(sqlStr, cn);

                da.SelectCommand = cmd;

                da.Fill(dt);
            }
            return dt;
        }
        public bool ExecuteQueryforOracle(string sqlStr)
        {
            bool result = false;
            using (OracleConnection cn = new OracleConnection(GetConnection()))
            {
                if (cn.State != ConnectionState.Open)
                    cn.Open();

                OracleCommand cmd = new OracleCommand(sqlStr, cn);
                result=  cmd.ExecuteNonQuery()>0?true:false;

                if (cn.State != ConnectionState.Closed)
                    cn.Close();
            }
            return result;
        }

        public bool ExecuteQueryforOracle(string sqlStr, Hashtable paramList)
        {
            bool isInserted = true;
            using (OracleConnection cn = new OracleConnection(GetConnection()))
            {
                OracleCommand cmd = new OracleCommand();
                if (cn.State != ConnectionState.Open)
                    cn.Open();
                cmd.CommandText = sqlStr;
                cmd.Connection = cn;
                foreach (DictionaryEntry entry in paramList)
                {
                    cmd.Parameters.AddWithValue(entry.Key.ToString(), entry.Value);
                }
          isInserted= cmd.ExecuteNonQuery()>=1?true:false;
              isInserted = true;
                if (cn.State != ConnectionState.Closed)
                    cn.Close();
                return isInserted;
            }
        }

        public string GetOracleResult(string sqlStr)
        {
            string retVal = string.Empty;
            OracleCommand cmd = new OracleCommand();
            OracleDataAdapter da = new OracleDataAdapter();
            DataSet ds;
            using (OracleConnection cn = new OracleConnection(GetConnection()))
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
        public string GetOracleResult(string sqlStr, Hashtable paramList)
        {
            string retVal = string.Empty;
            OracleCommand cmd = new OracleCommand();
            OracleDataAdapter da = new OracleDataAdapter();
            DataSet ds;
            using (OracleConnection cn = new OracleConnection(GetConnection()))
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
        public void ExecuteProc(string procName, OracleParameterCollection paramList)
        {
            try
            {
                using (OracleConnection cn = new OracleConnection(GetConnection()))
                {
                    cn.Open();
                    OracleCommand cmd = new OracleCommand();
                    cmd.Connection = cn;
                    cmd.CommandText = procName;
                    cmd.CommandType = CommandType.StoredProcedure;
                    foreach (OracleParameter param in paramList)
                    {
                        cmd.Parameters.Add(param.ParameterName, param.OracleType).Value = param.Value;
                    }
                   cmd.ExecuteNonQuery();
                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

    }
}
