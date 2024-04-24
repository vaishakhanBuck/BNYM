using System.Text;
using System.Data;
using System.Collections;
using System;
namespace  BNYM_Process2
{
    class Profile
    {

        int columnCount = 0;
        int ColumnSucess = 0;
        int ColumnFailed = 0;
        int columnMappingMissing = 0;
        
        public Profile()
        {
            profileCount = 0;
            profileSucess = 0;
            profileFailed = 0;
        }
        public void processProfile(DataTable dtClientData, DataRow dtClientRow, string clientId, string logFile, string logfileCsv)
        {
            Operations db = new Operations();
            UtilityClass utility = new UtilityClass();

            if (!utility.AreAllColumnsEmpty(dtClientRow))
            {
                profileCount++;
                DataTable dtProfileStructure;
                XmlReader xmlReader = new XmlReader();

                dtProfileStructure = xmlReader.ReadXML(clientId, 2, logFile);
                string baseId = string.Empty;
                Hashtable htProfileData = new Hashtable();
                if (dtProfileStructure.Rows.Count > 0)
                {
                    for (int pro = 0; pro < dtProfileStructure.Rows.Count; pro++)
                    {
                        string key = string.Empty;
                        string value = string.Empty;
                        key = dtProfileStructure.Rows[pro]["key"].ToString().Trim();
                        value = dtProfileStructure.Rows[pro]["value"].ToString().Trim();
                        columnCount = dtProfileStructure.Rows.Count - 1;
                        bool isMapping = false;

                        for (int col = 0; col < dtClientData.Columns.Count; col++)
                        {
                            try
                            {
                                string colName = string.Empty;
                                string fieldText = string.Empty;

                                colName = dtClientData.Columns[col].ColumnName.ToString().Trim();
                                fieldText = dtClientRow[colName].ToString().Trim();

                                //if (key.Trim().Equals("baseId"))
                                //{
                                //    if (value.Equals(colName))
                                //    {
                                //        isMapping = true;
                                //        baseId = db.RemoveSpecialCharactersBaseId(fieldText);

                                //        break;
                                //    }
                                //}
                                if (value.Equals(colName))
                                {
                                    isMapping = true;
                                    if (key.Trim().ToLower().Equals("baseid"))
                                    {
                                        fieldText = db.RemoveSpecialCharactersBaseId(fieldText);
                                        baseId = fieldText;
                                    }
                                    else
                                    {
                                        //!string.IsNullOrEmpty(fieldText.Trim()) &&

                                        if (value.Trim().ToUpper().Contains("DATE"))
                                        {
                                            fieldText = utility.FormatDate(clientId, logFile, fieldText);
                                        }
                                        htProfileData.Add(key, fieldText);
                                     
                                    }
                                    break;
                                }
                                else
                                {
                                    isMapping = false;
                                }
                            }
                            catch (Exception ex)
                            {
                                 Logs.WriteLog(logFile, ex.Message);
                            }

                        }
                        if (!isMapping)
                        {
                             Logs.WriteLog(logFile, "Cannot Find Mapping or column value or is Empty for the order : " + key);
                            columnMappingMissing++;
                        }

                    }

                    if (!string.IsNullOrEmpty(baseId))
                    {

                        foreach (DictionaryEntry item in htProfileData)
                        {

                            if (!db.IsExistProfile(clientId, baseId, item.Key.ToString()) )
                            {
                                try
                                {

                                    if (db.InsertMemberProfile(clientId, "12", baseId.Replace("-", " "), item.Key.ToString(), db.RemoveSpecialCharacters(item.Value.ToString())))
                                    {
                                        ColumnSucess++;
                                    }
                                    else
                                    {
                                        ColumnFailed++;
                                        Logs.WriteLog(logFile, "Failed To Insert Profile , orderid " + item.Key.ToString() + " , baseId " + baseId);
                                    }
                                }
                                catch (Exception ex)
                                {

                                    ColumnFailed++;
                                    Logs.WriteLog(logFile, "Failed To Insert Profile , orderid " + item.Key.ToString() + " , baseId " + baseId);
                                    Logs.WriteLog(logFile, ex.Message);
                                }

                            }
                            else
                            {
                                try
                                {
                                    //ColumnSucess++;
                                    //{
                                    if (db.UpdateMemberProfile(clientId, "12", baseId, item.Key.ToString(), db.RemoveSpecialCharacters(item.Value.ToString())))
                                    {
                                        ColumnSucess++;
                                    }
                                    else
                                    {
                                        ColumnFailed++;
                                        Logs.WriteLog(logFile, "Failed To Update Profile , Order Id " + item.Key.ToString() + ", baseId " + baseId);
                                    }
                                }
                                catch (Exception ex)
                                {

                                    ColumnFailed++;
                                    Logs.WriteLog(logFile, "Failed To Update Profile , Order Id " + item.Key.ToString() + ", baseId " + baseId);
                                    Logs.WriteLog(logFile, ex.Message);

                                }
                            }

                        }
                        if (ColumnFailed > 0)
                        {
                            profileFailed++;
                            StringBuilder sbCsv = new StringBuilder();
                            for (int col = 0; col < dtClientData.Columns.Count; col++)
                            {
                                string colName = string.Empty;
                                string fieldText = string.Empty;
                                colName = dtClientData.Columns[col].ColumnName.ToString().Trim();
                                fieldText = dtClientRow[colName].ToString().Trim();
                                sbCsv.Append(dtClientRow[colName].ToString() + " ,");
                            }
                            Logs.WriteLogCsv(logFile, sbCsv);
                        }
                        else
                        {
                            profileSucess++;
                        }

                        Logs.WriteLog(logFile, "BaseId : " + baseId);
                        Logs.WriteLog(logFile, "Total Number of columns (Profile) : " + columnCount);
                        Logs.WriteLog(logFile, "Profile columns inserted : " + ColumnSucess);
                        Logs.WriteLog(logFile, "Profile columns failed : " + ColumnFailed);
                        Logs.WriteLog(logFile, "Profile columns no matching fields : " + columnMappingMissing);
                        Logs.WriteLog(logFile, "");
                        columnCount = 0;
                        ColumnFailed = 0;
                        ColumnSucess = 0;
                        columnMappingMissing = 0;
                    }
                    else
                    {
                        profileFailed++;
                         Logs.WriteLog(logFile, "BaseID is Empty");
                    }

                }
                else
                {
                    profileFailed++;
                     Logs.WriteLog(logFile, "Process canceled : Xml file for memberprofile could not be found/mapped.");
                }
            }
            else
            {
                profileFailed++;
                 Logs.WriteLog(logFile, "Row is empty");
            }

        }
        public void processProfile_new(DataTable dtClientData, DataRow dtClientRow, string clientId, string logFile, string logfileCsv)
        {
            Operations db = new Operations();
            UtilityClass utility = new UtilityClass();

            if (!utility.AreAllColumnsEmpty(dtClientRow))
            {
                profileCount++;
                DataTable dtProfileStructure;
                XmlReader xmlReader = new XmlReader();

                dtProfileStructure = xmlReader.ReadXML(clientId, 2, logFile);
                //Get plan year starts
                DataTable dtBNYMFieldStructure = xmlReader.ReadXML(clientId, 4, logFile);
                DataRow drBNYMPlanYear = dtBNYMFieldStructure.Rows[1];
                string plan_year = drBNYMPlanYear["default_value"].ToString().Trim();
                DataRow drBNYMEmployeId = dtBNYMFieldStructure.Rows[4];
                //DataRow drBNYMPackageType = dtBNYMFieldStructure.Rows[5];
                string employee_id_key = drBNYMEmployeId["value"].ToString().Trim();
                //string package_type_key = drBNYMPackageType["value"].ToString().Trim();

                string baseId = string.Empty;
                Hashtable htProfileData = new Hashtable();

                if (dtProfileStructure.Rows.Count > 0)
                {


                    bool isMapping = false;
                    OracleDBOperations dataOp = new OracleDBOperations();
                    string employee_id = dtClientRow[employee_id_key].ToString();
                    //string package_type = dtClientRow[package_type_key].ToString();
                    string sqlStr = "";
                    //creation of dynamic query starts
                    string commmaSepColumns = CommaSeparated(dtProfileStructure);
                    string sqlDynamic = "select " + commmaSepColumns + " from ISS4_BNYM_EMP_DETAILS where  client_id=" + clientId + " and employee_id=" + employee_id + " and plan_year=" + plan_year;

                    sqlStr = sqlDynamic;

                    //sqlStr = "select employee_id,term_date,term_date_plus_45,ret_elig_dt,package_type,NOFE,notice_date,elected_coverage,coverage_end_date,LHH_FLYER,to_char(sev_amount, 'L999,999,999.00') as sev_amount from ISS4_BNYM_EMP_DETAILS where  client_id=" + clientId + " and employee_id=" + employee_id + " and plan_year=" + plan_year;

                    DataSet ds = new DataSet();
                    ds = dataOp.GetDSResult(sqlStr);
                    for (int pro = 0; pro < dtProfileStructure.Rows.Count; pro++)
                    {
                        string key = string.Empty;
                        string value = string.Empty;
                        string field_type = string.Empty;
                        key = dtProfileStructure.Rows[pro]["key"].ToString().Trim();
                        value = dtProfileStructure.Rows[pro]["value"].ToString().Trim();
                        columnCount = dtProfileStructure.Rows.Count - 1;
                       // field_type = dtProfileStructure.Rows[pro]["field_type"].ToString().Trim();

                        for (int col = 0; col < ds.Tables[0].Columns.Count; col++)
                        {
                            try
                            {
                                string colName = string.Empty;
                                string fieldText = string.Empty;

                                colName = ds.Tables[0].Columns[col].ColumnName.ToString().Trim();

                                fieldText = ds.Tables[0].Rows[0][colName].ToString().Trim();


                                if (value.Equals(colName.Trim().ToLower()))
                                {
                                    isMapping = true;
                                    if (value.Trim().ToLower().Equals("employee_id"))
                                    {
                                        fieldText = db.RemoveSpecialCharactersBaseId(fieldText);
                                        baseId = fieldText;
                                        htProfileData.Add(key, fieldText);
                                    }
                                    else
                                    {
                                        if (value.Trim().ToUpper().Contains("TERM_DATE_PLUS_45"))
                                        {
                                            //if (package_type == "BNYMTBA")
                                            //{
                                            //    fieldText = fieldText + " (21 Days from Term Date)";
                                            //}
                                            ////removed as per latest BRD v3.0//as per Ravi's mail on 10/7/2020
                                            ////else if (package_type == "BNYMTOP50" || package_type == "BNYMTOP")
                                            ////{
                                            ////    fieldText = fieldText + " (6 Months from Term Date)";
                                            ////}
                                            //else
                                            //{
                                            //    fieldText = fieldText + " (45 Days from Term Date)";
                                            //}
                                        }

                                        if (value.Trim().ToUpper().Contains("DATE") || value.Trim().ToUpper().Contains("DOB") || value.Trim().ToUpper().Contains("_DT") || value.Trim().ToUpper().Contains("LAST_DAY_WORKED"))
                                        {
                                            fieldText = utility.FormatDate(clientId, logFile, fieldText);
                                            fieldText = fieldText.Replace(" 12:00:00 AM", "");
                                        }
                                        //if (field_type == "U")
                                        //{
                                        //    fieldText = "US " + fieldText;
                                        //}
                                        //else if (field_type == "A")
                                        //{
                                        //    fieldText = "CA " + fieldText;
                                        //}
                                        //else if (field_type == "P")
                                        //{
                                        //    fieldText = fieldText.Replace("$", "");
                                        //    fieldText = "£" + fieldText;
                                        //}
                                        htProfileData.Add(key, fieldText);

                                    }
                                    break;
                                }
                                else
                                {
                                    isMapping = false;
                                }
                            }
                            catch (Exception ex)
                            {
                                Logs.WriteLog(logFile, ex.Message);
                            }

                        }
                    }

                    //if (!isMapping)
                    //{
                    //     Logs.WriteLog(logFile, "Cannot Find Mapping or column value or is Empty for the order : " + key);
                    //    columnMappingMissing++;
                    //}



                    if (!string.IsNullOrEmpty(baseId))
                    {

                        foreach (DictionaryEntry item in htProfileData)
                        {

                            if (!db.IsExistProfile(clientId, baseId, item.Key.ToString()))
                            {
                                try
                                {

                                    if (db.InsertMemberProfile(clientId, "12", baseId.Replace("-", " "), item.Key.ToString(), db.RemoveSpecialCharacters(item.Value.ToString())))
                                    {
                                        ColumnSucess++;
                                    }
                                    else
                                    {
                                        ColumnFailed++;
                                        Logs.WriteLog(logFile, "Failed To Insert Profile , orderid " + item.Key.ToString() + " , baseId " + baseId);
                                    }
                                }
                                catch (Exception ex)
                                {

                                    ColumnFailed++;
                                    Logs.WriteLog(logFile, "Failed To Insert Profile , orderid " + item.Key.ToString() + " , baseId " + baseId);
                                    Logs.WriteLog(logFile, ex.Message);
                                }

                            }
                            else
                            {
                                try
                                {
                                    //ColumnSucess++;
                                    //{
                                    if (db.UpdateMemberProfile(clientId, "12", baseId, item.Key.ToString(), db.RemoveSpecialCharacters(item.Value.ToString())))
                                    {
                                        ColumnSucess++;
                                    }
                                    else
                                    {
                                        ColumnFailed++;
                                        Logs.WriteLog(logFile, "Failed To Update Profile , Order Id " + item.Key.ToString() + ", baseId " + baseId);
                                    }
                                }
                                catch (Exception ex)
                                {

                                    ColumnFailed++;
                                    Logs.WriteLog(logFile, "Failed To Update Profile , Order Id " + item.Key.ToString() + ", baseId " + baseId);
                                    Logs.WriteLog(logFile, ex.Message);

                                }
                            }

                        }
                        if (ColumnFailed > 0)
                        {
                            profileFailed++;
                            StringBuilder sbCsv = new StringBuilder();
                            for (int col = 0; col < dtClientData.Columns.Count; col++)
                            {
                                string colName = string.Empty;
                                string fieldText = string.Empty;
                                colName = dtClientData.Columns[col].ColumnName.ToString().Trim();
                                fieldText = dtClientRow[colName].ToString().Trim();
                                sbCsv.Append(dtClientRow[colName].ToString() + " ,");
                            }
                            Logs.WriteLogCsv(logFile, sbCsv);
                        }
                        else
                        {
                            profileSucess++;
                        }

                        Logs.WriteLog(logFile, "BaseId : " + baseId);
                        Logs.WriteLog(logFile, "Total Number of columns (Profile) : " + columnCount);
                        Logs.WriteLog(logFile, "Profile columns inserted : " + ColumnSucess);
                        Logs.WriteLog(logFile, "Profile columns failed : " + ColumnFailed);
                        Logs.WriteLog(logFile, "Profile columns no matching fields : " + columnMappingMissing);
                        Logs.WriteLog(logFile, "");
                        columnCount = 0;
                        ColumnFailed = 0;
                        ColumnSucess = 0;
                        columnMappingMissing = 0;
                    }
                    else
                    {
                        profileFailed++;
                        Logs.WriteLog(logFile, "BaseID is Empty");
                    }

                }
                else
                {
                    profileFailed++;
                    Logs.WriteLog(logFile, "Process canceled : Xml file for memberprofile could not be found/mapped.");
                }
            }
            else
            {
                profileFailed++;
                Logs.WriteLog(logFile, "Row is empty");
            }

        }
        public string CommaSeparated(DataTable dt)
        {

            string output = string.Empty;
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                //string field_type = dt.Rows[i]["field_type"].ToString().Trim();
                string field_name = dt.Rows[i]["value"].ToString();
                //if (field_type == "")
                //{
                    output = output + dt.Rows[i]["value"].ToString();
                //}
                //else if (field_type == "U")
                //{
                //    output = output + "to_char(" + field_name + ",'L999,999,999.00') as " + field_name;

                //}
                //else if (field_type == "A")
                //{
                //    output = output + "to_char(" + field_name + ",'L999,999,999.00') as " + field_name;

                //}
                //else if (field_type == "P")
                //{
                //    output = output + "to_char(" + field_name + ",'L999,999,999.00') as " + field_name;

                //}
                //else if (field_type == "O")
                //{
                //    output = output + "to_char(" + field_name + ",'L999,999,999.00') as " + field_name;

                //}
                output += (i < (dt.Rows.Count - 1)) ? "," : string.Empty;
            }
            return output;

        }
        public int profileCount { get; set; }
        public int profileSucess { get; set; }
        public int profileFailed { get; set; }

        public void ShowProfileSummery(string logFile)
        {
             Logs.WriteLog(logFile, "----Profile ----");

             Logs.WriteLog(logFile, "Total " + profileCount);
             Logs.WriteLog(logFile, "Success " + profileSucess);
             Logs.WriteLog(logFile, "Failed " + profileFailed);

        }

    }
}


