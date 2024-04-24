using System;
using System.Text;
using System.Data;
using System.Collections;
using System.Linq;
namespace BNYM_Process2
{
    class Member
    {

        public Member()
        {
            sucessMember = 0;
            failedMember = 0;
            updatedMembers = 0;
            newMembers = 0;

        }


        public void processMember(DataTable dtClientData, DataRow dtClientRow, string clientId, string logFile, string logfileCsv)
        {
            Operations dbOperations = new Operations();
            UtilityClass utility = new UtilityClass();

            if (!utility.AreAllColumnsEmpty(dtClientRow))
            {
                DataTable dtMemberStructure, dtSettings;
                XmlReader xmlReader = new XmlReader();
                string dateFormat = string.Empty;
                dtMemberStructure = xmlReader.ReadXML(clientId, 1, logFile);

                if (dtMemberStructure.Rows.Count > 0)
                {
                    string password = string.Empty;
                    string baseId = string.Empty;
                    bool isPwdGenerated = false;
                    Hashtable htMemberData = new Hashtable();
                    StringBuilder sbCsv = new StringBuilder();

                    //sbCsv.Append("\"" + dtClientRow[colName].ToString() + "\" , ");
                    for (int i = 0; i < dtMemberStructure.Rows.Count; i++)
                    {
                        string key = string.Empty;
                        string value = string.Empty;
                        string default_value = string.Empty;
                        key = dtMemberStructure.Rows[i]["key"].ToString().Trim();
                        value = dtMemberStructure.Rows[i]["value"].ToString().Trim();
                        default_value = dtMemberStructure.Rows[i]["default_value"].ToString().Trim();
                        bool isMapping = false;
                        for (int col = 0; col < dtClientData.Columns.Count; col++)
                        {
                            string colName = string.Empty;
                            string fieldText = string.Empty;
                            colName = dtClientData.Columns[col].ColumnName.ToString().Trim();
                            fieldText =dbOperations.RemoveSpecialCharacters(dtClientRow[colName].ToString());

                            if (value == colName)
                            {
                                if (key == "FirstName")
                                {
                                    //var s = "INAGX4 Agatti Island";
                                    var lastSpaceIndex = fieldText.LastIndexOf(" ");
                                    var firstString = fieldText.Substring(0, lastSpaceIndex); // INAGX4
                                    //var secondString = fieldText.Substring(firstSpaceIndex + 1);
                                    //string[] ssize = fieldText.Split(new char[0]);
                                    fieldText = firstString;
                                    
                                }
                                if (key == "LastName")
                                {
                                    string lastWord = fieldText.Split(' ').Last();
                                    //var firstSpaceIndex = fieldText.IndexOf(" ");
                                    //var firstString = fieldText.Substring(0, firstSpaceIndex); // INAGX4
                                    //var secondString = fieldText.Substring(firstSpaceIndex + 1);
                                    //string[] ssize = fieldText.Split(new char[0]);
                                    fieldText = lastWord;
                                }
                                if (key.ToUpper().Trim().Equals("BASEID") && !string.IsNullOrEmpty(fieldText))
                                {
                                    baseId = dbOperations.RemoveSpecialCharactersBaseId(fieldText);
                                    htMemberData.Add(key, baseId.Replace("'", "''"));
                                    isMapping = true;
                                }
                                else
                                {
                                    htMemberData.Add(key, fieldText.Replace("'", "''"));
                                    isMapping = true;
                                   
                                }
                                break;
                            }
                            else
                            {
                                isMapping = false;
                                continue;
                            }
                        }
                        if (!isMapping)
                        {
                            if (key.ToUpper().Equals("PASSWORD") && !string.IsNullOrEmpty(default_value))
                            {
                                password = GeneratePassword(default_value.Trim());
                                if (!string.IsNullOrEmpty(password))
                                {
                                    isPwdGenerated = true;
                                    htMemberData.Add(key.ToUpper(), password);
                                }
                            }
                            else
                            {
                                htMemberData.Add(key, default_value);
                            }

                        }
                    }
                    //Strart Db operations
                    if (!string.IsNullOrEmpty(baseId))
                    {
                     
                        string memberId = string.Empty;
                        memberId = dbOperations.GetMemberID(clientId, baseId);
                        if (string.IsNullOrEmpty(memberId.Trim()))
                        {
                            memberId = dbOperations.GetNextSeqNumber("iss3_member_id_seq");
                            try
                            {
                                if (dbOperations.InsertUser(clientId, htMemberData, memberId))
                                {
                                    sucessMember++;
                                    newMembers++;
                                     Logs.WriteLog(logFile, "Member inserted successfully, BaseId :" +dbOperations.RemoveSpecialCharactersBaseId(baseId));

                                }
                                else
                                {
                                    failedMember++;
                                    GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                     Logs.WriteLog(logFile, "Failed to insert member, BaseId :" +dbOperations.RemoveSpecialCharactersBaseId(baseId));


                                }
                            }
                            catch (Exception ex)
                            {
                                failedMember++;
                                GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                 Logs.WriteLog(logFile, "Failed to insert member, BaseId :" + baseId);
                                 Logs.WriteLog(logFile, "Exception:" + ex.Message);
                            }
                        }
                        else
                        {
                            //Update Memeber  Update member portion not need for USSTEEL
                           
                            try
                            {
                                 //Logs.WriteLog(logFile, "Member already Exist Base Id " + baseId);
                                //sucessMember++;
                                //updatedMembers++;
                                if (baseId.Contains("-"))
                                {
                                    baseId = baseId.Contains("-") ? baseId.Replace("-", " ") : baseId;
                                    if (dbOperations.updateUser(clientId, htMemberData))
                                    {
                                        sucessMember++;
                                        updatedMembers++;
                                        Logs.WriteLog(logFile, " Member updated successfully , BaseId :-" + baseId);
                                    }
                                    else
                                    {
                                        failedMember++;
                                        GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                        Logs.WriteLog(logFile, "Failed to update member , BaseId :-" + baseId);
                                    }
                                }
                                else
                                {
                                    if (dbOperations.updateUser(clientId, htMemberData))
                                    {
                                        sucessMember++;
                                        updatedMembers++;
                                        Logs.WriteLog(logFile, " Member updated successfully , BaseId :-" + baseId);
                                    }
                                    else
                                    {
                                        failedMember++;
                                        GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                        Logs.WriteLog(logFile, "Failed to update member , BaseId :-" + baseId);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {

                                failedMember++;
                                GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                 Logs.WriteLog(logFile, "Failed to update member , BaseId :-" + baseId);
                                 Logs.WriteLog(logFile, "Exception:" + ex.Message);
                            }
                        }
                        //dbOperations.UpdateMemberSystems(htMemberData, clientId);
                    }
                    else
                    {
                         Logs.WriteLog(logFile, "Cannot process the row due to empty/invalid Employee_Id");
                        failedMember++;
                    }
                }
                else
                {
                     Logs.WriteLog(logFile, "Cannot find member structure");
                }
            }
            else
            {
                 Logs.WriteLog(logFile, "Empty row found.");
            }

        }
        public void insert_ISS4_BNYM_EMP_DETAILS(DataTable dtClientData, DataRow dtClientRow, string clientId, string logFile, string logfileCsv,string docNumber,string jobNumber,string creation_date)
        {
            Operations dbOperations = new Operations();
            UtilityClass utility = new UtilityClass();

            if (!utility.AreAllColumnsEmpty(dtClientRow))
            {
                DataTable dtMemberStructure, dtSettings;
                XmlReader xmlReader = new XmlReader();
                string dateFormat = string.Empty;
                dtMemberStructure = xmlReader.ReadXML(clientId, 4, logFile);

                if (dtMemberStructure.Rows.Count > 0)
                {
                    string password = string.Empty;
                    string baseId = string.Empty;
                    string planYear = string.Empty;
                    Hashtable htMemberData = new Hashtable();
                    StringBuilder sbCsv = new StringBuilder();

                    //sbCsv.Append("\"" + dtClientRow[colName].ToString() + "\" , ");
                    for (int i = 0; i < dtMemberStructure.Rows.Count; i++)
                    {
                        string key = string.Empty;
                        string value = string.Empty;
                        string default_value = string.Empty;
                        key = dtMemberStructure.Rows[i]["key"].ToString().Trim();
                        value = dtMemberStructure.Rows[i]["value"].ToString().Trim();
                        default_value = dtMemberStructure.Rows[i]["default_value"].ToString().Trim();
                        bool isMapping = false;
                        for (int col = 0; col < dtClientData.Columns.Count; col++)
                        {
                            string colName = string.Empty;
                            string fieldText = string.Empty;
                            colName = dtClientData.Columns[col].ColumnName.ToString().Trim();
                            fieldText = dbOperations.RemoveSpecialCharacters(dtClientRow[colName].ToString());
                            if (key.ToUpper().Trim().Equals("PLAN_YEAR"))
                            {
                                planYear = default_value;
                                htMemberData.Add(key, planYear.Replace("'", "''"));
                                isMapping = true;
                                break;
                            }
                            if (value == colName)
                            {
                                
                                
                                if (key.ToUpper().Trim().Equals("BASEID") && !string.IsNullOrEmpty(fieldText))
                                {
                                    baseId = dbOperations.RemoveSpecialCharactersBaseId(fieldText);
                                    htMemberData.Add(key, baseId.Replace("'", "''"));
                                    isMapping = true;
                                }
                                else
                                {
                                    htMemberData.Add(key, fieldText.Replace("'", "''"));
                                    isMapping = true;

                                }
                                break;
                            }
                            else
                            {
                                isMapping = false;
                                continue;
                            }
                        }
                        
                    }
                    //Strart Db operations
                    if (!string.IsNullOrEmpty(baseId))
                    {

                        string memberId = string.Empty;
                        memberId = dbOperations.checkbaseIdExistsforPlanYear(clientId, baseId, planYear);
                        if (string.IsNullOrEmpty(memberId.Trim()))
                        {
                            memberId = baseId;
                            try
                            {
                                if (dbOperations.InsertEmployeeDetails(clientId, htMemberData, memberId,docNumber,jobNumber,creation_date))
                                {
                                    sucessBNYMEmployee++;
                                    newBNYMEmployee++;
                                    Logs.WriteLog(logFile, "BNYM Employee details inserted successfully, Employee Id :" + dbOperations.RemoveSpecialCharactersBaseId(baseId));

                                }
                                else
                                {
                                    failedBNYMEmployee++;
                                    GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                    Logs.WriteLog(logFile, "Failed to insert BNYM Employee, Employee Id :" + dbOperations.RemoveSpecialCharactersBaseId(baseId));


                                }
                            }
                            catch (Exception ex)
                            {
                                failedBNYMEmployee++;
                                GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                Logs.WriteLog(logFile, "Failed to insert BNYM Employee, Employee Id :" + baseId);
                                Logs.WriteLog(logFile, "Exception:" + ex.Message);
                            }
                        }
                        else
                        {
                            //Update Member  

                            try
                            {
                                Logs.WriteLog(logFile, "BNYM Employee already Exist Employee Id " + baseId);

                                if (baseId.Contains("-"))
                                {
                                    baseId = baseId.Contains("-") ? baseId.Replace("-", " ") : baseId;
                                }
                                //if (baseId.Contains("50061843"))
                                //{
                                //    int i = 1;
                                //}
                                //if (baseId.Contains("50024655"))
                                //{
                                //    int i = 1;
                                //}
                                //if (baseId.Contains("50027122"))
                                //{
                                //    int i = 1;
                                //}
                                
                                //if (baseId.Contains("50033580"))
                                //{
                                //    int i = 1;
                                //}
                                    if (dbOperations.UpdateEmployeeDetails(clientId, htMemberData, memberId, docNumber, jobNumber,creation_date,logFile))
                                    {
                                        sucessBNYMEmployee++;
                                        updatedBNYMEmployee++;
                                        Logs.WriteLog(logFile, " BNYM Employee updated successfully , Employee Id :-" + baseId);
                                    }
                                    else
                                    {
                                        failedBNYMEmployee++;
                                        GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                        Logs.WriteLog(logFile, "Failed to update BNYM Employee , Employee Id :-" + baseId);
                                    }
                                
                               
                            }
                            catch (Exception ex)
                            {

                                failedBNYMEmployee++;
                                GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                Logs.WriteLog(logFile, "Failed to update BNYM Employee , Employee Id :-" + baseId);
                                Logs.WriteLog(logFile, "Exception:" + ex.Message);
                            }
                        }
                        //dbOperations.UpdateMemberSystems(htMemberData, clientId);
                    }
                    else
                    {
                        Logs.WriteLog(logFile, "Cannot process the row due to empty/invalid Employee_Id");
                        failedMember++;
                    }
                }
                else
                {
                    Logs.WriteLog(logFile, "Cannot find member structure");
                }
            }
            else
            {
                Logs.WriteLog(logFile, "Empty row found.");
            }

        }


        public void insert_SEV_EMP_DETAILS(DataTable dtClientData, DataRow dtClientRow, string clientId, string logFile, string logfileCsv, string docNumber, string jobNumber, string creation_date)
        {
            Operations dbOperations = new Operations();
            UtilityClass utility = new UtilityClass();

            if (!utility.AreAllColumnsEmpty(dtClientRow))
            {
                DataTable dtMemberStructure, dtSettings;
                XmlReader xmlReader = new XmlReader();
                string dateFormat = string.Empty;
                dtMemberStructure = xmlReader.ReadXML(clientId, 1, logFile);

                if (dtMemberStructure.Rows.Count > 0)
                {
                    string password = string.Empty;
                    string baseId = string.Empty;
                    string planYear = string.Empty;
                    Hashtable htMemberData = new Hashtable();
                    StringBuilder sbCsv = new StringBuilder();

                    //sbCsv.Append("\"" + dtClientRow[colName].ToString() + "\" , ");
                    for (int i = 0; i < dtMemberStructure.Rows.Count; i++)
                    {
                        string key = string.Empty;
                        string value = string.Empty;
                        string default_value = string.Empty;
                        key = dtMemberStructure.Rows[i]["key"].ToString().Trim();
                        value = dtMemberStructure.Rows[i]["value"].ToString().Trim();
                        default_value = dtMemberStructure.Rows[i]["default_value"].ToString().Trim();
                        bool isMapping = false;
                        for (int col = 0; col < dtClientData.Columns.Count; col++)
                        {
                            string colName = string.Empty;
                            string fieldText = string.Empty;
                            colName = dtClientData.Columns[col].ColumnName.ToString().Trim();
                            
                                fieldText = dbOperations.RemoveSpecialCharacters(dtClientRow[col].ToString());
                            
                            
                            if (value == colName)
                            {


                                if (key.ToUpper().Trim().Equals("EMPLOYEE_ID") && !string.IsNullOrEmpty(fieldText))
                                {
                                    baseId = dbOperations.RemoveSpecialCharactersBaseId(fieldText);
                                    htMemberData.Add(key, baseId.Replace("'", "''"));
                                    isMapping = true;
                                }
                                else
                                {
                                    htMemberData.Add(key, fieldText.Replace("'", "''"));
                                    isMapping = true;

                                }
                                break;
                            }
                            else
                            {
                                isMapping = false;
                                continue;
                            }
                        }

                    }
                    //Strart Db operations
                    if (!string.IsNullOrEmpty(baseId))
                    {

                        string memberId = string.Empty;
                        memberId = dbOperations.checkbaseIdExistsforPlanYear(clientId, baseId, planYear);
                        if (string.IsNullOrEmpty(memberId.Trim()))
                        {
                            memberId = baseId;
                            try
                            {
                                //Need to continue from here
                                if (dbOperations.InsertEmployeeDetailsIF(clientId, htMemberData, memberId, docNumber, jobNumber, creation_date))
                                {
                                    sucessBNYMEmployee++;
                                    newBNYMEmployee++;
                                    Logs.WriteLog(logFile, "SEV_EMP_DETAILS  inserted successfully, Employee Id :" + dbOperations.RemoveSpecialCharactersBaseId(baseId));

                                }
                                else
                                {
                                    failedBNYMEmployee++;
                                    GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                    Logs.WriteLog(logFile, "Failed to insert BNYM Employee in SEV_BASE_POP_REPORT, Employee Id :" + dbOperations.RemoveSpecialCharactersBaseId(baseId));


                                }
                            }
                            catch (Exception ex)
                            {
                                failedBNYMEmployee++;
                                GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                Logs.WriteLog(logFile, "Failed to insert BNYM Employee in SEV_EMP_DETAILS, Employee Id :" + baseId);
                                Logs.WriteLog(logFile, "Exception:" + ex.Message);
                            }
                        }
                        else
                        {
                            //Update Member  

                            try
                            {
                                Logs.WriteLog(logFile, "BNYM Employee already Exist in SEV_BASE_POP_REPORT- Employee Id " + baseId);

                                if (baseId.Contains("-"))
                                {
                                    baseId = baseId.Contains("-") ? baseId.Replace("-", " ") : baseId;
                                }
                                //if (baseId.Contains("50061843"))
                                //{
                                //    int i = 1;
                                //}
                                
                                if (dbOperations.UpdateEmployeeDetailsIF(clientId, htMemberData, memberId, docNumber, jobNumber, creation_date))
                                {
                                    sucessBNYMEmployee++;
                                    updatedBNYMEmployee++;
                                    Logs.WriteLog(logFile, " BNYM Employee updated successfully in SEV_EMP_DETAILS, Employee Id :-" + baseId);
                                }
                                else
                                {
                                    failedBNYMEmployee++;
                                    GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                    Logs.WriteLog(logFile, "Failed to update BNYM Employee in SEV_EMP_DETAILS, Employee Id :-" + baseId);
                                }


                            }
                            catch (Exception ex)
                            {

                                failedBNYMEmployee++;
                                GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                Logs.WriteLog(logFile, "Failed to update BNYM Employee in SEV_EMP_DETAILS , Employee Id :-" + baseId);
                                Logs.WriteLog(logFile, "Exception:" + ex.Message);
                            }
                        }
                        //dbOperations.UpdateMemberSystems(htMemberData, clientId);
                    }
                    else
                    {
                        Logs.WriteLog(logFile, "Cannot process the row due to empty/invalid Employee_Id");
                        failedMember++;
                    }
                }
                else
                {
                    Logs.WriteLog(logFile, "Cannot find member structure");
                }
            }
            else
            {
                Logs.WriteLog(logFile, "Empty row found.");
            }

        }

        public void insert_Master_List(DataTable dtClientData, DataRow dtClientRow, string clientId, string logFile, string logfileCsv, string docNumber, string jobNumber, string creation_date)
        {
            Operations dbOperations = new Operations();
            UtilityClass utility = new UtilityClass();

            if (!utility.AreAllColumnsEmpty(dtClientRow))
            {
                DataTable dtMemberStructure, dtSettings;
                XmlReader xmlReader = new XmlReader();
                string dateFormat = string.Empty;
                dtMemberStructure = xmlReader.ReadXML(clientId, 2, logFile);

                if (dtMemberStructure.Rows.Count > 0)
                {
                    string password = string.Empty;
                    string baseId = string.Empty;
                    string planYear = string.Empty;
                    Hashtable htMemberData = new Hashtable();
                    StringBuilder sbCsv = new StringBuilder();

                    //sbCsv.Append("\"" + dtClientRow[colName].ToString() + "\" , ");
                    for (int i = 0; i < dtMemberStructure.Rows.Count; i++)
                    {
                        string key = string.Empty;
                        string value = string.Empty;
                        string default_value = string.Empty;
                        key = dtMemberStructure.Rows[i]["key"].ToString().Trim();
                        value = dtMemberStructure.Rows[i]["value"].ToString().Trim();
                        default_value = dtMemberStructure.Rows[i]["default_value"].ToString().Trim();
                        bool isMapping = false;
                        for (int col = 0; col < dtClientData.Columns.Count; col++)
                        {
                            string colName = string.Empty;
                            string fieldText = string.Empty;
                            colName = dtClientData.Columns[col].ColumnName.ToString().Trim();
                            if (col != 10)
                            {
                                fieldText = dbOperations.RemoveSpecialCharacters(dtClientRow[colName].ToString());
                            }
                            else
                            {
                                fieldText = dbOperations.RemoveSpecialCharacters(dtClientRow[10].ToString());
                            }
                            //if (key.ToUpper().Trim().Equals("PLAN_YEAR"))
                            //{
                            //    planYear = default_value;
                            //    htMemberData.Add(key, planYear.Replace("'", "''"));
                            //    isMapping = true;
                            //    break;
                            //}
                            if (value == colName)
                            {


                                if (key.ToUpper().Trim().Equals("EMPLOYEE_ID") && !string.IsNullOrEmpty(fieldText))
                                {
                                    baseId = dbOperations.RemoveSpecialCharactersBaseId(fieldText);
                                    htMemberData.Add(key, baseId.Replace("'", "''"));
                                    isMapping = true;
                                }
                                else
                                {
                                    htMemberData.Add(key, fieldText.Replace("'", "''"));
                                    isMapping = true;

                                }
                                break;
                            }
                            else
                            {
                                isMapping = false;
                                continue;
                            }
                        }

                    }
                    //Strart Db operations
                    if (!string.IsNullOrEmpty(baseId))
                    {

                        string memberId = string.Empty;
                        memberId = dbOperations.checkbaseIdExistsforPlanYear1(clientId, baseId, planYear);
                        if (string.IsNullOrEmpty(memberId.Trim()))
                        {
                            memberId = baseId;
                            try
                            {
                                //Need to continue from here
                                if (dbOperations.InsertEmployeeDetailsML(clientId, htMemberData, memberId, docNumber, jobNumber, creation_date))
                                {
                                    sucessBNYMEmployee1++;
                                    newBNYMEmployee1++;
                                    Logs.WriteLog(logFile, "SEV_MASTER_LIST  inserted successfully, Employee Id :" + dbOperations.RemoveSpecialCharactersBaseId(baseId));

                                }
                                else
                                {
                                    failedBNYMEmployee1++;
                                    GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                    Logs.WriteLog(logFile, "Failed to insert BNYM Employee in SEV_MASTER_LIST, Employee Id :" + dbOperations.RemoveSpecialCharactersBaseId(baseId));


                                }
                            }
                            catch (Exception ex)
                            {
                                failedBNYMEmployee1++;
                                GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                Logs.WriteLog(logFile, "Failed to insert BNYM Employee in SEV_MASTER_LIST, Employee Id :" + baseId);
                                Logs.WriteLog(logFile, "Exception:" + ex.Message);
                            }
                        }
                        else
                        {
                            //Update Member  

                            try
                            {
                                Logs.WriteLog(logFile, "BNYM Employee already Exist in SEV_MASTER_LIST- Employee Id " + baseId);

                                if (baseId.Contains("-"))
                                {
                                    baseId = baseId.Contains("-") ? baseId.Replace("-", " ") : baseId;
                                }
                                //if (baseId.Contains("50061843"))
                                //{
                                //    int i = 1;
                                //}

                                if (dbOperations.UpdateEmployeeDetailsML(clientId, htMemberData, memberId, docNumber, jobNumber, creation_date, logFile))
                                {
                                    sucessBNYMEmployee1++;
                                    updatedBNYMEmployee1++;
                                    Logs.WriteLog(logFile, " BNYM Employee updated successfully in SEV_MASTER_LIST, Employee Id :-" + baseId);
                                }
                                else
                                {
                                    failedBNYMEmployee1++;
                                    GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                    Logs.WriteLog(logFile, "Failed to update BNYM Employee in SEV_MASTER_LIST, Employee Id :-" + baseId);
                                }


                            }
                            catch (Exception ex)
                            {

                                failedBNYMEmployee1++;
                                GenerateCsvLog(dtClientData, dtClientRow, logfileCsv);
                                Logs.WriteLog(logFile, "Failed to update BNYM Employee in SEV_MASTER_LIST , Employee Id :-" + baseId);
                                Logs.WriteLog(logFile, "Exception:" + ex.Message);
                            }
                        }
                        //dbOperations.UpdateMemberSystems(htMemberData, clientId);
                    }
                    else
                    {
                        Logs.WriteLog(logFile, "Cannot process the row due to empty/invalid Employee Id.");
                        failedMember++;
                    }
                }
                else
                {
                    Logs.WriteLog(logFile, "Cannot find member structure");
                }
            }
            else
            {
                Logs.WriteLog(logFile, "Empty row found.");
            }

        }
        public void ShowMemeberStatus(string logFile)
        {
             Logs.WriteLog(logFile, "----- Member ------ ");
             Logs.WriteLog(logFile, "Success count-" + sucessMember);
             Logs.WriteLog(logFile, "Fail count-" + failedMember);
             Logs.WriteLog(logFile, "New member count-" + newMembers);
             Logs.WriteLog(logFile, "Updated member count-" + updatedMembers);
        }
        public void ShowEmpDetailsTableStatus(string logFile)
        {
            Logs.WriteLog(logFile, "----- SEV_EMP_DETAILS   Table ------ ");
            Logs.WriteLog(logFile, "Success count-" + sucessBNYMEmployee);
            Logs.WriteLog(logFile, "Fail count-" + failedBNYMEmployee);
            Logs.WriteLog(logFile, "New member count-" + newBNYMEmployee);
            Logs.WriteLog(logFile, "Updated member count-" + updatedBNYMEmployee);
        }
        public void ShowMasterListTableStatus(string logFile)
        {
            Logs.WriteLog(logFile, "----- MASTER LIST   Table ------ ");
            Logs.WriteLog(logFile, "Success count-" + sucessBNYMEmployee1);
            Logs.WriteLog(logFile, "Fail count-" + failedBNYMEmployee1);
            Logs.WriteLog(logFile, "New member count-" + newBNYMEmployee1);
            Logs.WriteLog(logFile, "Updated member count-" + updatedBNYMEmployee1);
        }
        private static string GeneratePassword(string defaultVal)
        {
            //Generate Password
            string pwdGenerateFunction = string.Empty;
            string generatedPwd = string.Empty;
            OracleDBOperations dataOp = new OracleDBOperations();
            string strSql = "select * from ISS4_CLIENT_SETUP_LOOKUP where lookup_type = 'MAILVAR' and choice_code = '" + defaultVal + "'";
            DataTable ds1 = new DataTable();
            ds1 = dataOp.GetOracleDatatable(strSql);
            if (ds1.Rows.Count > 0)
            {
                pwdGenerateFunction = Convert.ToString(ds1.Rows[0]["ADDITIONAL_INFO1"]);
            }
            strSql = "SELECT " + pwdGenerateFunction + " FROM DUAL";
            ds1 = new DataTable();
            try
            {
                ds1 = dataOp.GetOracleDatatable(strSql);
                if (ds1.Rows.Count > 0)
                {
                    generatedPwd = Convert.ToString(ds1.Rows[0][0]);
                }
            }
            catch (Exception ex)
            {
                generatedPwd = string.Empty;
                // Logs.WriteLog(logFile, "Password cannot be created." + dataText);
            }
            return generatedPwd;
        }
        private void GenerateCsvLog(DataTable dt, DataRow dtRow, string logfileCsv)
        {
            UtilityClass ut = new UtilityClass();
            StringBuilder sbCsv = new StringBuilder();
            if (!ut.AreAllColumnsEmpty(dtRow))
            {
                string colName = string.Empty;
                string colValue = string.Empty;
                for (int col = 0; col < dt.Columns.Count; col++)
                {
                    colName = string.Empty;
                    colValue = string.Empty;
                    colName = dt.Columns[col].ColumnName.ToString().Trim();
                    colValue = dtRow[colName].ToString();
                    sbCsv.Append(colValue.Replace(",", " ") + ",");

                }

            }
            Logs.WriteLogCsv(logfileCsv, sbCsv);

        }
        public int sucessMember { get; set; }
        public int failedMember { get; set; }
        public int updatedMembers { get; set; }
        public int newMembers { get; set; }

        public int sucessBNYMEmployee { get; set; }
        public int failedBNYMEmployee { get; set; }
        public int updatedBNYMEmployee { get; set; }
        public int newBNYMEmployee { get; set; }
        public int sucessBNYMEmployee1 { get; set; }
        public int failedBNYMEmployee1 { get; set; }
        public int updatedBNYMEmployee1 { get; set; }
        public int newBNYMEmployee1 { get; set; }
    }
}

