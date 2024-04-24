using System;
using System.Text;
using System.Collections;
using System.Data;
using System.Text.RegularExpressions;
using System.Linq;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Collections.Generic;
using Aspose.Words;
using Aspose.Words.Fields;
using Aspose.Words.Tables;

namespace  BNYM_Process2
{
    class Operations
    {
        #region Lod wave file
        private static string sqlstrOptimized = null;
        private static string log = "";
        private static bool alertAdded = false;
        private static int myOrderCount  =0;
        public bool InsertMemberProfileOptimized(string clientId, string profileId, string baseId, string myOrder, string fieldText)
        {
            myOrderCount++;
            try
            {   if (myOrderCount==1)
		       	{
                 sqlstrOptimized = "INSERT ALL into Iss4_member_profile (client_id, profile_id, base_id, my_order, field_text) values "+   "("+clientId+","+profileId+",'"+baseId+"',"+myOrder+",'"+fieldText+"')";
				}
				else
				{
                    sqlstrOptimized = sqlstrOptimized + " into Iss4_member_profile (client_id, profile_id, base_id, my_order, field_text) values " + "(" + clientId + "," + profileId + ",'" + baseId + "'," + myOrder + ",'" + fieldText + "')";
				}
                
                if (myOrderCount==15)
				{
                    sqlstrOptimized = sqlstrOptimized + " SELECT 1 FROM DUAL";
                OracleDBOperations dbOp = new OracleDBOperations();
                bool result = dbOp.ExecuteQueryforOracle(sqlstrOptimized);
				sqlstrOptimized="";
                myOrderCount = 0;
                return result;
				}
				else
				{
				return true;
				}
            }
            catch (Exception Exception){
                throw;
            }

        }

        public bool IsExistProfileOptimized(string clientId, string baseId)
        {
            try
            {
                string count = string.Empty;
                bool isExist = false;
                if (string.IsNullOrEmpty(baseId.Trim()) || baseId.ToString().Equals("0"))
                {
                    return true;
                }
                string sqlStr = "select count(*) as numbers from  Iss4_member_profile where client_id=:CLIENT_ID  and base_id=:BASEID and profile_id=:profile_id";
                Hashtable paramList = new Hashtable();
                paramList.Add(":CLIENT_ID", clientId);
                //paramList.Add(":my_order", myorder);
                paramList.Add(":BASEID", baseId);
                paramList.Add(":profile_id", "12");
                OracleDBOperations dbOp = new OracleDBOperations();
                count = dbOp.GetOracleResult(sqlStr, paramList);
                isExist = Convert.ToInt32(count) > 0 ? true : false;
                return isExist;
            }
            catch (Exception ex)
            {
                return false;
            }

        }
        public string GetMemberID(string clientId, string baseId)
        {
            string memberId = string.Empty;
            if (string.IsNullOrEmpty(baseId.Trim()) || baseId.ToString().Equals("0"))
            {
                return memberId;
            }
            string sqlStr = "select NVL(member_id,'') from iss3_members where client_id = :CLIENT_ID and role_id IN (1,6) and  base_id= :BASEID";
            Hashtable paramList = new Hashtable();
            paramList.Add(":CLIENT_ID", clientId);
            paramList.Add(":BASEID", baseId);
            OracleDBOperations dbOp = new OracleDBOperations();
            memberId = dbOp.GetOracleResult(sqlStr, paramList);
            return memberId;

        }
        public string checkbaseIdExistsforPlanYear(string clientId, string baseId, string planYear)
        {
            string memberId = string.Empty;
            if (string.IsNullOrEmpty(baseId.Trim()) || baseId.ToString().Equals("0"))
            {
                return memberId;
            }
            string sqlStr = "select employee_id from SEV_EMP_DETAILS  where employee_id= '" + baseId+"'";
            
            SqlDBOperations dbOp = new SqlDBOperations();
            memberId = dbOp.GetSqlResult(sqlStr);
            return memberId;

        }

        public string checkbaseIdExistsforPlanYear1(string clientId, string baseId, string planYear)
        {
            string memberId = string.Empty;
            if (string.IsNullOrEmpty(baseId.Trim()) || baseId.ToString().Equals("0"))
            {
                return memberId;
            }
            string sqlStr = "select employee_id from SEV_MASTER_LIST  where employee_id= '" + baseId + "'";

            SqlDBOperations dbOp = new SqlDBOperations();
            memberId = dbOp.GetSqlResult(sqlStr);
            return memberId;

        }

        public string GetNextSeqNumber(string sequence)
        {
            string docSeqNo = string.Empty;
            string sqlStr = "SELECT " + sequence.Trim() + ".NEXTVAL FROM dual";
            OracleDBOperations dbOp = new OracleDBOperations();
            docSeqNo = dbOp.GetOracleResult(sqlStr);
            return docSeqNo;
        }



        public  string RemoveSpecialCharacters(string str)
        {
            string cleanString = string.Empty;
            try
            {
                //cleanString = Regex.Replace(str, @"[^a-zA-Z0-9.@_:,'\s\-\/]", " ").Trim();
                cleanString = Regex.Replace(str, @"^-", "").Trim();
            }
            catch(Exception ex)
            {
                cleanString = string.Empty;
            }
            return cleanString;
            //return Regex.Replace(str, "[^a-zA-Z0-9_.',- ]+", " ", RegexOptions.Compiled);
        }
        public string RemoveSpecialCharactersBaseId(string str)
        {
            string cleanString = string.Empty;
            try
            {
                cleanString = Regex.Replace(str, @"[^a-zA-Z0-9\s]", "").Trim();
                cleanString = cleanString.Replace(" ", "");
               // cleanString = Regex.Replace(cleanString, @"^-", "").Trim();
            }
            catch (Exception ex)
            {
                cleanString = string.Empty;
            }
            return cleanString;
            //return Regex.Replace(str, "[^a-zA-Z0-9_.',- ]+", " ", RegexOptions.Compiled);
        }

        public bool InsertUser( string clientId,Hashtable ht,string memberId)
        {
           
            try
            {
                string first_name = string.Empty;
                //string[] chars = new string[] { ",", ".", "/", "!", "@", "#", "$", "%", "^", "&", "*", "'", "\"", ";", "_", "(", ")", ":", "|", "[", "]" };
                //first_name = ht["FirstName"].ToString();
                first_name = RemoveSpecialCharacters(ht["FirstName"].ToString());
                if (first_name.Length > 50)
                {
                    first_name = first_name.Remove(50);
                }               

                string LastName = string.Empty;
                LastName = RemoveSpecialCharacters( ht["LastName"].ToString());
                if (LastName.Length > 36)
                {
                    LastName = LastName.Remove(36);
                }
                
                string City = string.Empty;
                City = RemoveSpecialCharacters(ht["City"].ToString());
                if (City.Length > 20)
                {
                    City = City.Remove(20);
                }

                string Address1 = string.Empty;
                Address1 = RemoveSpecialCharacters(ht["Address1"].ToString());
                if (Address1.Length > 35)
                {
                    Address1 = Address1.Remove(35);
                }

                string Address2 = string.Empty;
                Address2 = RemoveSpecialCharacters(ht["Address2"].ToString());
                if (Address2.Length > 35)
                {
                    Address2 = Address2.Remove(35);
                }

                string State = string.Empty;
                State = RemoveSpecialCharacters(ht["State"].ToString());
                if (State.Length > 2)
                {
                    State = State.Remove(2);
                }

                string Postal_Code = string.Empty;
                Postal_Code = RemoveSpecialCharacters(ht["Postal_Code"].ToString());
                if (Postal_Code.Length > 12)
                {
                    Postal_Code = Postal_Code.Remove(12);
                }

                string CountryCode = string.Empty;
                CountryCode = RemoveSpecialCharacters(ht["CountryCode"].ToString());
                if (CountryCode.Length > 3)
                {
                    CountryCode = CountryCode.Remove(3);
                }


                string Email = string.Empty;
                Email = ht["Email"].ToString();
                if (Email.Length > 128)
                {
                    Email = Email.Remove(128);
                }

                string LANGUAGE_ID = string.Empty;
                LANGUAGE_ID = ht["LANGUAGE_ID"].ToString();
                if (LANGUAGE_ID.Length > 2)
                {
                    LANGUAGE_ID = LANGUAGE_ID.Remove(2);
                }

                string sqlStr = "INSERT INTO ISS3_MEMBERS(MEMBER_ID ,  USER_ID,  ROLE_ID ,  CLIENT_ID ,  FIRST_NAME ,  LAST_NAME ,ACCESS_DISABLED, ACCESS_CONTROL_GROUP_ID ,  BASE_ID  ,  ADDR1,  ADDR2,  CITY,  STATE_CD ,  POSTAL_CD ,  COUNTRY_CD ,  LANGUAGE_ID,  EMAIL,  CREATION_DATE) VALUES ( :MEMBER_ID ,  :USER_ID,  :ROLE_ID ,  :CLIENT_ID ,  :FIRST_NAME ,  :LAST_NAME ,:ACCESS_DISABLED, :ACCESS_CONTROL_GROUP_ID ,  :BASE_ID  , :ADDR1,  :ADDR2,  :CITY,  :STATE_CD ,  :POSTAL_CD ,  :COUNTRY_CD ,  :LANGUAGE_ID,  :EMAIL,  SYSDATE)";
                Hashtable paramList = new Hashtable();
                paramList.Add(":MEMBER_ID", memberId);
                paramList.Add(":USER_ID", RemoveSpecialCharactersBaseId(ht["USER_ID"].ToString()));
                paramList.Add(":ROLE_ID", 1);
                paramList.Add(":CLIENT_ID", clientId);
                paramList.Add(":FIRST_NAME", first_name.Replace("'", "''"));                
                paramList.Add(":LAST_NAME", LastName.Replace("'", "''"));
                paramList.Add(":ACCESS_DISABLED", "N");
                paramList.Add(":ACCESS_CONTROL_GROUP_ID", 1);
                paramList.Add(":BASE_ID",RemoveSpecialCharactersBaseId(ht["BaseId"].ToString()));
                paramList.Add(":ADDR1", Address1.Replace("'", "''"));
                paramList.Add(":ADDR2", Address2.Replace("'", "''"));
                paramList.Add(":CITY", City.Replace("'", "''"));

                paramList.Add(":STATE_CD", State.Replace("'", "''"));
                paramList.Add(":POSTAL_CD", Postal_Code.Replace("'", "''"));
                paramList.Add(":COUNTRY_CD", CountryCode.Replace("'", "''"));
                paramList.Add(":LANGUAGE_ID", LANGUAGE_ID.Replace("'", "''"));
                paramList.Add(":EMAIL", Email.Replace("'", "''"));

                OracleDBOperations dbOp = new OracleDBOperations();
                bool result = dbOp.ExecuteQueryforOracle(sqlStr, paramList);
                if (result)
                {
                    InsertMemberSystems(clientId, memberId, ht["PASSWORD"].ToString(), "2");
                    InsertServiceIds(ht, clientId);
                }
              return result;
            }
            catch (Exception ex)
            {
                throw;
            }

        }
        public bool UpdateEmployeeDetailsBPR(string clientId, Hashtable ht, string memberId, string docNumber, string jobNumber, string creation_date, string logFile)
        {

            try
            {
                log = logFile;
                string SEGMENT = string.Empty;

                SEGMENT = RemoveSpecialCharacters(ht["SEGMENT"].ToString());
                if (SEGMENT.Length > 80)
                {
                    SEGMENT = SEGMENT.Remove(80);
                }
                string BUD = string.Empty;

                BUD = RemoveSpecialCharacters(ht["BUD"].ToString());
                if (BUD.Length > 80)
                {
                    BUD = BUD.Remove(80);
                }

                string BUD1 = string.Empty;

                BUD1 = RemoveSpecialCharacters(ht["BUD1"].ToString());
                if (BUD1.Length > 80)
                {
                    BUD1 = BUD1.Remove(80);
                }

                string EMPLOYEE_ID = string.Empty;
                EMPLOYEE_ID = RemoveSpecialCharacters(ht["EMPLOYEE_ID"].ToString());
                if (EMPLOYEE_ID.Length > 20)
                {
                    EMPLOYEE_ID = EMPLOYEE_ID.Remove(20);
                }



                string Last_Name = string.Empty;
                Last_Name = RemoveSpecialCharacters(ht["Last_Name"].ToString());
                if (Last_Name.Length > 50)
                {
                    Last_Name = Last_Name.Remove(50);
                }
                string First_Name = string.Empty;
                First_Name = RemoveSpecialCharacters(ht["First_Name"].ToString());
                if (First_Name.Length > 50)
                {
                    First_Name = First_Name.Remove(50);
                }
                string BIRTH_DATE = string.Empty;

                BIRTH_DATE = ht["BIRTH_DATE"].ToString();

                string Noti_Date = string.Empty;

                Noti_Date = ht["Noti_Date"].ToString();


                string Job_Title = string.Empty;
                Job_Title = ht["Job_Title"].ToString();
                if (Job_Title.Length > 50)
                {
                    Job_Title = Job_Title.Remove(50);
                }

                string GRADE_LEVEL = string.Empty;
                GRADE_LEVEL = ht["GRADE_LEVEL"].ToString();
                if (GRADE_LEVEL.Length > 20)
                {
                    GRADE_LEVEL = GRADE_LEVEL.Remove(20);
                }
                string Disp_Sep_Eff_Date = string.Empty;

                Disp_Sep_Eff_Date = ht["Disp_Sep_Eff_Date"].ToString();


                string p_BIRTH_DATE = ht["BIRTH_DATE"].ToString();
                UtilityClass utility = new UtilityClass();
                if (p_BIRTH_DATE == "")
                {
                    p_BIRTH_DATE = "NULL";
                }
                else
                {
                    p_BIRTH_DATE = ConvertToDateFormat(p_BIRTH_DATE);
                    if (p_BIRTH_DATE != null)
                    {
                        p_BIRTH_DATE = "CONVERT(date, '" + p_BIRTH_DATE + "', 23)";
                    }
                    else
                    {
                        p_BIRTH_DATE = "NULL";
                    }
                }
                string p_Noti_Date = ht[("Noti_Date")].ToString();
                if (p_Noti_Date == "")
                {
                    p_Noti_Date = "NULL";
                }
                else
                {
                    p_Noti_Date = ConvertToDateFormat(p_Noti_Date);
                    if (p_Noti_Date != null)
                    {

                        p_Noti_Date = "CONVERT(date, '" + p_Noti_Date + "', 23)";
                    }
                    else
                    {
                        p_Noti_Date = "NULL";
                    }
                }
                string p_Disp_Sep_Eff_Date = ht[("Disp_Sep_Eff_Date")].ToString();
                if (p_Disp_Sep_Eff_Date == "")
                {
                    p_Disp_Sep_Eff_Date = "NULL";
                }
                else
                {
                    p_Disp_Sep_Eff_Date = ConvertToDateFormat(p_Noti_Date);
                    if (p_Disp_Sep_Eff_Date != null)
                    {
                        p_Disp_Sep_Eff_Date = "CONVERT(date, '" + p_Disp_Sep_Eff_Date + "', 23)";
                    }
                    else
                    {
                        p_Disp_Sep_Eff_Date = "NULL";
                    }

                }



                if (creation_date == "")
                {
                    creation_date = "NULL";
                }
                else
                {
                    //p_Disp_Sep_Eff_Date = utility.FormatDate(clientId, "", p_Disp_Sep_Eff_Date);
                    creation_date = "CONVERT(date, '" + creation_date + "', 23)";
                }

                StringBuilder sb = new StringBuilder();
                string COMMA = ",";
                sb.Append("UPDATE SEV_BASE_POP_REPORT SET ")
                    .Append("SEGMENT='" + SEGMENT + "'")
                    .Append(COMMA)
                    .Append("BUD='" + BUD + "'")
                    .Append(COMMA)
                    .Append("BUD1='" + BUD1 + "'")
                    .Append(COMMA)
                    .Append("Last_Name='" + Last_Name + "'")
                    .Append(COMMA)
                    .Append("First_Name='" + First_Name + "'")
                    .Append(COMMA)
                    .Append("Job_Title='" + Job_Title + "'")
                    .Append(COMMA)
                    .Append("GRADE_LEVEL='" + GRADE_LEVEL + "'")
                    .Append(COMMA)
                    .Append("BIRTH_DATE=" + p_BIRTH_DATE + "")
                    .Append(COMMA)
                    .Append("Noti_Date=" + p_Noti_Date + "")
                    .Append(COMMA)
                    .Append("Disp_Sep_Eff_Date=" + p_Disp_Sep_Eff_Date + "")
                    .Append(COMMA)
                    .Append("creation_date=" + creation_date + "")
                    .Append(COMMA)
                    .Append("job_number='" + jobNumber + "'")
                    .Append(" WHERE EMPLOYEE_ID='" + EMPLOYEE_ID + "'");
                string sqlStr = sb.ToString();
                SqlDBOperations dbOp = new SqlDBOperations();
                dbOp.ExecuteQueryforSql(sqlStr);


                return true;

            }
            catch (Exception ex)
            {
                throw;
            }

        }

        public bool UpdateEmployeeDetailsML(string clientId, Hashtable ht, string memberId, string docNumber, string jobNumber, string creation_date, string logFile)
        {

            try
            {
                log = logFile;
                string Comit_ID = string.Empty;

                Comit_ID = RemoveSpecialCharacters(ht["Comit_ID"].ToString());
                if (Comit_ID.Length > 50)
                {
                    Comit_ID = Comit_ID.Remove(50);
                }
                string Rpt_to_Mgr_First_Name = string.Empty;

                Rpt_to_Mgr_First_Name = RemoveSpecialCharacters(ht["Rpt_to_Mgr_First_Name"].ToString());
                if (Rpt_to_Mgr_First_Name.Length > 50)
                {
                    Rpt_to_Mgr_First_Name = Rpt_to_Mgr_First_Name.Remove(50);
                }

                string Middle_Name = string.Empty;

                Middle_Name = RemoveSpecialCharacters(ht["Middle_Name"].ToString());
                if (Middle_Name.Length > 50)
                {
                    Middle_Name = Middle_Name.Remove(80);
                }

                string Employee_ID = string.Empty;
                Employee_ID = RemoveSpecialCharacters(ht["Employee_ID"].ToString());
                if (Employee_ID.Length > 20)
                {
                    Employee_ID = Employee_ID.Remove(20);
                }



                string Last_Name = string.Empty;
                Last_Name = RemoveSpecialCharacters(ht["Last_Name"].ToString());
                if (Last_Name.Length > 50)
                {
                    Last_Name = Last_Name.Remove(50);
                }
                string First_Name = string.Empty;
                First_Name = RemoveSpecialCharacters(ht["First_Name"].ToString());
                if (First_Name.Length > 50)
                {
                    First_Name = First_Name.Remove(50);
                }
                //string BIRTH_DATE = string.Empty;

                //BIRTH_DATE = ht["BIRTH_DATE"].ToString();

                string Noti_Date = string.Empty;

                Noti_Date = ht["Noti_Date"].ToString();


                string Gender = string.Empty;
                Gender = ht["Gender"].ToString();
                if (Gender.Length > 20)
                {
                    Gender = Gender.Remove(20);
                }

                string Grade_Level = string.Empty;
                Grade_Level = ht["Grade_Level"].ToString();
                if (Grade_Level.Length > 20)
                {
                    Grade_Level = Grade_Level.Remove(20);
                }
                string Separation_Eff_Date = string.Empty;

                Separation_Eff_Date = ht["Separation_Eff_Date"].ToString();


               // string p_BIRTH_DATE = ht["BIRTH_DATE"].ToString();
                UtilityClass utility = new UtilityClass();


                string p_Noti_Date = ht[("Noti_Date")].ToString();
                if (p_Noti_Date == "")
                {
                    p_Noti_Date = "NULL";
                }
                else
                {
                    p_Noti_Date = ConvertToDateFormat(p_Noti_Date);

                    if (p_Noti_Date != null)
                    {
                        p_Noti_Date = "CONVERT(date, '" + p_Noti_Date + "', 23)";
                    }
                    else
                    {
                        p_Noti_Date = "NULL";
                    }
                }
                string p_Separation_Eff_Date = ht[("Separation_Eff_Date")].ToString();
                if (p_Separation_Eff_Date == "")
                {
                    p_Separation_Eff_Date = "NULL";
                }
                else
                {
                    p_Separation_Eff_Date = ConvertToDateFormat(p_Separation_Eff_Date);
                    if (p_Separation_Eff_Date != null)
                    {
                        p_Separation_Eff_Date = "CONVERT(date, '" + p_Separation_Eff_Date + "', 23)";
                    }
                    else
                    {
                        p_Separation_Eff_Date = "NULL";
                    }

                }
                string Total_Weeks = string.Empty;
                Total_Weeks = ht["Total_Weeks"].ToString();


                string Home_Address1 = string.Empty;
                Home_Address1 = ht["Home_Address1"].ToString();
                if (Home_Address1.Length > 200)
                {
                    Home_Address1 = Home_Address1.Remove(200);
                }
                string Home_Address2 = string.Empty;
                Home_Address2 = ht["Home_Address2"].ToString();
                if (Home_Address2.Length > 200)
                {
                    Home_Address2 = Home_Address2.Remove(200);
                }

                string Home_City = string.Empty;
                Home_City = ht["Home_City"].ToString();
                if (Home_City.Length > 50)
                {
                    Home_City = Home_City.Remove(50);
                }

                string Home_State = string.Empty;
                Home_State = ht["Home_State"].ToString();
                if (Home_State.Length > 50)
                {
                    Home_State = Home_State.Remove(50);
                }

                string Home_Postal_Code = string.Empty;
                Home_Postal_Code = ht["Home_Postal_Code"].ToString();
                if (Home_Postal_Code.Length > 50)
                {
                    Home_Postal_Code = Home_Postal_Code.Remove(50);
                }

                string Segment = string.Empty;
                Segment = ht["Segment"].ToString();
                if (Segment.Length > 50)
                {
                    Segment = Segment.Remove(50);
                }

                if (creation_date == "")
                {
                    creation_date = "NULL";
                }
                else
                {
                    //p_Disp_Sep_Eff_Date = utility.FormatDate(clientId, "", p_Disp_Sep_Eff_Date);
                    creation_date = "CONVERT(date, '" + creation_date + "', 23)";
                }

                StringBuilder sb = new StringBuilder();
                //string COMMA = ",";
                sb.Append("UPDATE SEV_MASTER_LIST SET ")
                  .Append("Comit_ID = '" + Comit_ID + "', ")
                  .Append("Rpt_to_Mgr_First_Name = '" + Rpt_to_Mgr_First_Name + "', ")
                  .Append("Last_Name = '" + Last_Name + "', ")
                  .Append("First_Name = '" + First_Name + "', ")
                  .Append("Middle_Name = '" + Middle_Name + "', ")
                  .Append("Grade_Level = '" + Grade_Level + "', ")
                  .Append("Gender = '" + Gender + "', ")
                  .Append("Noti_Date = " + p_Noti_Date + ", ")
                  .Append("Separation_Eff_Date = " + p_Separation_Eff_Date + ", ")
                  .Append("Total_Weeks = " + Total_Weeks + ", ")
                  .Append("Home_Address1 = '" + Home_Address1 + "', ")
                  .Append("Home_Address2 = '" + Home_Address2 + "', ")
                  .Append("Home_City = '" + Home_City + "', ")
                  .Append("Home_State = '" + Home_State + "', ")
                  .Append("Home_Postal_Code = '" + Home_Postal_Code + "', ")
                  .Append("Segment = '" + Segment + "', ")
                  .Append("creation_date = " + creation_date + ", ")
                  .Append("job_number = '" + jobNumber + "' ")
                  .Append("WHERE Employee_ID = '" + Employee_ID + "'");

                string sqlStr = sb.ToString();
                SqlDBOperations dbOp = new SqlDBOperations();
                dbOp.ExecuteQueryforSql(sqlStr);

                return true;


            }
            catch (Exception ex)
            {
                throw;
            }

        }
        public bool UpdateEmployeeDetails(string clientId, Hashtable ht, string memberId, string docNumber, string jobNumber,string creation_date,string logFile)
        {

            try
            {
                log = logFile;
                alertAdded = false;
                string plan_year = string.Empty;

                plan_year = RemoveSpecialCharacters(ht["plan_year"].ToString());
                if (plan_year.Length > 4)
                {
                    plan_year = plan_year.Remove(4);
                }
                string worker_type = string.Empty;

                worker_type = RemoveSpecialCharacters(ht["worker_type"].ToString());
                if (worker_type.Length > 30)
                {
                    worker_type = worker_type.Remove(30);
                }

                string employee_type = string.Empty;

                employee_type = RemoveSpecialCharacters(ht["employee_type"].ToString());
                if (employee_type.Length > 30)
                {
                    employee_type = employee_type.Remove(30);
                }

                string employee_id = string.Empty;
                employee_id = RemoveSpecialCharacters(ht["employee_id"].ToString());
                if (employee_id.Length > 20)
                {
                    employee_id = employee_id.Remove(20);
                }



                string legal_name = string.Empty;
                legal_name = RemoveSpecialCharacters(ht["legal_name"].ToString());
                if (legal_name.Length > 100)
                {
                    legal_name = legal_name.Remove(100);
                }
                string DOB = string.Empty;

                DOB = ht["DOB"].ToString();

                string continous_serv_dt = string.Empty;

                continous_serv_dt = ht["continous_serv_dt"].ToString();
                string ret_elig_dt = string.Empty;

                ret_elig_dt = ht["ret_elig_dt"].ToString();

                string tot_base_pay_annual = string.Empty;
                tot_base_pay_annual = ht["tot_base_pay_annual"].ToString();
                if (tot_base_pay_annual.Length > 13)
                {
                    tot_base_pay_annual = tot_base_pay_annual.Remove(13);
                }

                string comp_plan_salary_plans = string.Empty;
                comp_plan_salary_plans = ht["comp_plan_salary_plans"].ToString();
                if (comp_plan_salary_plans.Length > 50)
                {
                    comp_plan_salary_plans = comp_plan_salary_plans.Remove(50);
                }

                string amount_salary_plans = string.Empty;
                amount_salary_plans = ht["amount_salary_plans"].ToString();
                if (amount_salary_plans.Length > 13)
                {
                    amount_salary_plans = amount_salary_plans.Remove(13);
                }

                string comp_plan_hourly_plans = string.Empty;
                comp_plan_hourly_plans = RemoveSpecialCharacters(ht["comp_plan_hourly_plans"].ToString());
                if (comp_plan_hourly_plans.Length > 50)
                {
                    comp_plan_hourly_plans = comp_plan_hourly_plans.Remove(50);
                }


                string amount_hourly_plans = string.Empty;
                amount_hourly_plans = ht["amount_hourly_plans"].ToString();
                if (amount_hourly_plans.Length > 13)
                {
                    amount_hourly_plans = amount_hourly_plans.Remove(13);
                }

                string bonus_plans_assmt_dtls = string.Empty;
                bonus_plans_assmt_dtls = ht["bonus_plans_assmt_dtls"].ToString();
                if (bonus_plans_assmt_dtls.Length > 100)
                {
                    bonus_plans_assmt_dtls = bonus_plans_assmt_dtls.Remove(100);
                }

                string schd_weekly_hrs = string.Empty;
                schd_weekly_hrs = ht["schd_weekly_hrs"].ToString();
                if (schd_weekly_hrs.Length > 6)
                {
                    schd_weekly_hrs = schd_weekly_hrs.Remove(6);
                }

                string comp_grade = string.Empty;
                comp_grade = ht["comp_grade"].ToString();
                if (comp_grade.Length > 69)
                {
                    comp_grade = comp_grade.Remove(69);
                }
                string comp_grade_profile = string.Empty;
                comp_grade_profile = ht["comp_grade_profile"].ToString();
                if (comp_grade_profile.Length > 30)
                {
                    comp_grade_profile = comp_grade_profile.Remove(30);
                }
                string business_unit = string.Empty;
                business_unit = ht["business_unit"].ToString();
                if (business_unit.Length > 49)
                {
                    business_unit = business_unit.Remove(49);
                }

                string cost_center = string.Empty;
                cost_center = ht["cost_center"].ToString();
                if (cost_center.Length > 69)
                {
                    cost_center = cost_center.Remove(69);
                }


                string time_type = string.Empty;
                time_type = ht["time_type"].ToString();
                if (time_type.Length > 19)
                {
                    time_type = time_type.Remove(19);
                }


                string job_code = string.Empty;
                job_code = ht["job_code"].ToString();
                if (job_code.Length > 19)
                {
                    job_code = job_code.Remove(19);
                }


                string location = string.Empty;
                location = ht["location"].ToString();
                if (location.Length > 99)
                {
                    location = location.Remove(99);
                }


                string company_origin_reg = string.Empty;
                company_origin_reg = ht["company_origin_reg"].ToString();
                if (company_origin_reg.Length > 49)
                {
                    company_origin_reg = company_origin_reg.Remove(49);
                }


                string home_addr1 = string.Empty;
                home_addr1 = ht["home_addr1"].ToString();
                if (home_addr1.Length > 100)
                {
                    home_addr1 = home_addr1.Remove(100);
                }
                string home_addr2 = string.Empty;
                home_addr2 = ht["home_addr2"].ToString();
                if (home_addr2.Length > 100)
                {
                    home_addr2 = home_addr2.Remove(100);
                }

                string home_addr3 = string.Empty;
                home_addr3 = ht["home_addr3"].ToString();
                if (home_addr3.Length > 100)
                {
                    home_addr3 = home_addr3.Remove(100);
                }
                string phone_home = string.Empty;
                phone_home = ht["phone_home"].ToString();
                if (phone_home.Length > 19)
                {
                    phone_home = phone_home.Remove(19);
                }


                string p_DOB = ht["DOB"].ToString();
                if (p_DOB == "")
                {
                    p_DOB = "NULL";
                }
                else
                {
                    p_DOB = "TO_DATE(replace(replace('" + p_DOB + "', ',', ''), ' ', '') , 'MM/DD/YY')";
                }
                string p_CONTINOUS_SERV_DT = ht[("CONTINOUS_SERV_DT").ToLower()].ToString();
                if (p_CONTINOUS_SERV_DT == "")
                {
                    p_CONTINOUS_SERV_DT = "NULL";
                }
                else
                {
                    p_CONTINOUS_SERV_DT = "TO_DATE(replace(replace('" + p_CONTINOUS_SERV_DT + "', ',', ''), ' ', '') , 'MM/DD/YY')";
                }
                string p_RET_ELIG_DT = ht[("RET_ELIG_DT").ToLower()].ToString();
                if (p_RET_ELIG_DT == "")
                {
                    p_RET_ELIG_DT = "NULL";
                }
                else
                {
                    p_RET_ELIG_DT = "TO_DATE(replace(replace('" + p_RET_ELIG_DT + "', ',', ''), ' ', '') , 'MM/DD/YY')";
                }

                string p_TOT_BASE_PAY_ANNUAL = ht[("TOT_BASE_PAY_ANNUAL").ToLower()].ToString();
                if (p_TOT_BASE_PAY_ANNUAL == "")
                {
                    p_TOT_BASE_PAY_ANNUAL = "NULL";
                }
                else
                {
                    p_TOT_BASE_PAY_ANNUAL = ht[("TOT_BASE_PAY_ANNUAL").ToLower()].ToString().Replace(",", "");
                }
                string p_AMOUNT_SALARY_PLANS = ht[("AMOUNT_SALARY_PLANS").ToLower()].ToString();
                if (p_AMOUNT_SALARY_PLANS == "")
                {
                    p_AMOUNT_SALARY_PLANS = "NULL";
                }
                else
                {
                    p_AMOUNT_SALARY_PLANS = ht[("AMOUNT_SALARY_PLANS").ToLower()].ToString().Replace(",", "");
                }
                string p_AMOUNT_HOURLY_PLANS = ht[("AMOUNT_HOURLY_PLANS").ToLower()].ToString();
                if (p_AMOUNT_HOURLY_PLANS == "")
                {
                    p_AMOUNT_HOURLY_PLANS = "NULL";
                }
                else
                {
                    p_AMOUNT_HOURLY_PLANS = ht[("AMOUNT_HOURLY_PLANS").ToLower()].ToString().Replace(",", "");
                }
                string p_SCHD_WEEKLY_HRS = ht[("SCHD_WEEKLY_HRS").ToLower()].ToString();
                if (p_SCHD_WEEKLY_HRS == "")
                {
                    p_SCHD_WEEKLY_HRS = "NULL";
                }
                else
                {
                    p_SCHD_WEEKLY_HRS = ht[("SCHD_WEEKLY_HRS").ToLower()].ToString().Replace(",", "");
                }


                if (creation_date == "")
                {
                    creation_date = "NULL";
                }
                else
                {
                    creation_date = "TO_DATE(replace(replace('" + creation_date + "', ',', ''), ' ', '') , 'MM/DD/YY')";
                }
                string sqlchkCalculated = "select count(*) from ISS4_BNYM_EMP_DETAILS where  client_id=" + clientId + " and employee_id=" + employee_id +"and NOFE is not null";
                OracleDBOperations dataOp = new OracleDBOperations();
                DataSet ds = new DataSet();
                ds = dataOp.GetDSResult(sqlchkCalculated);
                int calculated_count = Convert.ToInt32(ds.Tables[0].Rows[0][0]);
                //check whether audit record for address changes to employees needs to be inserted//based on whether package doc exist for member
                string package_doc_id = string.Empty;
                package_doc_id = ConfigurationManager.AppSettings["package_doc_id"].ToString().Trim();
               
                string sqlchkpackageExists = "select count(*) from iss3_memdoc a, iss3_members b where  b.client_id=" + clientId + " and b.base_id='" + employee_id + "' and b.member_id = a.member_id and   a.doc_id = "+package_doc_id;
                OracleDBOperations dataOp1 = new OracleDBOperations();
                DataSet ds1 = new DataSet();
                ds1 = dataOp1.GetDSResult(sqlchkpackageExists);
                int package_count = Convert.ToInt32(ds1.Tables[0].Rows[0][0]);
                //if (employee_id == "60050235")
                //{
                //    int i = 0;
                //}
                if (package_count > 0)
                {   

                        //calculation related fields//Audit table is inserted with FAIL ie BNYM table is not updated
                        Insert_audit_if_changed_fail("CONTINOUS_SERV_DT", p_CONTINOUS_SERV_DT, employee_id, clientId, docNumber, creation_date);
                        Insert_audit_if_changed_fail("RET_ELIG_DT", p_RET_ELIG_DT, employee_id, clientId, docNumber, creation_date);
                        Insert_audit_if_changed_fail("TOT_BASE_PAY_ANNUAL", p_TOT_BASE_PAY_ANNUAL, employee_id, clientId, docNumber, creation_date);
                        Insert_audit_if_changed_fail("COMP_PLAN_SALARY_PLANS", comp_plan_salary_plans, employee_id, clientId, docNumber, creation_date);
                        Insert_audit_if_changed_fail("AMOUNT_SALARY_PLANS", p_AMOUNT_SALARY_PLANS, employee_id, clientId, docNumber, creation_date);
                        Insert_audit_if_changed_fail("COMP_PLAN_HOURLY_PLANS", comp_plan_hourly_plans, employee_id, clientId, docNumber, creation_date);
                        Insert_audit_if_changed_fail("AMOUNT_HOURLY_PLANS", p_AMOUNT_HOURLY_PLANS, employee_id, clientId, docNumber, creation_date);
                        Insert_audit_if_changed_fail("BONUS_PLANS_ASSMT_DTLS", bonus_plans_assmt_dtls, employee_id, clientId, docNumber, creation_date);
                        Insert_audit_if_changed_fail("SCHD_WEEKLY_HRS", p_SCHD_WEEKLY_HRS, employee_id, clientId, docNumber, creation_date);
                        Insert_audit_if_changed_fail("COMP_GRADE", comp_grade, employee_id, clientId, docNumber, creation_date);
                        Insert_audit_if_changed_fail("COMP_GRADE_PROFILE", comp_grade_profile, employee_id, clientId, docNumber, creation_date);


                    //Address and other fields//Audit table is inserted with PASS ie BNYM table is  updated
                    Insert_audit_if_changed("WORKER_TYPE", worker_type, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("EMPLOYEE_TYPE", employee_type, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("LEGAL_NAME", legal_name, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("DOB", p_DOB, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("business_unit", business_unit, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("cost_center", cost_center, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("time_type", time_type, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("job_code", job_code, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("location", location, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("company_origin_reg", company_origin_reg, employee_id, clientId, docNumber, creation_date);
                    string strAddrCh = "select count(*) from iss4_BNYM_data_changes where   client_id=" + clientId + " and employee_id=" + employee_id + " and wave=" + docNumber + " and field_name in ('home_addr1','home_addr2','home_addr3')";
                    ds = dataOp.GetDSResult(strAddrCh);
                    string countAddrChB4 = ds.Tables[0].Rows[0][0].ToString();
                    Insert_audit_if_changed("home_addr1", home_addr1, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("home_addr2", home_addr2, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("home_addr3", home_addr3, employee_id, clientId, docNumber, creation_date);
                    
                    ds = dataOp.GetDSResult(strAddrCh);
                    string countAddrChAFTER = ds.Tables[0].Rows[0][0].ToString();
                    //Insert Notes
                    if (countAddrChB4 != countAddrChAFTER)
                    {
                        Insert_notes("home_addr3", home_addr3, employee_id, clientId, docNumber, creation_date);
                    }

                    Insert_audit_if_changed("phone_home", phone_home, employee_id, clientId, docNumber, creation_date);

                    StringBuilder sb = new StringBuilder();
                    string COMMA = ",";
                    sb.Append("UPDATE ISS4_BNYM_EMP_DETAILS SET ")
                            .Append("WORKER_TYPE='" + worker_type + "'")
                            .Append(COMMA)
                            .Append("EMPLOYEE_TYPE='" + employee_type + "'")
                            .Append(COMMA)
                            .Append("LEGAL_NAME='" + legal_name + "'")
                            .Append(COMMA)
                            .Append("DOB=" + p_DOB)
                            .Append(COMMA)
                            .Append("BUSINESS_UNIT='" + business_unit + "'")
                            .Append(COMMA)
                            .Append("COST_CENTER='" + cost_center + "'")
                            .Append(COMMA)
                            .Append("TIME_TYPE='" + time_type + "'")
                            .Append(COMMA)
                            .Append("JOB_CODE='" + job_code + "'")
                            .Append(COMMA)
                            .Append("LOCATION='" + location + "'")
                            .Append(COMMA)
                            .Append("COMPANY_ORIGIN_REG='" + company_origin_reg + "'")
                             .Append(COMMA)
                            .Append("HOME_ADDR1='" + home_addr1 + "'")
                            .Append(COMMA)
                            .Append("HOME_ADDR2='" + home_addr2 + "'")
                            .Append(COMMA)
                            .Append("HOME_ADDR3='" + home_addr3 + "'")
                            .Append(COMMA)
                            .Append("PHONE_HOME='" + phone_home + "'")
                            .Append(COMMA)
                            .Append("revised_imported_date=" + creation_date)
                            .Append(COMMA)
                            .Append("JOB_NUMBER_EXE1=" + jobNumber)
                            .Append(COMMA)
                            .Append("UPLOADED_DOCNUMBER=" + docNumber)
                            .Append(" where client_id=" + clientId + " and plan_year=" + plan_year + " and employee_id=" + employee_id);

                    string sqlStr = sb.ToString();
                    OracleDBOperations dbOp = new OracleDBOperations();
                    bool result = dbOp.ExecuteQueryforOracle(sqlStr);
                    return result;

               
                    
               }
                if (package_count == 0 && calculated_count>0)
                {

                    //calculation related fields//Audit table is inserted with FAIL ie BNYM table is not updated
                    Insert_audit_if_changed_fail("CONTINOUS_SERV_DT", p_CONTINOUS_SERV_DT, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_fail("RET_ELIG_DT", p_RET_ELIG_DT, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_fail("TOT_BASE_PAY_ANNUAL", p_TOT_BASE_PAY_ANNUAL, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_fail("COMP_PLAN_SALARY_PLANS", comp_plan_salary_plans, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_fail("AMOUNT_SALARY_PLANS", p_AMOUNT_SALARY_PLANS, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_fail("COMP_PLAN_HOURLY_PLANS", comp_plan_hourly_plans, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_fail("AMOUNT_HOURLY_PLANS", p_AMOUNT_HOURLY_PLANS, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_fail("BONUS_PLANS_ASSMT_DTLS", bonus_plans_assmt_dtls, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_fail("SCHD_WEEKLY_HRS", p_SCHD_WEEKLY_HRS, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_fail("COMP_GRADE", comp_grade, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_fail("COMP_GRADE_PROFILE", comp_grade_profile, employee_id, clientId, docNumber, creation_date);


                    //Address and other fields//Audit table is inserted with PASS ie BNYM table is  updated
                    Insert_audit_if_changed("WORKER_TYPE", worker_type, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("EMPLOYEE_TYPE", employee_type, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("LEGAL_NAME", legal_name, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("DOB", p_DOB, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("business_unit", business_unit, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("cost_center", cost_center, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("time_type", time_type, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("job_code", job_code, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("location", location, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("company_origin_reg", company_origin_reg, employee_id, clientId, docNumber, creation_date);
                    string strAddrCh = "select count(*) from iss4_BNYM_data_changes where   client_id=" + clientId + " and employee_id=" + employee_id + " and wave=" + docNumber + " and field_name in ('home_addr1','home_addr2','home_addr3')";
                    ds = dataOp.GetDSResult(strAddrCh);
                    string countAddrChB4 = ds.Tables[0].Rows[0][0].ToString();
                    Insert_audit_if_changed_with_no_alert("home_addr1", home_addr1, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_with_no_alert("home_addr2", home_addr2, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_with_no_alert("home_addr3", home_addr3, employee_id, clientId, docNumber, creation_date);

                    ds = dataOp.GetDSResult(strAddrCh);
                    string countAddrChAFTER = ds.Tables[0].Rows[0][0].ToString();
                    //Insert Notes
                    if (countAddrChB4 != countAddrChAFTER)
                    {
                        Insert_notes("home_addr3", home_addr3, employee_id, clientId, docNumber, creation_date);
                    }

                    Insert_audit_if_changed("phone_home", phone_home, employee_id, clientId, docNumber, creation_date);

                    StringBuilder sb = new StringBuilder();
                    string COMMA = ",";
                    sb.Append("UPDATE ISS4_BNYM_EMP_DETAILS SET ")
                            .Append("WORKER_TYPE='" + worker_type + "'")
                            .Append(COMMA)
                            .Append("EMPLOYEE_TYPE='" + employee_type + "'")
                            .Append(COMMA)
                            .Append("LEGAL_NAME='" + legal_name + "'")
                            .Append(COMMA)
                            .Append("DOB=" + p_DOB)
                            .Append(COMMA)
                            .Append("BUSINESS_UNIT='" + business_unit + "'")
                            .Append(COMMA)
                            .Append("COST_CENTER='" + cost_center + "'")
                            .Append(COMMA)
                            .Append("TIME_TYPE='" + time_type + "'")
                            .Append(COMMA)
                            .Append("JOB_CODE='" + job_code + "'")
                            .Append(COMMA)
                            .Append("LOCATION='" + location + "'")
                            .Append(COMMA)
                            .Append("COMPANY_ORIGIN_REG='" + company_origin_reg + "'")
                             .Append(COMMA)
                            .Append("HOME_ADDR1='" + home_addr1 + "'")
                            .Append(COMMA)
                            .Append("HOME_ADDR2='" + home_addr2 + "'")
                            .Append(COMMA)
                            .Append("HOME_ADDR3='" + home_addr3 + "'")
                            .Append(COMMA)
                            .Append("PHONE_HOME='" + phone_home + "'")
                            .Append(COMMA)
                            .Append("revised_imported_date=" + creation_date)
                            .Append(COMMA)
                            .Append("JOB_NUMBER_EXE1='" + jobNumber+"'")
                            .Append(COMMA)
                            .Append("UPLOADED_DOCNUMBER=" + docNumber)
                            .Append(" where client_id=" + clientId + " and plan_year=" + plan_year + " and employee_id=" + employee_id);

                    string sqlStr = sb.ToString();
                    OracleDBOperations dbOp = new OracleDBOperations();
                    bool result = dbOp.ExecuteQueryforOracle(sqlStr);
                    return result;

                }
                if (package_count== 0 && calculated_count==0)
                {


                    Insert_audit_if_changed("CONTINOUS_SERV_DT", p_CONTINOUS_SERV_DT, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("RET_ELIG_DT", p_RET_ELIG_DT, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("TOT_BASE_PAY_ANNUAL", p_TOT_BASE_PAY_ANNUAL, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("COMP_PLAN_SALARY_PLANS", comp_plan_salary_plans, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("AMOUNT_SALARY_PLANS", p_AMOUNT_SALARY_PLANS, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("COMP_PLAN_HOURLY_PLANS", comp_plan_hourly_plans, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("AMOUNT_HOURLY_PLANS", p_AMOUNT_HOURLY_PLANS, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("BONUS_PLANS_ASSMT_DTLS", bonus_plans_assmt_dtls, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("SCHD_WEEKLY_HRS", p_SCHD_WEEKLY_HRS, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("COMP_GRADE", comp_grade, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("COMP_GRADE_PROFILE", comp_grade_profile, employee_id, clientId, docNumber, creation_date);

                    Insert_audit_if_changed("business_unit", business_unit, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("cost_center", cost_center, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("time_type", time_type, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("job_code", job_code, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("location", location, employee_id, clientId, docNumber, creation_date);
                    
                    Insert_audit_if_changed("company_origin_reg", company_origin_reg, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_with_no_alert("home_addr1", home_addr1, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_with_no_alert("home_addr2", home_addr2, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed_with_no_alert("home_addr3", home_addr3, employee_id, clientId, docNumber, creation_date);
                    Insert_audit_if_changed("phone_home", phone_home, employee_id, clientId, docNumber, creation_date);

                    StringBuilder sb = new StringBuilder();
                    string COMMA = ",";
                    sb.Append("UPDATE ISS4_BNYM_EMP_DETAILS SET ")
                            .Append("WORKER_TYPE='" + worker_type + "'")
                            .Append(COMMA)
                            .Append("EMPLOYEE_TYPE='" + employee_type + "'")
                            .Append(COMMA)
                            .Append("LEGAL_NAME='" + legal_name + "'")
                            .Append(COMMA)
                            .Append("DOB=" + p_DOB)
                            .Append(COMMA)
                            .Append("CONTINOUS_SERV_DT=" + p_CONTINOUS_SERV_DT)
                            .Append(COMMA)
                            .Append("RET_ELIG_DT=" + p_RET_ELIG_DT)
                            .Append(COMMA)
                            .Append("TOT_BASE_PAY_ANNUAL=" + p_TOT_BASE_PAY_ANNUAL)
                            .Append(COMMA)
                            .Append("COMP_PLAN_SALARY_PLANS='" + comp_plan_salary_plans + "'")
                            .Append(COMMA)
                            .Append("AMOUNT_SALARY_PLANS=" + p_AMOUNT_SALARY_PLANS)
                            .Append(COMMA)
                            .Append("COMP_PLAN_HOURLY_PLANS='" + comp_plan_hourly_plans + "'")
                            .Append(COMMA)
                            .Append("AMOUNT_HOURLY_PLANS=" + p_AMOUNT_HOURLY_PLANS)
                            .Append(COMMA)
                            .Append("BONUS_PLANS_ASSMT_DTLS='" + bonus_plans_assmt_dtls + "'")
                            .Append(COMMA)
                            .Append("SCHD_WEEKLY_HRS=" + p_SCHD_WEEKLY_HRS)
                            .Append(COMMA)
                            .Append("COMP_GRADE='" + comp_grade + "'")
                            .Append(COMMA)
                            .Append("COMP_GRADE_PROFILE='" + comp_grade_profile + "'")
                            .Append(COMMA)
                            .Append("BUSINESS_UNIT='" + business_unit + "'")
                            .Append(COMMA)
                            .Append("COST_CENTER='" + cost_center + "'")
                            .Append(COMMA)
                            .Append("TIME_TYPE='" + time_type + "'")
                            .Append(COMMA)
                            .Append("JOB_CODE='" + job_code + "'")
                            .Append(COMMA)
                            .Append("LOCATION='" + location + "'")
                            .Append(COMMA)
                            .Append("COMPANY_ORIGIN_REG='" + company_origin_reg + "'")
                             .Append(COMMA)
                            .Append("HOME_ADDR1='" + home_addr1 + "'")
                            .Append(COMMA)
                            .Append("HOME_ADDR2='" + home_addr2 + "'")
                            .Append(COMMA)
                            .Append("HOME_ADDR3='" + home_addr3 + "'")
                            .Append(COMMA)
                            .Append("PHONE_HOME='" + phone_home + "'")
                            .Append(COMMA)
                            .Append("revised_imported_date=" + creation_date)
                            .Append(COMMA)
                            .Append("JOB_NUMBER_EXE1=" + jobNumber)
                            .Append(COMMA)
                            .Append("UPLOADED_DOCNUMBER=" + docNumber)
                            .Append(" where client_id=" + clientId + " and plan_year=" + plan_year + " and employee_id=" + employee_id);

                    string sqlStr = sb.ToString();
                    OracleDBOperations dbOp = new OracleDBOperations();
                    bool result = dbOp.ExecuteQueryforOracle(sqlStr);
                    return result;

                }

                return true;  
            }
            catch (Exception ex)
            {
                throw;
            }

        }
        private void Insert_audit_if_changed_fail(string BNYM_column_name, string new_value, string employee_id, string clientId, string wave, string updated_date)
        {
            string sqlDynamic = "select " + BNYM_column_name + ",revised_imported_date from ISS4_BNYM_EMP_DETAILS where  client_id=" + clientId + " and employee_id=" + employee_id;
            OracleDBOperations dataOp = new OracleDBOperations();
            DataSet ds = new DataSet();
            ds = dataOp.GetDSResult(sqlDynamic);
            string old_column_value = ds.Tables[0].Rows[0][0].ToString();
            string revised_imported_date = ds.Tables[0].Rows[0][1].ToString();
            updated_date = updated_date.Replace(" 12:00:00 AM", "");
            if (revised_imported_date == "")
            {
                revised_imported_date = "NULL";
            }
            else
            {
                revised_imported_date = revised_imported_date.Replace(" 12:00:00 AM", "");
                revised_imported_date = "TO_DATE(replace(replace('" + revised_imported_date + "', ',', ''), ' ', '') , 'MM/DD/YY')";

            }
            if (BNYM_column_name.Trim().ToUpper().Contains("DATE") || BNYM_column_name.Trim().ToUpper().Contains("_DT") || BNYM_column_name.Trim().ToUpper().Contains("DOB"))
            {
                UtilityClass utility = new UtilityClass();
                old_column_value = utility.FormatDate(clientId, "", old_column_value);
                old_column_value = old_column_value.Replace(" 12:00:00 AM", "");
                new_value = new_value.Replace("TO_DATE(replace(replace('", "");
                new_value = new_value.Replace("', ',', ''), ' ', '') , 'MM/DD/YY')", "");
            }
            if (new_value.EndsWith(".00"))
            {

                new_value = new_value.TrimEnd('0');
                new_value = new_value.TrimEnd('0');
                new_value = new_value.TrimEnd('.');
            }
            if (old_column_value.EndsWith(".00"))
            {

                old_column_value = old_column_value.TrimEnd('0');
                old_column_value = old_column_value.TrimEnd('0');
                old_column_value = old_column_value.TrimEnd('.');
            }
            if (old_column_value.EndsWith(".0"))
            {

                old_column_value = old_column_value.TrimEnd('0');
                old_column_value = old_column_value.TrimEnd('.');
            }
            if (new_value.EndsWith(".0"))
            {

                new_value = new_value.TrimEnd('0');
                new_value = new_value.TrimEnd('.');
            }
            if (old_column_value.Contains("."))
            {
                old_column_value = old_column_value.TrimEnd('0');

            }

            if (new_value.Contains("."))
            {

                new_value = new_value.TrimEnd('0');
            }
            if (old_column_value != "" && old_column_value != "NULL" && old_column_value != "0")
            {
            if (old_column_value != new_value)
            {
                old_column_value = old_column_value.Replace("'", "''");
                new_value = new_value.Replace("'", "''");
                string sqlstr = "INSERT into iss4_BNYM_data_changes (client_id, pass_fail,employee_id, wave, field_name,updated_date, old_field_value,new_field_value) values ('" + clientId + "','" + "FAIL" + "','" + employee_id + "','" + wave + "','" + BNYM_column_name + "'," + updated_date + ",'" + old_column_value + "','" + new_value + "')";
                bool result = dataOp.ExecuteQueryforOracle(sqlstr);
            }
        }
        }
        private void Insert_audit_if_changed_with_no_alert(string BNYM_column_name, string new_value, string employee_id, string clientId, string wave, string updated_date)
        {
            string sqlDynamic = "select " + BNYM_column_name + ",revised_imported_date from ISS4_BNYM_EMP_DETAILS where  client_id=" + clientId + " and employee_id=" + employee_id;
            OracleDBOperations dataOp = new OracleDBOperations();
            DataSet ds = new DataSet();
            ds = dataOp.GetDSResult(sqlDynamic);
            string old_column_value = ds.Tables[0].Rows[0][0].ToString();
            string revised_imported_date = ds.Tables[0].Rows[0][1].ToString();
            updated_date = updated_date.Replace(" 12:00:00 AM", "");
            if (revised_imported_date == "")
            {
                revised_imported_date = "NULL";
            }
            else
            {
                revised_imported_date = revised_imported_date.Replace(" 12:00:00 AM", "");
                revised_imported_date = "TO_DATE(replace(replace('" + revised_imported_date + "', ',', ''), ' ', '') , 'MM/DD/YY')";

            }
            if (BNYM_column_name.Trim().ToUpper().Contains("DATE") || BNYM_column_name.Trim().ToUpper().Contains("_DT") || BNYM_column_name.Trim().ToUpper().Contains("DOB"))
            {
                UtilityClass utility = new UtilityClass();
                old_column_value = utility.FormatDate(clientId, "", old_column_value);
                old_column_value = old_column_value.Replace(" 12:00:00 AM", "");
                new_value = new_value.Replace("TO_DATE(replace(replace('", "");
                new_value = new_value.Replace("', ',', ''), ' ', '') , 'MM/DD/YY')", "");
            }
            if (new_value.EndsWith(".00"))
            {

                new_value = new_value.TrimEnd('0');
                new_value = new_value.TrimEnd('0');
                new_value = new_value.TrimEnd('.');
            }
            if (old_column_value.EndsWith(".00"))
            {

                old_column_value = old_column_value.TrimEnd('0');
                old_column_value = old_column_value.TrimEnd('0');
                old_column_value = old_column_value.TrimEnd('.');
            }
            if (old_column_value.EndsWith(".0"))
            {

                old_column_value = old_column_value.TrimEnd('0');
                old_column_value = old_column_value.TrimEnd('.');
            }
            if (new_value.EndsWith(".0"))
            {

                new_value = new_value.TrimEnd('0');
                new_value = new_value.TrimEnd('.');
            }
            if (old_column_value.Contains("."))
            {
                old_column_value = old_column_value.TrimEnd('0');

            }

            if (new_value.Contains("."))
            {

                new_value = new_value.TrimEnd('0');
            }
            if (old_column_value != "" && old_column_value != "NULL" && old_column_value != "0")
            {
                if (old_column_value != new_value)
                {
                    old_column_value = old_column_value.Replace("'", "''");
                    new_value = new_value.Replace("'", "''");
                    string sqlstr = "INSERT into iss4_BNYM_data_changes (client_id, employee_id, wave, field_name,updated_date, old_field_value,new_field_value) values ('" + clientId + "','" + employee_id + "','" + wave + "','" + BNYM_column_name + "'," + updated_date + ",'" + old_column_value + "','" + new_value + "')";
                    bool result = dataOp.ExecuteQueryforOracle(sqlstr);
                    

                }
            }
        }
        private void Insert_audit_if_changed(string BNYM_column_name, string new_value, string employee_id, string clientId, string wave, string updated_date)
        {
           
            string sqlDynamic = "select " + BNYM_column_name + ",revised_imported_date from ISS4_BNYM_EMP_DETAILS where  client_id=" + clientId + " and employee_id=" + employee_id;
            OracleDBOperations dataOp = new OracleDBOperations();
            DataSet ds = new DataSet();
            ds = dataOp.GetDSResult(sqlDynamic);
            string old_column_value = ds.Tables[0].Rows[0][0].ToString();
            string revised_imported_date = ds.Tables[0].Rows[0][1].ToString();
            string member_id = "";
            updated_date = updated_date.Replace(" 12:00:00 AM", "");
            if (revised_imported_date == "")
            {
                revised_imported_date = "NULL";
            }
            else
            {
                revised_imported_date = revised_imported_date.Replace(" 12:00:00 AM", "");
                revised_imported_date = "TO_DATE(replace(replace('" + revised_imported_date + "', ',', ''), ' ', '') , 'MM/DD/YY')";

            }
            if (BNYM_column_name.Trim().ToUpper().Contains("DATE") || BNYM_column_name.Trim().ToUpper().Contains("_DT") || BNYM_column_name.Trim().ToUpper().Contains("DOB"))
            {
                UtilityClass utility = new UtilityClass();
                old_column_value = utility.FormatDate(clientId, "", old_column_value);
                old_column_value = old_column_value.Replace(" 12:00:00 AM", "");
                new_value = new_value.Replace("TO_DATE(replace(replace('", "");
                new_value = new_value.Replace("', ',', ''), ' ', '') , 'MM/DD/YY')", "");
            }
            if (new_value.EndsWith(".00"))
            {

                new_value = new_value.TrimEnd('0');
                new_value = new_value.TrimEnd('0');
                new_value = new_value.TrimEnd('.');
            }
            if (old_column_value.EndsWith(".00"))
            {

                old_column_value = old_column_value.TrimEnd('0');
                old_column_value = old_column_value.TrimEnd('0');
                old_column_value = old_column_value.TrimEnd('.');
            }
            if (old_column_value.EndsWith(".0"))
            {

                old_column_value = old_column_value.TrimEnd('0');
                old_column_value = old_column_value.TrimEnd('.');
            }
            if (new_value.EndsWith(".0"))
            {

                new_value = new_value.TrimEnd('0');
                new_value = new_value.TrimEnd('.');
            }
            if (old_column_value.Contains("."))
            {
                old_column_value = old_column_value.TrimEnd('0');

            }

            if (new_value.Contains("."))
            {

                new_value = new_value.TrimEnd('0');
            }
            if (old_column_value != "" && old_column_value != "NULL" && old_column_value != "0")
            {
                if (old_column_value != new_value)
                {
                    old_column_value = old_column_value.Replace("'", "''");
                    new_value = new_value.Replace("'", "''");
                    string sqlstr = "INSERT into iss4_BNYM_data_changes (client_id, employee_id, wave, field_name,updated_date, old_field_value,new_field_value) values ('" + clientId + "','" + employee_id + "','" + wave + "','" + BNYM_column_name + "'," + updated_date + ",'" + old_column_value + "','" + new_value + "')";
                    bool result = dataOp.ExecuteQueryforOracle(sqlstr);
                    if (BNYM_column_name.Trim().ToUpper().Contains("HOME_ADDR"))
                    {
                        string sqlgetMemberId = "select member_id from iss3_members where  client_id=" + clientId + " and user_id='" + employee_id + "'";
                        ds = dataOp.GetDSResult(sqlgetMemberId);
                         member_id = ds.Tables[0].Rows[0][0].ToString();
                        string alert_id = string.Empty;
                        alert_id = ConfigurationManager.AppSettings["alert_id"].ToString().Trim();

                        if (alertAdded == false)
                        {
                            sqlstr = "delete from iss4_member_alerts  where client_id=" + clientId + "and member_id=" + member_id + " and alert_id=" + alert_id;
                            result = dataOp.ExecuteQueryforOracle(sqlstr);
                            sqlstr = "INSERT into iss4_member_alerts (client_id, member_id, alert_id) values ('" + clientId + "','" + member_id + "','" + alert_id + "'" + ")";
                            result = dataOp.ExecuteQueryforOracle(sqlstr);
                            Logs.WriteLog(log, "Address change alert added for Employee Id " + employee_id);
                            alertAdded = true;

                        }
                    }

                }
            }
            
        }
        private void Insert_notes(string BNYM_column_name, string new_value, string employee_id, string clientId, string wave, string updated_date)
        {
            if (employee_id == "50033580")
            {
                int i = 0;
            }
            OracleDBOperations dataOp = new OracleDBOperations();
            DataSet ds = new DataSet();
            string sqlgetMemberId = "select member_id from iss3_members where  client_id=" + clientId + " and user_id='" + employee_id + "'";
            ds = dataOp.GetDSResult(sqlgetMemberId);
            string member_id = ds.Tables[0].Rows[0][0].ToString();
           
                string strAddrCh = "select count(*) from iss4_BNYM_data_changes where   client_id=" + clientId + " and employee_id=" + employee_id + " and wave=" + wave + " and field_name in ('home_addr1','home_addr2','home_addr3')";
                ds = dataOp.GetDSResult(strAddrCh);
                string countAddrCh = ds.Tables[0].Rows[0][0].ToString();
                if (countAddrCh != "0")
                {
                    string strNoteText = "";
                    string strOldAddr = "";
                    string strNewAddr = "";
                    strNoteText = strNoteText + "************* Address Update during Census file";
                    strNoteText = strNoteText + Environment.NewLine;
                    strAddrCh = "select field_name,old_field_value,new_field_value from iss4_BNYM_data_changes   where  client_id=" + clientId + " and employee_id=" + employee_id + " and wave=" + wave + " and field_name in ('home_addr1','home_addr2','home_addr3')";
                    ds = dataOp.GetDSResult(strAddrCh);
                    DataTable dt = ds.Tables[0];
                    int totalRows = dt.Rows.Count;
                    strOldAddr = "Old Address";
                    strOldAddr = strOldAddr + Environment.NewLine;
                    for (int i = 0; i < totalRows; i++)
                    {

                        strOldAddr = strOldAddr + dt.Rows[i][0].ToString() + "  -  " + dt.Rows[i][1].ToString();
                        strOldAddr = strOldAddr + Environment.NewLine;


                    }
                    strOldAddr = strOldAddr + Environment.NewLine;
                    strNewAddr = "New Address";
                    strNewAddr = strNewAddr + Environment.NewLine;
                    for (int i = 0; i < totalRows; i++)
                    {

                        strNewAddr = strNewAddr + dt.Rows[i][0].ToString() + "  -  " + dt.Rows[i][2].ToString();
                        strNewAddr = strNewAddr + Environment.NewLine;

                    }

                    string sqlStrU = "select client_id,doc_name,doc_path,to_char(creation_date,'MM/DD/YYYY') as creation_date,user_id from iss3_memdoc where doc_number=" + wave;

                    DataSet dsU = dataOp.GetDSResult(sqlStrU);
                    string scsr = "10462";
                    string g_u_id = dsU.Tables[0].Rows[0][4].ToString();
                    string sval = strNoteText + strOldAddr + strNewAddr;
                    sval = sval.Replace("'", "''");
                    //sqlstr = "INSERT into iss4_member_alerts (client_id, member_id, alert_id) values ('" + clientId + "','" + member_id + "','" + alert_id + "'" + ")";
                    string sqlstr = "Insert into iss4_member_note (client_id, member_id, note_date, csr_id, note_text, user_id ) values (" +
            clientId + "," + member_id + ", sysdate , " + scsr + ",'" + sval + "', '" + g_u_id + "' )";
                    bool result = dataOp.ExecuteQueryforOracle(sqlstr);
                    Logs.WriteLog(log, "Address change note added for Employee Id " + employee_id);
                }
            
        }
        public bool InsertEmployeeDetailsML(string clientId, Hashtable ht, string memberId, string docNumber, string jobNumber, string creation_date)
        {

            try
            {
                string Comit_ID = string.Empty;

                Comit_ID = RemoveSpecialCharacters(ht["Comit_ID"].ToString());
                if (Comit_ID.Length > 50)
                {
                    Comit_ID = Comit_ID.Remove(50);
                }
                string Rpt_to_Mgr_First_Name = string.Empty;

                Rpt_to_Mgr_First_Name = RemoveSpecialCharacters(ht["Rpt_to_Mgr_First_Name"].ToString());
                if (Rpt_to_Mgr_First_Name.Length > 50)
                {
                    Rpt_to_Mgr_First_Name = Rpt_to_Mgr_First_Name.Remove(50);
                    Rpt_to_Mgr_First_Name = Rpt_to_Mgr_First_Name.Remove(50);
                }

                string Middle_Name = string.Empty;

                Middle_Name = RemoveSpecialCharacters(ht["Middle_Name"].ToString());
                if (Middle_Name.Length > 50)
                {
                    Middle_Name = Middle_Name.Remove(80);
                }

                string Employee_ID = string.Empty;
                Employee_ID = RemoveSpecialCharacters(ht["Employee_ID"].ToString());
                if (Employee_ID.Length > 20)
                {
                    Employee_ID = Employee_ID.Remove(20);
                }



                string Last_Name = string.Empty;
                Last_Name = RemoveSpecialCharacters(ht["Last_Name"].ToString());
                if (Last_Name.Length > 50)
                {
                    Last_Name = Last_Name.Remove(50);
                }
                string First_Name = string.Empty;
                First_Name = RemoveSpecialCharacters(ht["First_Name"].ToString());
                if (First_Name.Length > 50)
                {
                    First_Name = First_Name.Remove(50);
                }
                //string BIRTH_DATE = string.Empty;

                //BIRTH_DATE = ht["BIRTH_DATE"].ToString();

                string Noti_Date = string.Empty;

                Noti_Date = ht["Noti_Date"].ToString();


                string Gender = string.Empty;
                Gender = ht["Gender"].ToString();
                if (Gender.Length > 20)
                {
                    Gender = Gender.Remove(20);
                }

                string Grade_Level = string.Empty;
                Grade_Level = ht["Grade_Level"].ToString();
                if (Grade_Level.Length > 20)
                {
                    Grade_Level = Grade_Level.Remove(20);
                }
                string Separation_Eff_Date = string.Empty;

                Separation_Eff_Date = ht["Separation_Eff_Date"].ToString();


                //string p_BIRTH_DATE = ht["BIRTH_DATE"].ToString();
                UtilityClass utility = new UtilityClass();
               
                string p_Noti_Date = ht[("Noti_Date")].ToString();
                if (p_Noti_Date == "")
                {
                    p_Noti_Date = "NULL";
                }
                else
                {
                    p_Noti_Date = ConvertToDateFormat(p_Noti_Date);

                    if (p_Noti_Date != null)
                    {
                        p_Noti_Date = "CONVERT(date, '" + p_Noti_Date + "', 23)";
                    }
                    else
                    {
                        p_Noti_Date = "NULL";
                    }
                }
                string p_Separation_Eff_Date = ht[("Separation_Eff_Date")].ToString();
                if (p_Separation_Eff_Date == "")
                {
                    p_Separation_Eff_Date = "NULL";
                }
                else
                {
                    p_Separation_Eff_Date = ConvertToDateFormat(p_Separation_Eff_Date);
                    if (p_Separation_Eff_Date != null)
                    {
                        p_Separation_Eff_Date = "CONVERT(date, '" + p_Separation_Eff_Date + "', 23)";
                    }
                    else
                    {
                        p_Separation_Eff_Date = "NULL";
                    }

                }
                string Total_Weeks = string.Empty;
                Total_Weeks = ht["Total_Weeks"].ToString();
               

                string Home_Address1 = string.Empty;
                Home_Address1 = ht["Home_Address1"].ToString();
                if (Home_Address1.Length > 200)
                {
                    Home_Address1 = Home_Address1.Remove(200);
                }
                string Home_Address2 = string.Empty;
                Home_Address2 = ht["Home_Address2"].ToString();
                if (Home_Address2.Length > 200)
                {
                    Home_Address2 = Home_Address2.Remove(200);
                }

                string Home_City = string.Empty;
                Home_City = ht["Home_City"].ToString();
                if (Home_City.Length > 50)
                {
                    Home_City = Home_City.Remove(50);
                }

                string Home_State = string.Empty;
                Home_State = ht["Home_State"].ToString();
                if (Home_State.Length > 50)
                {
                    Home_State = Home_State.Remove(50);
                }

                string Home_Postal_Code = string.Empty;
                Home_Postal_Code = ht["Home_Postal_Code"].ToString();
                if (Home_Postal_Code.Length > 50)
                {
                    Home_Postal_Code = Home_Postal_Code.Remove(50);
                }

                string Segment = string.Empty;
                Segment = ht["Segment"].ToString();
                if (Segment.Length > 50)
                {
                    Segment = Segment.Remove(50);
                }

                if (creation_date == "")
                {
                    creation_date = "NULL";
                }
                else
                {
                    //p_Disp_Sep_Eff_Date = utility.FormatDate(clientId, "", p_Disp_Sep_Eff_Date);
                    creation_date = "CONVERT(date, '" + creation_date + "', 23)";
                }

                StringBuilder sb = new StringBuilder();
                string COMMA = ",";
                sb.Append("INSERT INTO SEV_MASTER_LIST (Comit_ID, Rpt_to_Mgr_First_Name, Last_Name, First_Name, Middle_Name, Employee_ID, Grade_Level, Gender, Noti_Date, Separation_Eff_Date, Total_Weeks, Home_Address1, Home_Address2, Home_City, Home_State, Home_Postal_Code, Segment, creation_date, job_number) VALUES ")
                   .Append("(")
                       .Append("'" + Comit_ID + "'")
                       .Append(COMMA)
                       .Append("'" + Rpt_to_Mgr_First_Name + "'")
                       .Append(COMMA)
                       .Append("'" + Last_Name + "'")
                       .Append(COMMA)
                       .Append("'" + First_Name + "'")
                       .Append(COMMA)
                       .Append("'" + Middle_Name + "'")
                       .Append(COMMA)
                       .Append("'" + Employee_ID + "'")
                       .Append(COMMA)
                       .Append("'" + Grade_Level + "'")
                       .Append(COMMA)
                       .Append("'" + Gender + "'")
                       .Append(COMMA)
                       .Append("" + p_Noti_Date+ "")
                       .Append(COMMA)
                       .Append("" + p_Separation_Eff_Date + "")
                       .Append(COMMA)
                       .Append(Total_Weeks)
                       .Append(COMMA)
                       .Append("'" + Home_Address1 + "'")
                       .Append(COMMA)
                       .Append("'" + Home_Address2 + "'")
                       .Append(COMMA)
                       .Append("'" + Home_City + "'")
                       .Append(COMMA)
                       .Append("'" + Home_State + "'")
                       .Append(COMMA)
                       .Append("'" + Home_Postal_Code + "'")
                       .Append(COMMA)
                       .Append("'" + Segment + "'")
                       .Append(COMMA)
                       .Append("" + creation_date + "")
                       .Append(COMMA)
                       .Append("'" + jobNumber + "'")
                   .Append(")");

                string sqlStr = sb.ToString();
                SqlDBOperations dbOp = new SqlDBOperations();
                dbOp.ExecuteQueryforSql(sqlStr);


                return true;
            }
            catch (Exception ex)
            {
                throw;
            }

        }
        public static string ConvertToDateFormat(string inputDate)
        {
            string[] formats = { "yyyy/MM/dd", "yyyy-MM-dd", "MM/dd/yyyy", "M/dd/yyyy", "M/d/yyyy", "MM/d/yyyy", "d/M/yyyy", "dd/M/yyyy", "d/MM/yyyy", "dd/MM/yyyy" };
            DateTime date;

            foreach (string format in formats)
            {
                if (DateTime.TryParseExact(inputDate, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out date))
                {
                    return date.ToString("yyyy-MM-dd");
                }
            }

            return null;
        }
        public void RegisterDocuments(DataTable dtClientData, DataRow dtClientRow, string clientId, string logFile, string logfileCsv, string docNumber, string jobNumber, string creation_date)
        {
            string EMPLOYEE_ID = string.Empty;
            DataTable dtEMPDetails = new DataTable();
            XmlReader xmlReader = new XmlReader();
            DataTable dtbFieldStrucure = xmlReader.ReadXML(clientId, 1, logFile);
            string valueEmpid = Convert.ToString(dtbFieldStrucure.Rows[0]["value"]);
            EMPLOYEE_ID = dtClientRow[valueEmpid].ToString();
          string sqlStr = "select * from SEV_EMP_DETAILS  where employee_id= '" + EMPLOYEE_ID + "'";

            SqlDBOperations dbOp = new SqlDBOperations();
            dtEMPDetails = dbOp.GetSqlDatatable(sqlStr);

            DataTable dtdocTemplates = new DataTable();
            //Register all static documents common to all employees first
            sqlStr = "select * from SEV_DOC_TEMPLATE where static='Y' and conditional='N'";
            dtdocTemplates = dbOp.GetSqlDatatable(sqlStr);
       
            string RandomName = "";
           
            if (dtdocTemplates.Rows.Count > 0)
            {
                // Loop through each row in the DataTable
                foreach (DataRow row in dtdocTemplates.Rows)
                {
                    RandomName = GetRandomFileName();
                    string DocName = RandomName + "_" + Path.GetFileName(Convert.ToString(row["location"]));
                    string Batch = Convert.ToString(row["batch"]);
                    int cid = 1;
                    int docid = 4;
                    string username = EMPLOYEE_ID;
                    string DocDesc = Convert.ToString(row["doc_desc"]);
                    string WfStepCode = "PENDREV";
                    string UploadedBy = "000178730";
                    int DocYear = GetBatchYear(Batch);
                    string DocPath = GetDocPath();
                    string OldDocFullPath = Convert.ToString(row["location"]);
                    File.Copy(OldDocFullPath, Path.Combine(DocPath, DocName));
                    string result = BNYM_DOC_InsertDocument(cid, username, docid, DocPath, DocName, DocDesc, DocYear, Batch, UploadedBy, WfStepCode);
                    BNYM_DOC_InsertSecurityRoles(cid,docid,Batch);
                }
            }
            //Register all conditional documents that are static
            sqlStr = "select * from SEV_DOC_TEMPLATE where static='Y' and conditional='Y'";
            dtdocTemplates = dbOp.GetSqlDatatable(sqlStr);
            string outprovider = Convert.ToString(dtEMPDetails.Rows[0]["OUTPLACEMENT_PROVIDER_ATT_B"]);
            string outprogram = Convert.ToString(dtEMPDetails.Rows[0]["OUTPLACEMENT_PROGRAM_ATT_B"]);
            
            //assign a blank row as default
            string statChoiceDefault = "123";
            DataRow[] selectedRow= dtdocTemplates.Select($"STAT_CHOICE = '{statChoiceDefault}'");
            if (outprovider.ToLower().Contains("right") && outprogram.ToLower().Contains("1"))
            {
               // Select rows based on the value of the "STAT_CHOICE" column
                string statChoiceToSelect = "RightCh13"; 
                selectedRow = dtdocTemplates.Select($"STAT_CHOICE = '{statChoiceToSelect}'"); // Filter expression with column name and value

            }
            if (outprovider.ToLower().Contains("right") && outprogram.ToLower().Contains("3"))
            {
               // Select rows based on the value of the "STAT_CHOICE" column
                string statChoiceToSelect = "RightCh13";
                selectedRow = dtdocTemplates.Select($"STAT_CHOICE = '{statChoiceToSelect}'"); // Filter expression with column name and value
            }
            if (outprovider.ToLower().Contains("right") && outprogram.ToLower().Contains("6"))
            {
               // Select rows based on the value of the "STAT_CHOICE" column
                string statChoiceToSelect = "RightCh13";
                selectedRow = dtdocTemplates.Select($"STAT_CHOICE = '{statChoiceToSelect}'"); // Filter expression with column name and value
            }
            if (outprovider.ToLower().Contains("right") && outprogram.ToLower().Contains("6"))
            {
               // Select rows based on the value of the "STAT_CHOICE" column
                string statChoiceToSelect = "RightCh6";
                selectedRow = dtdocTemplates.Select($"STAT_CHOICE = '{statChoiceToSelect}'"); // Filter expression with column name and value
            }
            if (outprovider.ToLower().Contains("lhh") && outprogram.ToLower().Contains("1"))
            {
               // Select rows based on the value of the "STAT_CHOICE" column
                string statChoiceToSelect = "LHHSelect1";
                selectedRow = dtdocTemplates.Select($"STAT_CHOICE = '{statChoiceToSelect}'"); // Filter expression with column name and value
            }
           
            if (outprovider.ToLower().Contains("lhh") && outprogram.ToLower().Contains("3"))
            {
               // Select rows based on the value of the "STAT_CHOICE" column
                string statChoiceToSelect = "LHHSelect3";
                selectedRow = dtdocTemplates.Select($"STAT_CHOICE = '{statChoiceToSelect}'"); // Filter expression with column name and value
            }
            if (outprovider.ToLower().Contains("lhh") && outprogram.ToLower().Contains("6"))
            {
               // Select rows based on the value of the "STAT_CHOICE" column
                string statChoiceToSelect = "LHHSelect6";
                selectedRow = dtdocTemplates.Select($"STAT_CHOICE = '{statChoiceToSelect}'"); // Filter expression with column name and value
            }
            if (dtdocTemplates.Rows.Count > 0)
            {
                // Loop through each row in the DataTable//There is only one row expected as per the condition.
                foreach (DataRow row in selectedRow)
                {
                    
                    RandomName = GetRandomFileName();
                    string DocName = RandomName + "_" + Path.GetFileName(Convert.ToString(row["location"]));
                    string Batch = Convert.ToString(row["batch"]);
                    int cid = 1;
                    int docid = 4;
                    string username = EMPLOYEE_ID;
                    string DocDesc = Convert.ToString(row["doc_desc"]);
                    string WfStepCode = "PENDREV";
                    string UploadedBy = "000178730";
                    int DocYear = GetBatchYear(Batch);
                    string DocPath = GetDocPath();
                    string OldDocFullPath = Convert.ToString(row["location"]);
                    File.Copy(OldDocFullPath, Path.Combine(DocPath, DocName));
                    string result = BNYM_DOC_InsertDocument(cid, username, docid, DocPath, DocName, DocDesc, DocYear, Batch, UploadedBy, WfStepCode);
                    BNYM_DOC_InsertSecurityRoles(cid, docid, Batch);
                }
            }
            //Register if BKShares or RSUS
            string BK_SHARES = Convert.ToString(dtEMPDetails.Rows[0]["STOCK_OPT"]);
            string RSUS = Convert.ToString(dtEMPDetails.Rows[0]["R_STOCK"]);
            string BK_OR_RSUS = "N";
            //if (BK_SHARES.ToLower().Contains("y"))
            //{
            //    BK_OR_RSUS = "Y";
            //    // Select rows based on the value of the "STAT_CHOICE" column
            //    string statChoiceToSelect = "BKSHARES";
            //    selectedRow = dtdocTemplates.Select($"STAT_CHOICE = '{statChoiceToSelect}'"); // Filter expression with column name and value
            //}
            if (RSUS.ToLower().Contains("y"))
            {
                BK_OR_RSUS = "Y";
               // Select rows based on the value of the "STAT_CHOICE" column
                string statChoiceToSelect = "RSUS";
                selectedRow = dtdocTemplates.Select($"STAT_CHOICE = '{statChoiceToSelect}'"); // Filter expression with column name and value
            }
            if (dtdocTemplates.Rows.Count > 0 && BK_OR_RSUS=="Y")
            {
                // Loop through each row in the DataTable//There is only one row expected as per the condition.
                foreach (DataRow row in selectedRow)
                {

                    RandomName = GetRandomFileName();
                    string DocName = RandomName + "_" + Path.GetFileName(Convert.ToString(row["location"]));
                    string Batch = Convert.ToString(row["batch"]);
                    int cid = 1;
                    int docid = 4;
                    string username = EMPLOYEE_ID;
                    string DocDesc = Convert.ToString(row["doc_desc"]);
                    string WfStepCode = "PENDREV";
                    string UploadedBy = "000178730";
                    int DocYear = GetBatchYear(Batch);
                    string DocPath = GetDocPath();
                    string OldDocFullPath = Convert.ToString(row["location"]);
                    File.Copy(OldDocFullPath, Path.Combine(DocPath, DocName));
                    string result = BNYM_DOC_InsertDocument(cid, username, docid, DocPath, DocName, DocDesc, DocYear, Batch, UploadedBy, WfStepCode);
                    BNYM_DOC_InsertSecurityRoles(cid, docid, Batch);
                }
            }
            //Register documents which requires word merge
            sqlStr = "select * from SEV_DOC_TEMPLATE where wordmerge='Y'";
            dtdocTemplates = dbOp.GetSqlDatatable(sqlStr);
            string CITY = Convert.ToString(dtEMPDetails.Rows[0]["CITY"]);
            //assign a blank row as default
            
           
                // Select rows based on the value of the "CITY" column in emp_details and doc_desc in template table
                string letterToSelect = CITY;
            DataRow[] selectedRowletter = dtdocTemplates.Select($"DOC_DESC LIKE '{letterToSelect}'"); // Filter expression with column name and value

            if (selectedRowletter.Length == 0)
            {
                // Code to handle when selectedRowletter contains no element for the selected city
                string letterDefault = "Letter Agreement - New York";
                 selectedRowletter = dtdocTemplates.Select($"DOC_DESC = '{letterDefault}'");

            }
            SetAsposeCellsLicense();
            if (dtdocTemplates.Rows.Count > 0)
            {
                // Loop through each row in the DataTable//There is only one row expected as per the condition.
                foreach (DataRow row in selectedRowletter)
                {

                    RandomName = GetRandomFileName();
                    string DocName = RandomName + "_" + Path.GetFileName(Convert.ToString(row["location"]));
                    string Batch = Convert.ToString(row["batch"]);
                    int cid = 1;
                    int docid = 4;
                    string username = EMPLOYEE_ID;
                    string DocDesc = "Letter Agreement";
                    string WfStepCode = "PENDREV";
                    string UploadedBy = "000178730";
                    int DocYear = GetBatchYear(Batch);
                    string DocPath = GetDocPath();
                    string OldDocFullPath = Convert.ToString(row["location"]);
                    string outputFullPath = Path.Combine(DocPath, DocName);
                   // File.Copy(OldDocFullPath, outputFullPath);
                    //Now go for the aspose merge
                    
                    // Retrieve merge fields and values from database
                    string doc_desc = Convert.ToString(row["doc_desc"]);
                     sqlStr = "select merge_field, mapping_table_field from SEV_WORDMRGFLD_MAPPING where doc_desc='" + doc_desc + "'";
                    DataTable dtMergeFields = dbOp.GetSqlDatatable(sqlStr);

                    // Create a dictionary to store merge field names and their corresponding values
                    // Create a case-insensitive dictionary
                    Dictionary<string, string> fieldValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                    // Loop through the retrieved data table and fetch values from SEV_EMP_DETAILS table
                    foreach (DataRow row1 in dtMergeFields.Rows)
                    {
                        string mergeField = Convert.ToString(row1["merge_field"]);
                        string mappingField = Convert.ToString(row1["mapping_table_field"]);

                        // Fetch the value from SEV_EMP_DETAILS table using the mapping field
                        // You may need to modify the SQL query to suit your database schema
                        sqlStr = "SELECT " + mappingField + " FROM SEV_EMP_DETAILS WHERE employee_id='"+EMPLOYEE_ID+"'";
                        DataTable dtMergeValue = dbOp.GetSqlDatatable(sqlStr);
                        string value = Convert.ToString(dtMergeValue.Rows[0][mappingField]);
                        if(mappingField.ToUpper().Contains("DATE"))
                        {
                            DateTime DT_value = DateTime.Parse(value);

                            //value = DT_value.ToString("yyyy-MM-dd");
                            value = DT_value.ToString("MMMM dd, yyyy");
                            //value = value.Replace(",", " ");

                        }

                        // Add the merge field and its corresponding value to the dictionary
                        if (!fieldValues.ContainsKey(mergeField))
                        {
                            fieldValues.Add(mergeField, value.Trim());
                        }
                    }

                    // Call the MergeFieldsInTemplateDocument function with the retrieved field values
                    string templateFilePath = OldDocFullPath;
                    string outputFilePath = "";
                    if (outputFullPath.Contains(".docx"))
                    {
                        outputFilePath = outputFullPath.Replace(".docx", ".pdf");
                    }
                    else if (outputFullPath.Contains(".DOCX"))
                    {
                        outputFilePath = outputFullPath.Replace(".DOCX", ".pdf");
                    }
                    else if (outputFullPath.Contains(".DOC"))
                    {
                        outputFilePath = outputFullPath.Replace(".DOC", ".pdf");
                    }
                    else
                    {
                        outputFilePath = outputFullPath.Replace(".doc", ".pdf");
                    }
                    
                    DocName = DocName.Replace(".docx", ".pdf");
                    DocName = DocName.Replace(".DOCX", ".pdf");
                    DocName = DocName.Replace(".DOC", ".pdf");
                    DocName = DocName.Replace(".doc", ".pdf");
                    MergeFieldsInTemplateDocument(templateFilePath, outputFilePath, fieldValues,logFile);


                    string result = BNYM_DOC_InsertDocument(cid, username, docid, DocPath, DocName, DocDesc, DocYear, Batch, UploadedBy, WfStepCode);
                    BNYM_DOC_InsertSecurityRoles(cid, docid, Batch);
                }
            }
           

        }

        public void RegisterOWBPA(DataTable dtClientData, DataRow dtClientRow, string clientId, string logFile, string logfileCsv, string docNumber, string jobNumber, string creation_date)
        {
            string EMPLOYEE_ID = string.Empty;
            DataTable dtEMPDetails = new DataTable();
            XmlReader xmlReader = new XmlReader();
            DataTable dtbFieldStrucure = xmlReader.ReadXML(clientId, 1, logFile);
            string valueEmpid = Convert.ToString(dtbFieldStrucure.Rows[0]["value"]);
            EMPLOYEE_ID = dtClientRow[valueEmpid].ToString();
            string sqlStr = "select * from SEV_EMP_DETAILS  where employee_id= '" + EMPLOYEE_ID + "'";

            SqlDBOperations dbOp = new SqlDBOperations();
            dtEMPDetails = dbOp.GetSqlDatatable(sqlStr);

            DataTable dtdocTemplates = new DataTable();
            //Get OWBPA template document from template table
            string OWBPA_TYPE = Convert.ToString(dtEMPDetails.Rows[0]["OWBPA_TYPE"]);
            sqlStr = "select * from SEV_DOC_TEMPLATE where static='N' and wordmerge='N' and doc_desc like '%"+OWBPA_TYPE +"%'";
            dtdocTemplates = dbOp.GetSqlDatatable(sqlStr);
            
            

            DataTable dtBPR = new DataTable();
            //Get all records from base_population report
            sqlStr = "select * from SEV_BASE_POP_REPORT";
            dtBPR = dbOp.GetSqlDatatable(sqlStr);
            // Add two columns to the DataTable
            DataColumn col1 = new DataColumn("Age*", typeof(int));
            DateTime DT_ReleaseDate = DateTime.Parse(Convert.ToString(dtEMPDetails.Rows[0]["RELEASE_DATE"]));

            string Release_date_formatted = DT_ReleaseDate.ToString("MMMM dd, yyyy");
            string columnName2 = "Exact Age as of " + Release_date_formatted.Replace(","," ");
            DataColumn col2 = new DataColumn(columnName2, typeof(decimal));
            dtBPR.Columns.Add(col1);
            dtBPR.Columns.Add(col2);
            UpdateDataTable(dtBPR, columnName2, Release_date_formatted);
            //OWBPA Generation starts
            DataTable dtSevEmpDetailsForJob = new DataTable();
            //Get all records from base_population report
            string job_number = Convert.ToString(dtEMPDetails.Rows[0]["job_number"]);
            sqlStr = "select * from SEV_EMP_DETAILS where job_number='" + job_number+ "'";
            dtSevEmpDetailsForJob = dbOp.GetSqlDatatable(sqlStr);
            string SEGMENT= Convert.ToString(dtEMPDetails.Rows[0]["SEGMENT"]);
            string BUD = Convert.ToString(dtEMPDetails.Rows[0]["BUD"]);
            string BUD1 = Convert.ToString(dtEMPDetails.Rows[0]["BUD1"]);
            //  dtSevEmpDetailsForJob is a DataTable containing the SEV_EMP_DETAILS data for this job
            string filterExpression = "";
            if (BUD1 != "")
            {
                filterExpression = "SEGMENT = '" + SEGMENT + "' AND BUD = '" + BUD + "' AND BUD1 = '" + BUD1 + "'";
            }
            else
                {
                filterExpression = "SEGMENT = '" + SEGMENT + "' AND BUD = '" + BUD + "'";
            }
            DataRow[] filteredRows = dtSevEmpDetailsForJob.Select(filterExpression);

            // Create a new datatable with the same structure as the original datatable
            DataTable dtFilteredSevEmpDetailsForJob = dtSevEmpDetailsForJob.Clone();

            // Add the filtered rows to the new datatable
            foreach (DataRow row in filteredRows)
            {
                dtFilteredSevEmpDetailsForJob.ImportRow(row);
            }
            
            filteredRows = dtBPR.Select(filterExpression);

            // Create a new datatable with the same structure as the original datatable
            DataTable dtFilteredSevBasePopReport = dtBPR.Clone();

            // Add the filtered rows to the new datatable
            foreach (DataRow row in filteredRows)
            {
                dtFilteredSevBasePopReport.ImportRow(row);
            }

            // Assuming you have a DataTable named dtBPR
            // You can use the Select() method to get an array of DataRow that meet the condition

            // Remove all columns except the required ones
            foreach (DataColumn column in dtFilteredSevBasePopReport.Columns.Cast<DataColumn>().ToArray())
            {
                string columnName = column.ColumnName.ToLower();
                if (columnName != "employee_id" &&
                    columnName != "last_name" &&
                    columnName != "first_name" &&
                    columnName != "segment" &&
                    columnName != "bud" &&
                    columnName != "grade_level" &&
                    columnName != "job_title" &&
                    columnName != "age*" &&
                    columnName != columnName2.ToLower())
                {
                    dtFilteredSevBasePopReport.Columns.Remove(columnName);
                }
            }
            // Call the MergeNames function to merge last_name and first_name into "Employee Name"
            //MergeNames(dtBPR, "last_name", "first_name");
            // Add two columns to the DataTable
            DataColumn col3 = new DataColumn("Selected", typeof(string));
            
            DataColumn col4 = new DataColumn("Not Selected", typeof(string));
            dtFilteredSevBasePopReport.Columns.Add(col3);
            dtFilteredSevBasePopReport.Columns.Add(col4);

            if(OWBPA_TYPE=="Cumulative")
            {
                DataColumn col5 = new DataColumn("Selected in the Prior Phase of Program", typeof(string));
                dtFilteredSevBasePopReport.Columns.Add(col5);
            }
            else
            {
                AddXToSelected(dtFilteredSevBasePopReport,dtFilteredSevEmpDetailsForJob);
                // Sort the data table according to the specified sorting criteria
                dtFilteredSevBasePopReport.DefaultView.Sort = "Job_title ASC,"+ columnName2 +" ASC";
                dtFilteredSevBasePopReport = dtFilteredSevBasePopReport.DefaultView.ToTable();
                dtFilteredSevBasePopReport.DefaultView.Sort = "Selected DESC";
                dtFilteredSevBasePopReport = dtFilteredSevBasePopReport.DefaultView.ToTable();
                // Remove all columns except the required ones
                foreach (DataColumn column in dtFilteredSevBasePopReport.Columns.Cast<DataColumn>().ToArray())
                {
                    string columnName = column.ColumnName.ToLower();
                    if (columnName != "grade_level" &&
                        columnName != "selected" &&
                        columnName != "not selected" &&
                        columnName != "job_title" &&
                        columnName != "age*" )
                    {
                        dtFilteredSevBasePopReport.Columns.Remove(columnName);
                    }
                }
                dtFilteredSevBasePopReport.Columns["GRADE_LEVEL"].ColumnName = "Level";
                dtFilteredSevBasePopReport.Columns["Job_Title"].ColumnName = "Job Title";
                ExchangeColumnPosition(dtFilteredSevBasePopReport,"Level","Age*");
            }

            string RandomName = "";
            SetAsposeCellsLicense();
            if (dtdocTemplates.Rows.Count > 0)
            {
                // Loop through each row in the DataTable
                foreach (DataRow row in dtdocTemplates.Rows)
                {
                    RandomName = GetRandomFileName();
                    string DocName = RandomName + "_" + Path.GetFileName(Convert.ToString(row["location"]));
                    string Batch = Convert.ToString(row["batch"]);
                    int cid = 1;
                    int docid = 4;
                    string username = EMPLOYEE_ID;
                    string DocDesc = "OWBPA Analysis";
                    string WfStepCode = "PENDREV";
                    string UploadedBy = "000178730";
                    int DocYear = GetBatchYear(Batch);
                    string DocPath = GetDocPath();
                    string OldDocFullPath = Convert.ToString(row["location"]);
                    string outputFullPath = Path.Combine(DocPath, DocName);
                    // File.Copy(OldDocFullPath, outputFullPath);
                    //Now go for the aspose merge

                    // Retrieve merge fields and values from database
                    string doc_desc = Convert.ToString(row["doc_desc"]);
                    sqlStr = "select merge_field, mapping_table_field from SEV_WORDMRGFLD_MAPPING where doc_desc='" + doc_desc + "'";
                    DataTable dtMergeFields = dbOp.GetSqlDatatable(sqlStr);

                    // Create a dictionary to store merge field names and their corresponding values
                    // Create a case-insensitive dictionary
                    Dictionary<string, string> fieldValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                    // Loop through the retrieved data table and fetch values from SEV_EMP_DETAILS table
                    foreach (DataRow row1 in dtMergeFields.Rows)
                    {
                        string mergeField = Convert.ToString(row1["merge_field"]);
                        string mappingField = Convert.ToString(row1["mapping_table_field"]);

                        // Fetch the value from SEV_EMP_DETAILS table using the mapping field
                        // You may need to modify the SQL query to suit your database schema
                        sqlStr = "SELECT " + mappingField + " FROM SEV_EMP_DETAILS WHERE employee_id='" + EMPLOYEE_ID + "'";
                        DataTable dtMergeValue = dbOp.GetSqlDatatable(sqlStr);
                        string value = Convert.ToString(dtMergeValue.Rows[0][mappingField]);
                        if (mappingField.ToUpper().Contains("DATE"))
                        {
                            DateTime DT_value = DateTime.Parse(value);

                            //value = DT_value.ToString("yyyy-MM-dd");
                            value = DT_value.ToString("MMMM dd, yyyy");
                            //value = value.Replace(","," ");

                        }

                        // Add the merge field and its corresponding value to the dictionary
                        if (!fieldValues.ContainsKey(mergeField))
                        {
                            fieldValues.Add(mergeField, value.Trim());
                        }
                    }

                    // Call the MergeFieldsInTemplateDocument function with the retrieved field values
                    string templateFilePath = OldDocFullPath;
                    string outputFilePath = "";
                    MergeFieldsInTemplateDocumentOWBPA(templateFilePath, outputFullPath, fieldValues, logFile);
                    if (outputFullPath.Contains(".docx"))
                    {
                        outputFilePath = outputFullPath.Replace(".docx", ".pdf");
                    }
                    else if (outputFullPath.Contains(".DOCX"))
                    {
                        outputFilePath = outputFullPath.Replace(".DOCX", ".pdf");
                    }
                    else if (outputFullPath.Contains(".DOC"))
                    {
                        outputFilePath = outputFullPath.Replace(".DOC", ".pdf");
                    }
                    else
                    {
                        outputFilePath = outputFullPath.Replace(".doc", ".pdf");
                    }

                    DocName = DocName.Replace(".docx", ".pdf");
                    DocName = DocName.Replace(".DOCX", ".pdf");
                    DocName = DocName.Replace(".DOC", ".pdf");
                    DocName = DocName.Replace(".doc", ".pdf");
                    
                    AddDataTableToWordDocument(outputFullPath,dtFilteredSevBasePopReport, outputFilePath);

                    string result = BNYM_DOC_InsertDocument(cid, username, docid, DocPath, DocName, DocDesc, DocYear, Batch, UploadedBy, WfStepCode);
                    BNYM_DOC_InsertSecurityRoles(cid, docid, Batch);
                }
            }
            
          
            


        }
        public static void AddXToSelected(DataTable dtFilteredSevBasePopReport, DataTable dtFilteredSevEmpDetailsForJob)
        {
            try
            {
                // Check if both data tables are not null and contain the required columns
                if (dtFilteredSevBasePopReport != null && dtFilteredSevEmpDetailsForJob != null &&
                    dtFilteredSevBasePopReport.Columns.Contains("employee_id") &&
                    dtFilteredSevEmpDetailsForJob.Columns.Contains("employee_id") &&
                    dtFilteredSevBasePopReport.Columns.Contains("Selected"))
                {
                    // Loop through each row in dtFilteredSevBasePopReport
                    foreach (DataRow row in dtFilteredSevBasePopReport.Rows)
                    {
                        // Get the employee_id from the current row
                        string employeeId = row["employee_id"].ToString();

                        // Check if the employee_id exists in dtFilteredSevEmpDetailsForJob
                        DataRow[] matchingRows = dtFilteredSevEmpDetailsForJob.Select("employee_id = '" + employeeId + "'");
                        if (matchingRows.Length > 0)
                        {
                            // If employee_id exists, set "x" in the "Selected" column of dtFilteredSevBasePopReport
                            row["Selected"] = "x";
                            
                        }
                        else
                        {
                            row["Not Selected"] = "x";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Handle any exceptions here
                Console.WriteLine("Error: " + ex.Message);
            }
        }
        public static void MergeNames(DataTable dt, string lastNameColumn, string firstNameColumn)
        {
            // Create a new column "Employee Name" with a DataColumn expression that concatenates last_name and first_name
            DataColumn employeeNameColumn = new DataColumn("Employee Name", typeof(string));
            employeeNameColumn.Expression = string.Format("{0} + ', ' + {1}", lastNameColumn, firstNameColumn);
            dt.Columns.Add(employeeNameColumn);

            // Update the values in the new "Employee Name" column
            foreach (DataRow row in dt.Rows)
            {
                row["Employee Name"] = row[lastNameColumn] + ", " + row[firstNameColumn];
            }

            // Remove the original last_name and first_name columns
            dt.Columns.Remove(lastNameColumn);
            dt.Columns.Remove(firstNameColumn);
        }
        public static void UpdateDataTable(DataTable dtBPR, string columnName2, string ageAsOf)
        {
            // Convert the "ageAsOf" parameter to a DateTime object
            DateTime targetDate = DateTime.ParseExact(ageAsOf, "MMMM dd, yyyy", System.Globalization.CultureInfo.InvariantCulture);

            // Loop through each row in the DataTable and calculate values for the new columns
            foreach (DataRow row in dtBPR.Rows)
            {
                // Get the birth_date from the row
                DateTime birthDate = Convert.ToDateTime(row["birth_date"]);

                // Calculate age with two decimal points
                TimeSpan age = targetDate - birthDate;
                decimal ageInYears = (decimal)age.TotalDays / 365;
                decimal ageRounded= Math.Round(ageInYears, 2, MidpointRounding.AwayFromZero);
                row[columnName2] = ageRounded;

                // Calculate Age* which is the age with decimal points removed.
                decimal exactAgeInYears = Convert.ToDecimal(row[columnName2]);
                row["Age*"] = (int)exactAgeInYears;
            }
        }
        public void AddDataTableToWordDocument(string documentFullPath, DataTable dt, string outputPdfPath)
        {
            // Load the Word document
            Document doc = new Document(documentFullPath);

            // Create a new section in the document
            DocumentBuilder builder = new DocumentBuilder(doc);
            builder.MoveToDocumentEnd();

            // Add the datatable as a table
            Table table = builder.StartTable();

            // Insert table headers
            foreach (DataColumn column in dt.Columns)
            {
                builder.InsertCell();
                builder.Write(column.ColumnName);
            }
            builder.EndRow();

            // Insert table data
            foreach (DataRow row in dt.Rows)
            {
                foreach (var item in row.ItemArray)
                {
                    builder.InsertCell();
                    builder.Write(item.ToString());
                }
                builder.EndRow();
            }

            // Format the table
            builder.EndTable();
            table.PreferredWidth = PreferredWidth.FromPercent(100);

            // Save the document as a PDF
            doc.Save(outputPdfPath, SaveFormat.Pdf);
        }

        public void ExchangeColumnPosition(DataTable dt, string columnName1, string columnName2)
        {
            if (dt.Columns.Contains(columnName1) && dt.Columns.Contains(columnName2))
            {
                int index1 = dt.Columns.IndexOf(columnName1);
                int index2 = dt.Columns.IndexOf(columnName2);

                dt.Columns[columnName1].SetOrdinal(index2);
                dt.Columns[columnName2].SetOrdinal(index1);
            }
        }

        public static void MergeFieldsInTemplateDocumentOWBPA(string templateFilePath, string outputFilePath, Dictionary<string, string> fieldValues, string logFile)
        {
            // Load the template Word document
            Document doc = new Document(templateFilePath);

            // Loop through all the fields in the document
            foreach (Field field in doc.Range.Fields)
            {
                // Check if the field is a merge field
                if (field.Type == FieldType.FieldMergeField)
                {
                    // Get the name of the merge field
                    string fieldName = field.GetFieldCode().Replace(" MERGEFIELD", "").Trim();

                    fieldName = SplitStringAtFirstSpace(fieldName);


                    // Check if the field name exists in the dictionary of field values, ignoring case
                    if (fieldValues.TryGetValue(fieldName, out string fieldValue))
                    {
                        // Replace the field with its corresponding value from the dictionary
                        field.Update();

                        // Insert the value into the field
                        field.Result = fieldValue;
                    }
                    else
                    {
                        // Handle the case where the field name is not found in the dictionary
                        // For example, you can skip the field or log a message
                        Logs.WriteLog(logFile, $"Field '{fieldName}' not found in dictionary.");
                    }
                }
            }

            // Save the output document
            doc.Save(outputFilePath);
        }
        public static void MergeFieldsInTemplateDocument(string templateFilePath, string outputFilePath, Dictionary<string, string> fieldValues,string logFile)
        {
            // Load the template Word document
            Document doc = new Document(templateFilePath);

            // Loop through all the fields in the document
            foreach (Field field in doc.Range.Fields)
            {
                // Check if the field is a merge field
                if (field.Type == FieldType.FieldMergeField)
                {
                    // Get the name of the merge field
                    string fieldName = field.GetFieldCode().Replace(" MERGEFIELD", "").Trim();
                    
                        fieldName = SplitStringAtFirstSpace(fieldName);
                    
                    
                    // Check if the field name exists in the dictionary of field values, ignoring case
                    if (fieldValues.TryGetValue(fieldName, out string fieldValue))
                    {
                        // Replace the field with its corresponding value from the dictionary
                        field.Update();

                        // Insert the value into the field
                        field.Result = fieldValue;
                    }
                    else
                    {
                        // Handle the case where the field name is not found in the dictionary
                        // For example, you can skip the field or log a message
                        Logs.WriteLog(logFile, $"Field '{fieldName}' not found in dictionary.");
                    }
                }
            }

            // Save the output document
            doc.Save(outputFilePath, SaveFormat.Pdf);
        }


        public static string SplitStringAtFirstSpace(string input)
        {
            string result = input;

            if (input.Contains(" "))
            {
                result = input.Substring(0, input.IndexOf(' '));
            }

            return result;
        }

        private void SetAsposeCellsLicense()
        {
            string licenseFile = @"Aspose.Total.lic";
            if (File.Exists(licenseFile))
            {
                //This shows how to license Aspose.Words, if you don't specify a license, 
                //Aspose.Words works in evaluation mode.
                Aspose.Words.License license = new Aspose.Words.License();
                license.SetLicense(licenseFile);
            }
        }





        public string BNYM_DOC_InsertSecurityRoles(int cid, int docid, string Batch)
        {
            string Roles = "5,6,7,8,9,10,12,13";
            string retStatus = string.Empty;
            string sqlStr = "BNYM_DOC_InsertSecurityRoles";
            Hashtable paramList = new Hashtable();
            paramList.Add("@Roles", Roles);
            paramList.Add("@ClientID", cid);
            paramList.Add("@DocID", docid);
            paramList.Add("@Batch", Batch);

            SqlDBOperations dbOp = new SqlDBOperations();
            dbOp.ExecuteQueryProc(sqlStr, paramList);
            return retStatus;
        }
        private  int GetBatchYear( string batch)
        {
            DataTable dtBatch = new DataTable();
            string sqlStr = "select doc_year from bnym_batch where batch='"+ batch + "'";

            SqlDBOperations dbOp = new SqlDBOperations();
            dtBatch = dbOp.GetSqlDatatable(sqlStr);
            return Convert.ToInt32(dtBatch.Rows[0]["doc_year"]); ;
        }
        private string GetDocPath()
        {
            DataTable dtBatch = new DataTable();
            string sqlStr = "select * from Globalization where ResourceId = 'uploadPath'";

            SqlDBOperations dbOp = new SqlDBOperations();
            dtBatch = dbOp.GetSqlDatatable(sqlStr);
            return Convert.ToString(dtBatch.Rows[0]["Value"]); 
        }
        public string BNYM_DOC_InsertDocument(int cid, string username, int docid, string DocPath, string DocName, string DocDesc, int DocYear, string Batch, string UploadedBy, string WfStepCode)
        {
            int userId = 0;
            string retStatus = string.Empty;
            string sqlStr = "BNYM_DOC_InsertDocument";
            Hashtable paramList = new Hashtable();
            paramList.Add("@ClientId", cid);
            paramList.Add("@Username", username);
            paramList.Add("@DocId", docid);
            paramList.Add("@DocPath", DocPath);
            paramList.Add("@DocName", DocName);
            paramList.Add("@DocDesc", DocDesc);
            paramList.Add("@DocYear", DocYear);
            paramList.Add("@Batch", Batch);
            paramList.Add("@UploadedBy", UploadedBy);
            paramList.Add("@WfStepCode", WfStepCode);

            SqlDBOperations dbOp = new SqlDBOperations();
            retStatus = dbOp.ExecuteScalarProc(sqlStr, paramList);
            return retStatus;
        }
        private static string GetRandomFileName()
        {
            string RandomName = DateTime.Now.Day.ToString() + DateTime.Now.Month.ToString() + DateTime.Now.Year.ToString() + DateTime.Now.Hour.ToString() + DateTime.Now.Minute.ToString() + DateTime.Now.Second.ToString() + DateTime.Now.Millisecond.ToString(); ;
            return RandomName;
        }
        public bool InsertEmployeeDetailsIF(string clientId, Hashtable ht, string memberId, string docNumber, string jobNumber, string creation_date)
        {

            try
            {
                
                string EMPLOYEE_ID = string.Empty;
                EMPLOYEE_ID = RemoveSpecialCharacters(ht["EMPLOYEE_ID"].ToString());
                if (EMPLOYEE_ID.Length > 20)
                {
                    EMPLOYEE_ID = EMPLOYEE_ID.Remove(20);
                }
                DataTable dtML = new DataTable();
                string sqlStr = "select * from SEV_MASTER_LIST  where employee_id= '" + EMPLOYEE_ID + "'";

                SqlDBOperations dbOp = new SqlDBOperations();
                dtML = dbOp.GetSqlDatatable(sqlStr);

                string LAST_NAME = string.Empty;
                LAST_NAME = RemoveSpecialCharacters(ht["LAST_NAME"].ToString());
                if (LAST_NAME.Length > 50)
                {
                    LAST_NAME = LAST_NAME.Remove(50);
                }
                if (string.IsNullOrEmpty(LAST_NAME) && dtML.Rows.Count> 0)
                {
                    LAST_NAME = Convert.ToString(dtML.Rows[0]["LAST_NAME"]);
                }

                string FIRST_NAME = string.Empty;
                FIRST_NAME = RemoveSpecialCharacters(ht["FIRST_NAME"].ToString());
                if (FIRST_NAME.Length > 50)
                {
                    FIRST_NAME = FIRST_NAME.Remove(50);
                }
                if (string.IsNullOrEmpty(FIRST_NAME) && dtML.Rows.Count > 0)
                {
                    FIRST_NAME = Convert.ToString(dtML.Rows[0]["FIRST_NAME"]);
                }
                string MIDDLE_NAME = string.Empty;
                MIDDLE_NAME = RemoveSpecialCharacters(ht["MIDDLE_NAME"].ToString());
                if (MIDDLE_NAME.Length > 50)
                {
                    MIDDLE_NAME = MIDDLE_NAME.Remove(50);
                }
                if (string.IsNullOrEmpty(MIDDLE_NAME) && dtML.Rows.Count > 0)
                {
                    MIDDLE_NAME = Convert.ToString(dtML.Rows[0]["MIDDLE_NAME"]);
                }

                string GRADE_LEVEL = string.Empty;
                GRADE_LEVEL = RemoveSpecialCharacters(ht["GRADE_LEVEL"].ToString());
                if (GRADE_LEVEL.Length > 2)
                {
                    GRADE_LEVEL = GRADE_LEVEL.Remove(2);
                }
                if (string.IsNullOrEmpty(GRADE_LEVEL)&& dtML.Rows.Count > 0)
                {
                    GRADE_LEVEL = Convert.ToString(dtML.Rows[0]["GRADE_LEVEL"]);
                }
                
                string GENDER = string.Empty;
                GENDER = RemoveSpecialCharacters(ht["GENDER"].ToString());
               
                if (string.IsNullOrEmpty(GENDER) && dtML.Rows.Count > 0)
                {
                    GENDER = Convert.ToString(dtML.Rows[0]["GENDER"]);
                }
                if (GENDER.ToLower()=="male")
                {
                    GENDER = "Mr.";
                }

                else if (GENDER.ToLower() == "female")
                {
                    GENDER = "Ms.";
                }
                else
                {
                    GENDER = "";
                }
                if (GENDER.Length > 3)
                {
                    GENDER = GENDER.Remove(3);
                }

                string TOTAL_WEEKS = string.Empty;
                TOTAL_WEEKS = RemoveSpecialCharacters(ht["TOTAL_WEEKS"].ToString());
                if (TOTAL_WEEKS.Length > 10)
                {
                    TOTAL_WEEKS = TOTAL_WEEKS.Remove(10);
                }
                if (string.IsNullOrEmpty(TOTAL_WEEKS) && dtML.Rows.Count > 0)
                {
                    TOTAL_WEEKS = Convert.ToString(dtML.Rows[0]["TOTAL_WEEKS"]);
                }

                string HOME_ADDRESS_1 = string.Empty;
                HOME_ADDRESS_1 = RemoveSpecialCharacters(ht["HOME_ADDRESS_1"].ToString());
                if (HOME_ADDRESS_1.Length > 50)
                {
                    HOME_ADDRESS_1 = HOME_ADDRESS_1.Remove(50);
                }
                if (string.IsNullOrEmpty(HOME_ADDRESS_1) && dtML.Rows.Count > 0)
                {
                    HOME_ADDRESS_1 = Convert.ToString(dtML.Rows[0]["HOME_ADDRESS1"]);
                }

                string HOME_ADDRESS_2 = string.Empty;
                HOME_ADDRESS_2 = RemoveSpecialCharacters(ht["HOME_ADDRESS_2"].ToString());
                if (HOME_ADDRESS_2.Length > 50)
                {
                    HOME_ADDRESS_2 = HOME_ADDRESS_2.Remove(50);
                }
                if (string.IsNullOrEmpty(HOME_ADDRESS_2) && dtML.Rows.Count > 0)
                {
                    HOME_ADDRESS_2 = Convert.ToString(dtML.Rows[0]["HOME_ADDRESS2"]);
                }

                string CITY = string.Empty;
                CITY = RemoveSpecialCharacters(ht["CITY"].ToString());
                if (CITY.Length > 50)
                {
                    CITY = CITY.Remove(50);
                }
                if (string.IsNullOrEmpty(CITY) && dtML.Rows.Count > 0)
                {
                    CITY = Convert.ToString(dtML.Rows[0]["HOME_CITY"]);
                }

                string STATE = string.Empty;
                STATE = RemoveSpecialCharacters(ht["STATE"].ToString());
                if (STATE.Length > 50)
                {
                    STATE = STATE.Remove(50);
                }
                if (string.IsNullOrEmpty(STATE) && dtML.Rows.Count > 0)
                {
                    STATE = Convert.ToString(dtML.Rows[0]["HOME_STATE"]);
                }

                string ZIP_CODE = string.Empty;
                ZIP_CODE = RemoveSpecialCharacters(ht["ZIP_CODE"].ToString());
                if (ZIP_CODE.Length > 10)
                {
                    ZIP_CODE = ZIP_CODE.Remove(10);
                }
                if (string.IsNullOrEmpty(ZIP_CODE) && dtML.Rows.Count > 0)
                {
                    ZIP_CODE = Convert.ToString(dtML.Rows[0]["Home_Postal_Code"]);
                }

                string SEGMENT = string.Empty;
                SEGMENT = RemoveSpecialCharacters(ht["SEGMENT"].ToString());
                if (SEGMENT.Length > 50)
                {
                    SEGMENT = SEGMENT.Remove(50);
                }
                if (string.IsNullOrEmpty(SEGMENT) && dtML.Rows.Count > 0)
                {
                    SEGMENT = Convert.ToString(dtML.Rows[0]["SEGMENT"]);
                }

                DataTable dtBNYMWAVES = new DataTable();
                 sqlStr = "select * from SEV_BNYM_WAVES  where active='Y'";

                 dbOp = new SqlDBOperations();
                dtBNYMWAVES = dbOp.GetSqlDatatable(sqlStr);

                string PROGRAM_DATE= Convert.ToString(dtBNYMWAVES.Rows[0]["PROGRAM_DATE"]);
                string RELEASE_DATE = Convert.ToString(dtBNYMWAVES.Rows[0]["RELEASE_DATE"]);

                DateTime DT_PROGRAM_DATE = DateTime.Parse(PROGRAM_DATE);

                PROGRAM_DATE = DT_PROGRAM_DATE.ToString("yyyy-MM-dd");


                DateTime DT_RELEASE_DATE = DateTime.Parse(RELEASE_DATE);

                RELEASE_DATE = DT_RELEASE_DATE.ToString("yyyy-MM-dd");

                string SEPARATION_EFF_DATE = string.Empty;
                SEPARATION_EFF_DATE = ht["SEPARATION_EFF_DATE"].ToString();
                if (string.IsNullOrEmpty(SEPARATION_EFF_DATE) && dtML.Rows[0]["SEPARATION_DATE"] != null)
                {
                    SEPARATION_EFF_DATE = Convert.ToString(dtBNYMWAVES.Rows[0]["SEPARATION_DATE"]);
                    
                }
                DateTime DT_SEPARATION_EFF_DATE = DateTime.Parse(SEPARATION_EFF_DATE);

                SEPARATION_EFF_DATE = DT_SEPARATION_EFF_DATE.ToString("yyyy-MM-dd");

                //calculate SUBPAY_END_DATE ->  SUBPAY_END_DATE= Seperation Date + Total weeks

                DateTime DT_SUBPAY_END_DATE = AddWeeks(DT_SEPARATION_EFF_DATE, Convert.ToInt32(TOTAL_WEEKS));

                string SUBPAY_END_DATE = DT_SUBPAY_END_DATE.ToString("yyyy-MM-dd");

                string SUB_PAY_THROUGH_DATE_ATT_B = string.Empty;
                SUB_PAY_THROUGH_DATE_ATT_B =ht["SUB_PAY_THROUGH_DATE_ATT_B"].ToString();

                if (string.IsNullOrEmpty(SUB_PAY_THROUGH_DATE_ATT_B))
                {
                    //calculate SUB_PAY_THROUGH_DATE_ATT_B ->  SUB_PAY_THROUGH_DATE_ATT_B= SUBPAY_END_DATE - 1
                    DateTime DT_SUB_PAY_THROUGH_DATE_ATT_B = DT_SUBPAY_END_DATE.AddDays(-1);
                    SUB_PAY_THROUGH_DATE_ATT_B= DT_SUB_PAY_THROUGH_DATE_ATT_B.ToString("yyyy-MM-dd");
                }

                string BENEFITS_END_DATE_ATT_B = string.Empty;
                BENEFITS_END_DATE_ATT_B = ht["BENEFITS_END_DATE_ATT_B"].ToString();

                if (string.IsNullOrEmpty(BENEFITS_END_DATE_ATT_B))
                {
                    //calculate BENEFITS_END_DATE_ATT_B ->  Last day of Month(SUBPAY_END_DATE)
                    DateTime DT_BENEFITS_END_DATE_ATT_B =  LastDayOfMonth(DT_SUBPAY_END_DATE);
                    BENEFITS_END_DATE_ATT_B = DT_BENEFITS_END_DATE_ATT_B.ToString("yyyy-MM-dd");
                }

               






                string NOTI_DATE = string.Empty;
                NOTI_DATE = RemoveSpecialCharacters(ht["NOTI_DATE"].ToString());
                if (string.IsNullOrEmpty(NOTI_DATE) && dtML.Rows.Count > 0)
                {
                    NOTI_DATE = Convert.ToString(dtBNYMWAVES.Rows[0]["NOTI_DATE"]);
                    DateTime DT_NOTI_DATE = DateTime.Parse(NOTI_DATE);

                    NOTI_DATE = DT_NOTI_DATE.ToString("yyyy-MM-dd");
                }
                string BK_SHARES = string.Empty;
                BK_SHARES = RemoveSpecialCharacters(ht["BK_SHARES"].ToString());
                if (BK_SHARES.Length > 1)
                {
                    BK_SHARES = BK_SHARES.Remove(1);
                }

                string R_STOCK_ATT_B = string.Empty;
                R_STOCK_ATT_B = RemoveSpecialCharacters(ht["R_STOCK_ATT_B"].ToString());
                if (R_STOCK_ATT_B.Length > 1)
                {
                    R_STOCK_ATT_B = R_STOCK_ATT_B.Remove(1);
                }

                string OUTPLACEMENT_PROGRAM_ATT_B = string.Empty;
                OUTPLACEMENT_PROGRAM_ATT_B = RemoveSpecialCharacters(ht["OUTPLACEMENT_PROGRAM_ATT_B"].ToString());
                if (OUTPLACEMENT_PROGRAM_ATT_B.Length > 50)
                {
                    OUTPLACEMENT_PROGRAM_ATT_B = OUTPLACEMENT_PROGRAM_ATT_B.Remove(50);
                }

                string OUTPLACEMENT_PROVIDER_ATT_B = string.Empty;
                OUTPLACEMENT_PROVIDER_ATT_B = RemoveSpecialCharacters(ht["OUTPLACEMENT_PROVIDER_ATT_B"].ToString());
                if (OUTPLACEMENT_PROVIDER_ATT_B.Length > 50)
                {
                    OUTPLACEMENT_PROVIDER_ATT_B = OUTPLACEMENT_PROVIDER_ATT_B.Remove(50);
                }


                DataTable dtBPR = new DataTable();
                 sqlStr = "select * from SEV_BASE_POP_REPORT  where employee_id= '" + EMPLOYEE_ID + "'";

                 dbOp = new SqlDBOperations();
                dtBPR = dbOp.GetSqlDatatable(sqlStr);

                string BUD = "";
                string BUD1 = "";
                if (dtBPR.Rows.Count > 0)
                {
                     BUD = Convert.ToString(dtBPR.Rows[0]["BUD"]);
                     BUD1 = Convert.ToString(dtBPR.Rows[0]["BUD1"]);
                }
                else
                {
                    BUD = "";
                    BUD1 = "";
                }

                if (creation_date == "")
                {
                    creation_date = "NULL";
                }

                //Letter creation date is notice date
                string LETTER_CREATION_DATE = NOTI_DATE;
                DateTime DT_LETTER_CREATION_DATE = DateTime.Parse(LETTER_CREATION_DATE);
                DateTime DT_LETTER_CREATION_DATE_PLUS_49DYS = DT_LETTER_CREATION_DATE.AddDays(49);
                string LETTER_CREATION_DATE_PLUS_49DYS = DT_LETTER_CREATION_DATE_PLUS_49DYS.ToString("yyyy-MM-dd");

                //Need to get letter dates based on the same logic
                //calculate LETTER_SUBPAY_END_DATE ->  LETTER_SUBPAY_END_DATE= Letter creation date  + 45

                DateTime DT_LETTER_SUBPAY_END_DATE = DT_LETTER_CREATION_DATE.AddDays(45);

                string LETTER_SUBPAY_END_DATE = DT_LETTER_SUBPAY_END_DATE.ToString("yyyy-MM-dd");

                string LETTER_SUB_PAY_THROUGH_DATE_ATT_B = string.Empty;


                //calculate LETTER_SUB_PAY_THROUGH_DATE_ATT_B ->  LETTER_SUB_PAY_THROUGH_DATE_ATT_B= LETTER_SUBPAY_END_DATE - 1
                DateTime DT_LETTER_SUB_PAY_THROUGH_DATE_ATT_B = DT_LETTER_SUBPAY_END_DATE.AddDays(-1);
                LETTER_SUB_PAY_THROUGH_DATE_ATT_B = DT_LETTER_SUB_PAY_THROUGH_DATE_ATT_B.ToString("yyyy-MM-dd");


                string LETTER_BENEFITS_END_DATE_ATT_B = string.Empty;


                //calculate LETTER_BENEFITS_END_DATE_ATT_B ->  Last day of Month(DT_LETTER_CREATION_DATE_PLUS_49DYS)
                DateTime DT_LETTER_BENEFITS_END_DATE_ATT_B = LastDayOfMonth(DT_LETTER_SUBPAY_END_DATE);
                LETTER_BENEFITS_END_DATE_ATT_B = DT_LETTER_BENEFITS_END_DATE_ATT_B.ToString("yyyy-MM-dd");

                

                StringBuilder sb = new StringBuilder();
                sb.Append("INSERT INTO SEV_EMP_DETAILS (EMPLOYEE_ID, LAST_NAME, FIRST_NAME, MIDDLE_NAME, SEGMENT, BUD, BUD1, GRADE, SEX, NOTI_DATE, SEPARATION_EFF_DATE, TOTAL_WEEKS, HOME_ADDRESS_1, HOME_ADDRESS_2, CITY, STATE, ZIP_CODE, SUB_PAY_THROUGH_DATE_ATT_B, BENEFITS_END_DATE_ATT_B, STOCK_OPT, R_STOCK, OUTPLACEMENT_PROGRAM_ATT_B, OUTPLACEMENT_PROVIDER_ATT_B, SUBPAY_END_DATE, LETTER_SUBPAY_END_DATE, LETTER_SUB_PAY_THROUGH_DATE_ATT_B, LETTER_BENEFITS_END_DATE_ATT_B, LETTER_CREATION_DATE, LETTER_CREATION_DATE_PLUS_49DYS, PROGRAM_DATE, RELEASE_DATE, CREATION_DATE, JOB_NUMBER) VALUES ")
.Append("(")
.Append("'" + EMPLOYEE_ID + "', ")
.Append("'" + LAST_NAME + "', ")
.Append("'" + FIRST_NAME + "', ")
.Append("'" + MIDDLE_NAME + "', ")
.Append("'" + SEGMENT + "', ")
.Append("'" + BUD + "', ")
.Append("'" + BUD1 + "', ")
.Append("'" + GRADE_LEVEL + "', ")
.Append("'" + GENDER + "', ")
.Append("'" + NOTI_DATE + "', ")
.Append("'" + SEPARATION_EFF_DATE + "', ")
.Append("" + TOTAL_WEEKS + ", ")
.Append("'" + HOME_ADDRESS_1 + "', ")
.Append("'" + HOME_ADDRESS_2 + "', ")
.Append("'" + CITY + "', ")
.Append("'" + STATE + "', ")
.Append("'" + ZIP_CODE + "', ")
.Append("'" + SUB_PAY_THROUGH_DATE_ATT_B + "', ")
.Append("'" + BENEFITS_END_DATE_ATT_B + "', ")
.Append("'" + BK_SHARES + "', ")
.Append("'" + R_STOCK_ATT_B + "', ")
.Append("'" + OUTPLACEMENT_PROGRAM_ATT_B + "', ")
.Append("'" + OUTPLACEMENT_PROVIDER_ATT_B + "', ")
.Append("'" + SUBPAY_END_DATE + "', ")
.Append("'" + LETTER_SUBPAY_END_DATE + "', ")
.Append("'" + LETTER_SUB_PAY_THROUGH_DATE_ATT_B + "', ")
.Append("'" + LETTER_BENEFITS_END_DATE_ATT_B + "', ")
.Append("'" + LETTER_CREATION_DATE + "', ")
.Append("'" + LETTER_CREATION_DATE_PLUS_49DYS + "', ")
.Append("'" + PROGRAM_DATE + "', ")
.Append("'" + RELEASE_DATE + "', ")
.Append("'" + creation_date + "', ")
.Append("'" + jobNumber + "'")
.Append(")");



                sqlStr = sb.ToString();
                dbOp = new SqlDBOperations();
                dbOp.ExecuteQueryforSql(sqlStr);
                StringBuilder sb1 = new StringBuilder();
                sb1.Append("UPDATE sev_bnym_waves SET ")
                  .Append("JOB_NUMBER = '" + jobNumber + "' ")
                  .Append("WHERE ACTIVE = 'Y'");
                sqlStr = sb1.ToString();
                dbOp.ExecuteQueryforSql(sqlStr);
                return true;
            }
            catch (Exception ex)
            {
                throw;
            }

        }


        public bool UpdateEmployeeDetailsIF(string clientId, Hashtable ht, string memberId, string docNumber, string jobNumber, string creation_date)
        {

            try
            {

                string EMPLOYEE_ID = string.Empty;
                EMPLOYEE_ID = RemoveSpecialCharacters(ht["EMPLOYEE_ID"].ToString());
                if (EMPLOYEE_ID.Length > 20)
                {
                    EMPLOYEE_ID = EMPLOYEE_ID.Remove(20);
                }
                if (EMPLOYEE_ID== "000263705")
                {
                   int i=0;
                }
                DataTable dtML = new DataTable();
                string sqlStr = "select * from SEV_MASTER_LIST  where employee_id= '" + EMPLOYEE_ID + "'";

                SqlDBOperations dbOp = new SqlDBOperations();
                dtML = dbOp.GetSqlDatatable(sqlStr);

                string LAST_NAME = string.Empty;
                LAST_NAME = RemoveSpecialCharacters(ht["LAST_NAME"].ToString());
                if (LAST_NAME.Length > 50)
                {
                    LAST_NAME = LAST_NAME.Remove(50);
                }
                if (string.IsNullOrEmpty(LAST_NAME)&& dtML.Rows.Count>0)
                {
                    LAST_NAME = Convert.ToString(dtML.Rows[0]["LAST_NAME"]);
                }

                string FIRST_NAME = string.Empty;
                FIRST_NAME = RemoveSpecialCharacters(ht["FIRST_NAME"].ToString());
                if (FIRST_NAME.Length > 50)
                {
                    FIRST_NAME = FIRST_NAME.Remove(50);
                }
                if (string.IsNullOrEmpty(FIRST_NAME) && dtML.Rows.Count > 0)
                {
                    FIRST_NAME = Convert.ToString(dtML.Rows[0]["FIRST_NAME"]);
                }
                string MIDDLE_NAME = string.Empty;
                MIDDLE_NAME = RemoveSpecialCharacters(ht["MIDDLE_NAME"].ToString());
                if (MIDDLE_NAME.Length > 50)
                {
                    MIDDLE_NAME = MIDDLE_NAME.Remove(50);
                }
                if (string.IsNullOrEmpty(MIDDLE_NAME) && dtML.Rows.Count > 0)
                {
                    MIDDLE_NAME = Convert.ToString(dtML.Rows[0]["MIDDLE_NAME"]);
                }

                string GRADE_LEVEL = string.Empty;
                GRADE_LEVEL = RemoveSpecialCharacters(ht["GRADE_LEVEL"].ToString());
                if (GRADE_LEVEL.Length > 2)
                {
                    GRADE_LEVEL = GRADE_LEVEL.Remove(2);
                }
                if (string.IsNullOrEmpty(GRADE_LEVEL) && dtML.Rows.Count > 0)
                {
                    GRADE_LEVEL = Convert.ToString(dtML.Rows[0]["GRADE_LEVEL"]);
                }

                string GENDER = string.Empty;
                GENDER = RemoveSpecialCharacters(ht["GENDER"].ToString());
                

                if (string.IsNullOrEmpty(GENDER) && dtML.Rows.Count > 0)
                {
                    GENDER = Convert.ToString(dtML.Rows[0]["GENDER"]);
                }
                if (GENDER.ToLower() == "male")
                {
                    GENDER = "Mr.";
                }

                else if (GENDER.ToLower() == "female")
                {
                    GENDER = "Ms.";
                }
                else
                {
                    GENDER = "";
                }
                if (GENDER.Length > 3)
                {
                    GENDER = GENDER.Remove(3);
                }
                string TOTAL_WEEKS = string.Empty;
                TOTAL_WEEKS = RemoveSpecialCharacters(ht["TOTAL_WEEKS"].ToString());
                if (TOTAL_WEEKS.Length > 10)
                {
                    TOTAL_WEEKS = TOTAL_WEEKS.Remove(10);
                }
                if (string.IsNullOrEmpty(TOTAL_WEEKS) && dtML.Rows.Count > 0)
                {
                    TOTAL_WEEKS = Convert.ToString(dtML.Rows[0]["TOTAL_WEEKS"]);
                }

                string HOME_ADDRESS_1 = string.Empty;
                HOME_ADDRESS_1 = RemoveSpecialCharacters(ht["HOME_ADDRESS_1"].ToString());
                if (HOME_ADDRESS_1.Length > 50)
                {
                    HOME_ADDRESS_1 = HOME_ADDRESS_1.Remove(50);
                }
                if (string.IsNullOrEmpty(HOME_ADDRESS_1) && dtML.Rows.Count > 0)
                {
                    HOME_ADDRESS_1 = Convert.ToString(dtML.Rows[0]["HOME_ADDRESS1"]);
                }

                string HOME_ADDRESS_2 = string.Empty;
                HOME_ADDRESS_2 = RemoveSpecialCharacters(ht["HOME_ADDRESS_2"].ToString());
                if (HOME_ADDRESS_2.Length > 50)
                {
                    HOME_ADDRESS_2 = HOME_ADDRESS_2.Remove(50);
                }
                if (string.IsNullOrEmpty(HOME_ADDRESS_2) && dtML.Rows.Count > 0)
                {
                    HOME_ADDRESS_2 = Convert.ToString(dtML.Rows[0]["HOME_ADDRESS2"]);
                }

                string CITY = string.Empty;
                CITY = RemoveSpecialCharacters(ht["CITY"].ToString());
                if (CITY.Length > 50)
                {
                    CITY = CITY.Remove(50);
                }
                if (string.IsNullOrEmpty(CITY) && dtML.Rows.Count > 0)
                {
                    CITY = Convert.ToString(dtML.Rows[0]["HOME_CITY"]);
                }

                string STATE = string.Empty;
                STATE = RemoveSpecialCharacters(ht["STATE"].ToString());
                if (STATE.Length > 50)
                {
                    STATE = STATE.Remove(50);
                }
                if (string.IsNullOrEmpty(STATE) && dtML.Rows.Count > 0)
                {
                    STATE = Convert.ToString(dtML.Rows[0]["HOME_STATE"]);
                }

                string ZIP_CODE = string.Empty;
                ZIP_CODE = RemoveSpecialCharacters(ht["ZIP_CODE"].ToString());
                if (ZIP_CODE.Length > 10)
                {
                    ZIP_CODE = ZIP_CODE.Remove(10);
                }
                if (string.IsNullOrEmpty(ZIP_CODE) && dtML.Rows.Count > 0)
                {
                    ZIP_CODE = Convert.ToString(dtML.Rows[0]["Home_Postal_Code"]);
                }

                string SEGMENT = string.Empty;
                SEGMENT = RemoveSpecialCharacters(ht["SEGMENT"].ToString());
                if (SEGMENT.Length > 50)
                {
                    SEGMENT = SEGMENT.Remove(50);
                }
                if (string.IsNullOrEmpty(SEGMENT) && dtML.Rows.Count > 0)
                {
                    SEGMENT = Convert.ToString(dtML.Rows[0]["SEGMENT"]);
                }

                DataTable dtBNYMWAVES = new DataTable();
                sqlStr = "select * from SEV_BNYM_WAVES  where active='Y'";

                dbOp = new SqlDBOperations();
                dtBNYMWAVES = dbOp.GetSqlDatatable(sqlStr);

                string PROGRAM_DATE = Convert.ToString(dtBNYMWAVES.Rows[0]["PROGRAM_DATE"]);
                string RELEASE_DATE = Convert.ToString(dtBNYMWAVES.Rows[0]["RELEASE_DATE"]);

                DateTime DT_PROGRAM_DATE = DateTime.Parse(PROGRAM_DATE);

                PROGRAM_DATE = DT_PROGRAM_DATE.ToString("yyyy-MM-dd");


                DateTime DT_RELEASE_DATE = DateTime.Parse(RELEASE_DATE);

                RELEASE_DATE = DT_RELEASE_DATE.ToString("yyyy-MM-dd");

                string SEPARATION_EFF_DATE = string.Empty;
                SEPARATION_EFF_DATE = ht["SEPARATION_EFF_DATE"].ToString();
                if (string.IsNullOrEmpty(SEPARATION_EFF_DATE))
                {
                    SEPARATION_EFF_DATE = Convert.ToString(dtBNYMWAVES.Rows[0]["SEPARATION_DATE"]);

                }
                DateTime DT_SEPARATION_EFF_DATE = DateTime.Parse(SEPARATION_EFF_DATE);

                SEPARATION_EFF_DATE = DT_SEPARATION_EFF_DATE.ToString("yyyy-MM-dd");

                //calculate SUBPAY_END_DATE ->  SUBPAY_END_DATE= Seperation Date + Total weeks

                DateTime DT_SUBPAY_END_DATE = AddWeeks(DT_SEPARATION_EFF_DATE, Convert.ToInt32(TOTAL_WEEKS));

                string SUBPAY_END_DATE = DT_SUBPAY_END_DATE.ToString("yyyy-MM-dd");

                string SUB_PAY_THROUGH_DATE_ATT_B = string.Empty;
                SUB_PAY_THROUGH_DATE_ATT_B = ht["SUB_PAY_THROUGH_DATE_ATT_B"].ToString();

                if (string.IsNullOrEmpty(SUB_PAY_THROUGH_DATE_ATT_B))
                {
                    //calculate SUB_PAY_THROUGH_DATE_ATT_B ->  SUB_PAY_THROUGH_DATE_ATT_B= SUBPAY_END_DATE - 1
                    DateTime DT_SUB_PAY_THROUGH_DATE_ATT_B = DT_SUBPAY_END_DATE.AddDays(-1);
                    SUB_PAY_THROUGH_DATE_ATT_B = DT_SUB_PAY_THROUGH_DATE_ATT_B.ToString("yyyy-MM-dd");
                }

                string BENEFITS_END_DATE_ATT_B = string.Empty;
                BENEFITS_END_DATE_ATT_B = ht["BENEFITS_END_DATE_ATT_B"].ToString();

                if (string.IsNullOrEmpty(BENEFITS_END_DATE_ATT_B))
                {
                    //calculate BENEFITS_END_DATE_ATT_B ->  Last day of Month(SUBPAY_END_DATE)
                    DateTime DT_BENEFITS_END_DATE_ATT_B = LastDayOfMonth(DT_SUBPAY_END_DATE);
                    BENEFITS_END_DATE_ATT_B = DT_BENEFITS_END_DATE_ATT_B.ToString("yyyy-MM-dd");
                }








                string NOTI_DATE = string.Empty;
                NOTI_DATE = RemoveSpecialCharacters(ht["NOTI_DATE"].ToString());
                if (string.IsNullOrEmpty(NOTI_DATE))
                {
                    NOTI_DATE = Convert.ToString(dtBNYMWAVES.Rows[0]["NOTI_DATE"]);
                    DateTime DT_NOTI_DATE = DateTime.Parse(NOTI_DATE);

                    NOTI_DATE = DT_NOTI_DATE.ToString("yyyy-MM-dd");
                }
                string BK_SHARES = string.Empty;
                BK_SHARES = RemoveSpecialCharacters(ht["BK_SHARES"].ToString());
                if (BK_SHARES.Length > 1)
                {
                    BK_SHARES = BK_SHARES.Remove(1);
                }

                string R_STOCK_ATT_B = string.Empty;
                R_STOCK_ATT_B = RemoveSpecialCharacters(ht["R_STOCK_ATT_B"].ToString());
                if (R_STOCK_ATT_B.Length > 1)
                {
                    R_STOCK_ATT_B = R_STOCK_ATT_B.Remove(1);
                }

                string OUTPLACEMENT_PROGRAM_ATT_B = string.Empty;
                OUTPLACEMENT_PROGRAM_ATT_B = RemoveSpecialCharacters(ht["OUTPLACEMENT_PROGRAM_ATT_B"].ToString());
                if (OUTPLACEMENT_PROGRAM_ATT_B.Length > 50)
                {
                    OUTPLACEMENT_PROGRAM_ATT_B = OUTPLACEMENT_PROGRAM_ATT_B.Remove(50);
                }

                string OUTPLACEMENT_PROVIDER_ATT_B = string.Empty;
                OUTPLACEMENT_PROVIDER_ATT_B = RemoveSpecialCharacters(ht["OUTPLACEMENT_PROVIDER_ATT_B"].ToString());
                if (OUTPLACEMENT_PROVIDER_ATT_B.Length > 50)
                {
                    OUTPLACEMENT_PROVIDER_ATT_B = OUTPLACEMENT_PROVIDER_ATT_B.Remove(50);
                }


                DataTable dtBPR = new DataTable();
                sqlStr = "select * from SEV_BASE_POP_REPORT  where employee_id= '" + EMPLOYEE_ID + "'";

                dbOp = new SqlDBOperations();
                dtBPR = dbOp.GetSqlDatatable(sqlStr);

                string BUD = Convert.ToString(dtBPR.Rows[0]["BUD"]);
                string BUD1 = Convert.ToString(dtBPR.Rows[0]["BUD1"]);

                if (creation_date == "")
                {
                    creation_date = "NULL";
                }

                //Letter creation date is notice date
                string LETTER_CREATION_DATE = NOTI_DATE;
                DateTime DT_LETTER_CREATION_DATE = DateTime.Parse(LETTER_CREATION_DATE);
                DateTime DT_LETTER_CREATION_DATE_PLUS_49DYS = DT_LETTER_CREATION_DATE.AddDays(49);
                string LETTER_CREATION_DATE_PLUS_49DYS = DT_LETTER_CREATION_DATE_PLUS_49DYS.ToString("yyyy-MM-dd");

                //Need to get letter dates based on the same logic
                //calculate LETTER_SUBPAY_END_DATE ->  LETTER_SUBPAY_END_DATE= Letter creation date  + 45

                DateTime DT_LETTER_SUBPAY_END_DATE = DT_LETTER_CREATION_DATE.AddDays(45);

                string LETTER_SUBPAY_END_DATE = DT_LETTER_SUBPAY_END_DATE.ToString("yyyy-MM-dd");

                string LETTER_SUB_PAY_THROUGH_DATE_ATT_B = string.Empty;


                //calculate LETTER_SUB_PAY_THROUGH_DATE_ATT_B ->  LETTER_SUB_PAY_THROUGH_DATE_ATT_B= LETTER_SUBPAY_END_DATE - 1
                DateTime DT_LETTER_SUB_PAY_THROUGH_DATE_ATT_B = DT_LETTER_SUBPAY_END_DATE.AddDays(-1);
                LETTER_SUB_PAY_THROUGH_DATE_ATT_B = DT_LETTER_SUB_PAY_THROUGH_DATE_ATT_B.ToString("yyyy-MM-dd");


                string LETTER_BENEFITS_END_DATE_ATT_B = string.Empty;


                //calculate LETTER_BENEFITS_END_DATE_ATT_B ->  Last day of Month(DT_LETTER_CREATION_DATE_PLUS_49DYS)
                DateTime DT_LETTER_BENEFITS_END_DATE_ATT_B = LastDayOfMonth(DT_LETTER_SUBPAY_END_DATE);
                LETTER_BENEFITS_END_DATE_ATT_B = DT_LETTER_BENEFITS_END_DATE_ATT_B.ToString("yyyy-MM-dd");



                StringBuilder sb = new StringBuilder();
                sb.Append("UPDATE SEV_EMP_DETAILS SET ")
                  .Append("LAST_NAME = '" + LAST_NAME + "', ")
                  .Append("FIRST_NAME = '" + FIRST_NAME + "', ")
                  .Append("MIDDLE_NAME = '" + MIDDLE_NAME + "', ")
                  .Append("SEGMENT = '" + SEGMENT + "', ")
                  .Append("BUD = '" + BUD + "', ")
                  .Append("BUD1 = '" + BUD1 + "', ")
                  .Append("GRADE = '" + GRADE_LEVEL + "', ")
                  .Append("SEX = '" + GENDER + "', ")
                  .Append("NOTI_DATE = '" + NOTI_DATE + "', ")
                  .Append("SEPARATION_EFF_DATE = '" + SEPARATION_EFF_DATE + "', ")
                  .Append("TOTAL_WEEKS = " + TOTAL_WEEKS + ", ")
                  .Append("HOME_ADDRESS_1 = '" + HOME_ADDRESS_1 + "', ")
                  .Append("HOME_ADDRESS_2 = '" + HOME_ADDRESS_2 + "', ")
                  .Append("CITY = '" + CITY + "', ")
                  .Append("STATE = '" + STATE + "', ")
                  .Append("ZIP_CODE = '" + ZIP_CODE + "', ")
                  .Append("SUB_PAY_THROUGH_DATE_ATT_B = '" + SUB_PAY_THROUGH_DATE_ATT_B + "', ")
                  .Append("BENEFITS_END_DATE_ATT_B = '" + BENEFITS_END_DATE_ATT_B + "', ")
                  .Append("STOCK_OPT = '" + BK_SHARES + "', ")
                  .Append("R_STOCK = '" + R_STOCK_ATT_B + "', ")
                  .Append("OUTPLACEMENT_PROGRAM_ATT_B = '" + OUTPLACEMENT_PROGRAM_ATT_B + "', ")
                  .Append("OUTPLACEMENT_PROVIDER_ATT_B = '" + OUTPLACEMENT_PROVIDER_ATT_B + "', ")
                  .Append("SUBPAY_END_DATE = '" + SUBPAY_END_DATE + "', ")
                  .Append("LETTER_SUBPAY_END_DATE = '" + LETTER_SUBPAY_END_DATE + "', ")
                  .Append("LETTER_SUB_PAY_THROUGH_DATE_ATT_B = '" + LETTER_SUB_PAY_THROUGH_DATE_ATT_B + "', ")
                  .Append("LETTER_BENEFITS_END_DATE_ATT_B = '" + LETTER_BENEFITS_END_DATE_ATT_B + "', ")
                  .Append("LETTER_CREATION_DATE = '" + LETTER_CREATION_DATE + "', ")
                  .Append("LETTER_CREATION_DATE_PLUS_49DYS = '" + LETTER_CREATION_DATE_PLUS_49DYS + "', ")
                  .Append("PROGRAM_DATE = '" + PROGRAM_DATE + "', ")
                  .Append("RELEASE_DATE = '" + RELEASE_DATE + "', ")
                  .Append("CREATION_DATE = '" + creation_date + "', ")
                  .Append("JOB_NUMBER = '" + jobNumber + "' ")
                  .Append("WHERE EMPLOYEE_ID = '" + EMPLOYEE_ID + "'");




                sqlStr = sb.ToString();
                dbOp = new SqlDBOperations();
                dbOp.ExecuteQueryforSql(sqlStr);
                StringBuilder sb1 = new StringBuilder();
                sb1.Append("UPDATE sev_bnym_waves SET ")
                  .Append("JOB_NUMBER = '" + jobNumber + "' ")
                  .Append("WHERE ACTIVE = 'Y'");
                sqlStr = sb1.ToString();
                dbOp.ExecuteQueryforSql(sqlStr);
                return true;
            }
            catch (Exception ex)
            {
                throw;
            }

        }
        public DateTime AddWeeks(DateTime dateTime, int numberOfWeeks)
        {
            return dateTime.AddDays(numberOfWeeks * 7);
        }
        public DateTime LastDayOfMonth(DateTime dt)
        {
            DateTime ss = new DateTime(dt.Year, dt.Month, 1);
            return ss.AddMonths(1).AddDays(-1);
        }
        public bool InsertEmployeeDetails(string clientId, Hashtable ht, string memberId,string docNumber,string jobNumber,string creation_date)
        {

            try
            {
                string plan_year = string.Empty;

                plan_year = RemoveSpecialCharacters(ht["plan_year"].ToString());
                if (plan_year.Length > 4)
                {
                    plan_year = plan_year.Remove(4);
                }
                string worker_type = string.Empty;

                worker_type = RemoveSpecialCharacters(ht["worker_type"].ToString());
                if (worker_type.Length > 30)
                {
                    worker_type = worker_type.Remove(30);
                }

                string employee_type = string.Empty;

                employee_type = RemoveSpecialCharacters(ht["employee_type"].ToString());
                if (employee_type.Length > 30)
                {
                    employee_type = employee_type.Remove(30);
                }

                string employee_id = string.Empty;
                employee_id = RemoveSpecialCharacters(ht["employee_id"].ToString());
                if (employee_id.Length > 20)
                {
                    employee_id = employee_id.Remove(20);
                }

               

                string legal_name = string.Empty;
                legal_name = RemoveSpecialCharacters(ht["legal_name"].ToString());
                if (legal_name.Length > 100)
                {
                    legal_name = legal_name.Remove(100);
                }
                string DOB = string.Empty;

                DOB = ht["DOB"].ToString();

                string continous_serv_dt = string.Empty;

                continous_serv_dt = ht["continous_serv_dt"].ToString();
                string ret_elig_dt = string.Empty;

                ret_elig_dt = ht["ret_elig_dt"].ToString();

                string tot_base_pay_annual = string.Empty;
                tot_base_pay_annual = ht["tot_base_pay_annual"].ToString();
                if (tot_base_pay_annual.Length > 13)
                {
                    tot_base_pay_annual = tot_base_pay_annual.Remove(13);
                }

                string comp_plan_salary_plans = string.Empty;
                comp_plan_salary_plans = ht["comp_plan_salary_plans"].ToString();
                if (comp_plan_salary_plans.Length > 50)
                {
                    comp_plan_salary_plans = comp_plan_salary_plans.Remove(50);
                }

                string amount_salary_plans = string.Empty;
                amount_salary_plans = ht["amount_salary_plans"].ToString();
                if (amount_salary_plans.Length > 13)
                {
                    amount_salary_plans = amount_salary_plans.Remove(13);
                }

                string comp_plan_hourly_plans = string.Empty;
                comp_plan_hourly_plans = RemoveSpecialCharacters(ht["comp_plan_hourly_plans"].ToString());
                if (comp_plan_hourly_plans.Length > 50)
                {
                    comp_plan_hourly_plans = comp_plan_hourly_plans.Remove(50);
                }


                string amount_hourly_plans = string.Empty;
                amount_hourly_plans = ht["amount_hourly_plans"].ToString();
                if (amount_hourly_plans.Length > 13)
                {
                    amount_hourly_plans = amount_hourly_plans.Remove(13);
                }

                string bonus_plans_assmt_dtls = string.Empty;
                bonus_plans_assmt_dtls = ht["bonus_plans_assmt_dtls"].ToString();
                if (bonus_plans_assmt_dtls.Length > 100)
                {
                    bonus_plans_assmt_dtls = bonus_plans_assmt_dtls.Remove(100);
                }

                string schd_weekly_hrs = string.Empty;
                schd_weekly_hrs = ht["schd_weekly_hrs"].ToString();
                if (schd_weekly_hrs.Length > 6)
                {
                    schd_weekly_hrs = schd_weekly_hrs.Remove(6);
                }

                string comp_grade = string.Empty;
                comp_grade = ht["comp_grade"].ToString();
                if (comp_grade.Length > 69)
                {
                    comp_grade = comp_grade.Remove(69);
                }
                string comp_grade_profile = string.Empty;
                comp_grade_profile = ht["comp_grade_profile"].ToString();
                if (comp_grade_profile.Length > 30)
                {
                    comp_grade_profile = comp_grade_profile.Remove(30);
                }
                string business_unit = string.Empty;
                business_unit = ht["business_unit"].ToString();
                if (business_unit.Length > 49)
                {
                    business_unit = business_unit.Remove(49);
                }

                string cost_center = string.Empty;
                cost_center = ht["cost_center"].ToString();
                if (cost_center.Length > 69)
                {
                    cost_center = cost_center.Remove(69);
                }


                string time_type = string.Empty;
                time_type = ht["time_type"].ToString();
                if (time_type.Length > 19)
                {
                    time_type = time_type.Remove(19);
                }


                string job_code = string.Empty;
                job_code = ht["job_code"].ToString();
                if (job_code.Length > 19)
                {
                    job_code = job_code.Remove(19);
                }


                string location = string.Empty;
                location = ht["location"].ToString();
                if (location.Length > 99)
                {
                    location = location.Remove(99);
                }


                string company_origin_reg = string.Empty;
                company_origin_reg = ht["company_origin_reg"].ToString();
                if (company_origin_reg.Length > 49)
                {
                    company_origin_reg = company_origin_reg.Remove(49);
                }


                string home_addr1 = string.Empty;
                home_addr1 = ht["home_addr1"].ToString();
                if (home_addr1.Length > 100)
                {
                    home_addr1 = home_addr1.Remove(100);
                }
                string home_addr2 = string.Empty;
                home_addr2 = ht["home_addr2"].ToString();
                if (home_addr2.Length > 100)
                {
                    home_addr2 = home_addr2.Remove(100);
                }

                string home_addr3 = string.Empty;
                home_addr3 = ht["home_addr3"].ToString();
                if (home_addr3.Length > 100)
                {
                    home_addr3 = home_addr3.Remove(100);
                }
                string phone_home = string.Empty;
                phone_home = ht["phone_home"].ToString();
                if (phone_home.Length > 19)
                {
                    phone_home = phone_home.Remove(19);
                }


                string p_DOB = ht["DOB"].ToString();
                if (p_DOB == "")
                {
                    p_DOB = "NULL";
                }
                else
                {
                    p_DOB="TO_DATE(replace(replace('" + p_DOB + "', ',', ''), ' ', '') , 'MM/DD/YY')";
                }
                string p_CONTINOUS_SERV_DT = ht[("CONTINOUS_SERV_DT").ToLower()].ToString();
                if (p_CONTINOUS_SERV_DT == "")
                {
                    p_CONTINOUS_SERV_DT = "NULL";
                }
                else
                {
                    p_CONTINOUS_SERV_DT = "TO_DATE(replace(replace('" + p_CONTINOUS_SERV_DT + "', ',', ''), ' ', '') , 'MM/DD/YY')";
                }
                string p_RET_ELIG_DT = ht[("RET_ELIG_DT").ToLower()].ToString();
                if (p_RET_ELIG_DT == "")
                {
                    p_RET_ELIG_DT = "NULL";
                }
                else
                {
                    p_RET_ELIG_DT = "TO_DATE(replace(replace('" + p_RET_ELIG_DT + "', ',', ''), ' ', '') , 'MM/DD/YY')";
                }

                string p_TOT_BASE_PAY_ANNUAL = ht[("TOT_BASE_PAY_ANNUAL").ToLower()].ToString();
                if (p_TOT_BASE_PAY_ANNUAL == "")
                {
                    p_TOT_BASE_PAY_ANNUAL = "NULL";
                }
                else
                {
                    p_TOT_BASE_PAY_ANNUAL = ht[("TOT_BASE_PAY_ANNUAL").ToLower()].ToString().Replace(",", "");
                }
                string p_AMOUNT_SALARY_PLANS = ht[("AMOUNT_SALARY_PLANS").ToLower()].ToString();
                if (p_AMOUNT_SALARY_PLANS == "")
                {
                    p_AMOUNT_SALARY_PLANS = "NULL";
                }
                else
                {
                    p_AMOUNT_SALARY_PLANS = ht[("AMOUNT_SALARY_PLANS").ToLower()].ToString().Replace(",", "");
                }
                string p_AMOUNT_HOURLY_PLANS = ht[("AMOUNT_HOURLY_PLANS").ToLower()].ToString();
                if (p_AMOUNT_HOURLY_PLANS == "")
                {
                    p_AMOUNT_HOURLY_PLANS = "NULL";
                }
                else
                {
                    p_AMOUNT_HOURLY_PLANS = ht[("AMOUNT_HOURLY_PLANS").ToLower()].ToString().Replace(",", "");
                }
                string p_SCHD_WEEKLY_HRS = ht[("SCHD_WEEKLY_HRS").ToLower()].ToString();
                if (p_SCHD_WEEKLY_HRS == "")
                {
                    p_SCHD_WEEKLY_HRS = "NULL";
                }
                else
                {
                    p_SCHD_WEEKLY_HRS = ht[("SCHD_WEEKLY_HRS").ToLower()].ToString().Replace(",", "");
                }

                if (creation_date == "")
                {
                    creation_date = "NULL";
                }
                else
                {
                    creation_date = "TO_DATE(replace(replace('" + creation_date + "', ',', ''), ' ', '') , 'MM/DD/YY')";
                }

                StringBuilder sb = new StringBuilder();
                string COMMA = ",";
                sb.Append("INSERT INTO ISS4_BNYM_EMP_DETAILS (CLIENT_ID, PLAN_YEAR, WORKER_TYPE, EMPLOYEE_TYPE, EMPLOYEE_ID,LEGAL_NAME, DOB, CONTINOUS_SERV_DT, RET_ELIG_DT, TOT_BASE_PAY_ANNUAL,COMP_PLAN_SALARY_PLANS, AMOUNT_SALARY_PLANS, COMP_PLAN_HOURLY_PLANS, AMOUNT_HOURLY_PLANS, BONUS_PLANS_ASSMT_DTLS,SCHD_WEEKLY_HRS, COMP_GRADE, COMP_GRADE_PROFILE, BUSINESS_UNIT, COST_CENTER,TIME_TYPE, JOB_CODE, LOCATION, COMPANY_ORIGIN_REG, HOME_ADDR1, HOME_ADDR2, HOME_ADDR3, PHONE_HOME,ORIG_IMPORTED_DATE, JOB_NUMBER_EXE1,UPLOADED_DOCNUMBER)  VALUES ")
                 .Append("(")
                        .Append(clientId)
                        .Append(COMMA)
                        .Append("'" + plan_year + "'")
                        .Append(COMMA)
                        .Append("'" + worker_type + "'")
                        .Append(COMMA)
                        .Append("'" + employee_type + "'")
                        .Append(COMMA)
                        .Append("'" + employee_id + "'")
                        .Append(COMMA)
                        .Append("'" + legal_name + "'")
                        .Append(COMMA)
                        .Append(p_DOB)
                        .Append(COMMA)
                        .Append(p_CONTINOUS_SERV_DT)
                        .Append(COMMA)
                        .Append(p_RET_ELIG_DT)
                        .Append(COMMA)
                        .Append(p_TOT_BASE_PAY_ANNUAL)
                        .Append(COMMA)
                        .Append("'" + comp_plan_salary_plans+ "'")
                        .Append(COMMA)
                        .Append(p_AMOUNT_SALARY_PLANS)
                        .Append(COMMA)
                        .Append("'" + comp_plan_hourly_plans + "'")
                        .Append(COMMA)
                        .Append(p_AMOUNT_HOURLY_PLANS)
                        .Append(COMMA)
                        .Append("'" + bonus_plans_assmt_dtls + "'")
                        .Append(COMMA)
                        .Append(p_SCHD_WEEKLY_HRS)
                        .Append(COMMA)
                        .Append("'" + comp_grade + "'")
                        .Append(COMMA)
                        .Append("'" + comp_grade_profile + "'")
                        .Append(COMMA)
                        .Append("'" + business_unit + "'")
                        .Append(COMMA)
                        .Append("'" + cost_center + "'")
                        .Append(COMMA)
                        .Append("'" + time_type + "'")
                        .Append(COMMA)
                        .Append("'" + job_code + "'")
                        .Append(COMMA)
                        .Append("'" + location + "'")
                        .Append(COMMA)
                        .Append("'" + company_origin_reg + "'")
                         .Append(COMMA)
                        .Append("'" +home_addr1 + "'")
                        .Append(COMMA)
                        .Append("'" + home_addr2 + "'")
                        .Append(COMMA)
                        .Append("'" + home_addr3 + "'")
                        .Append(COMMA)
                        .Append("'" + phone_home + "'")
                        .Append(COMMA)
                        .Append(creation_date)       
                        .Append(COMMA)
                        .Append(jobNumber)
                        .Append(COMMA)
                        .Append(docNumber)
                        .Append(")");
                string sqlStr = sb.ToString();
                OracleDBOperations dbOp = new OracleDBOperations();
                bool result = dbOp.ExecuteQueryforOracle(sqlStr);
               
                return result;
            }
            catch (Exception ex)
            {
                throw;
            }

        }
        public bool InsertMemberSystems(string client_id, string member_id, string password, string v_system_id)
        {
            string str = "INSERT INTO ISS3_MEMBER_SYSTEMS(MEMBER_ID, SYSTEM_ID,PASSWORD_TYPE, INITIAL_PASSWORD, PASSWORD,TIMES_FAILED,STATUS_ID)  VALUES (:MEMBER_ID, :SYSTEM_ID,'U', :INITIAL_PASSWORD, :PASSWORD,0,0)";
            Hashtable paramList = new Hashtable();
            paramList.Add(":MEMBER_ID", member_id);
            paramList.Add(":SYSTEM_ID", v_system_id);
            paramList.Add(":INITIAL_PASSWORD", password);            
            paramList.Add(":PASSWORD", password);
            OracleDBOperations dbOp = new OracleDBOperations();
            bool result = dbOp.ExecuteQueryforOracle(str, paramList);
            return result;
        }
        public bool updateUser( string clientId,Hashtable ht)
        {

            try
            {
                string first_name = string.Empty;
                first_name = RemoveSpecialCharacters(ht["FirstName"].ToString());
                if (first_name.Length > 50)
                {
                    first_name = first_name.Remove(50);
                }
                
                string LastName = string.Empty;
                LastName = RemoveSpecialCharacters(ht["LastName"].ToString());
                if (LastName.Length > 36)
                {
                    LastName = LastName.Remove(36);
                }
                
                string City = string.Empty;
                City = RemoveSpecialCharacters(ht["City"].ToString());
                if (City.Length > 20)
                {
                    City = City.Remove(20);
                }

                string Address1 = string.Empty;
                Address1 = RemoveSpecialCharacters(ht["Address1"].ToString());
                if (Address1.Length > 35)
                {
                    Address1 = Address1.Remove(35);
                }

                string Address2 = string.Empty;
                Address2 = RemoveSpecialCharacters(ht["Address2"].ToString());
                if (Address2.Length > 35)
                {
                    Address2 = Address2.Remove(35);
                }

                string State = string.Empty;
                State = RemoveSpecialCharacters(ht["State"].ToString());
                if (State.Length > 2)
                {
                    State = State.Remove(2);
                }

                string Postal_Code = string.Empty;
                Postal_Code = RemoveSpecialCharacters(ht["Postal_Code"].ToString());
                if (Postal_Code.Length > 12)
                {
                    Postal_Code = Postal_Code.Remove(12);
                }

                string CountryCode = string.Empty;
                CountryCode = RemoveSpecialCharacters(ht["CountryCode"].ToString());
                if (CountryCode.Length > 3)
                {
                    CountryCode = CountryCode.Remove(3);
                }


                string Email = string.Empty;
                Email = ht["Email"].ToString();
                if (Email.Length > 128)
                {
                    Email = Email.Remove(128);
                }

                StringBuilder sb = new StringBuilder();
                sb.Append("CALL bns_sp.Update_member(")
                    .Append(clientId)
                    .Append(",'")
                    .Append(ht["USER_ID"])
                    .Append("','")
                    .Append(LastName.Replace("'", "''"))
                    .Append("','");
               
                sb.Append(first_name)
                    .Append("',")
                    .Append(ht["ACCESS_CONTROL_GROUP"])
                    .Append(",'")
                    .Append(Address1.Replace("'", "''"))
                    .Append("','")
                    .Append(Address2.Replace("'", "''"))
                    .Append("','")
                    .Append(City.Replace("'", "''"))
                    .Append("','")
                    .Append(State.Replace("'", "''"))
                    .Append("','")
                    .Append(Postal_Code.Replace("'", "''"))
                    .Append("','")
                    .Append(CountryCode.Replace("'", "''"))
                    .Append("','")
                    .Append(Email.Replace("'", "''"))
                    .Append("')");
                string strSql = sb.ToString();
                OracleDBOperations dbOp = new OracleDBOperations();
              bool result = dbOp.ExecuteQueryforOracle(strSql);
           //    AddMemberService(clientId, baseID, "13");
              //  AddMemberService(clientId, baseID, "25");
                    InsertServiceIds(ht, clientId);
                return true;
            }
            catch (Exception ex)
            {
                throw;
            }

        }
        public  bool InsertServiceIds(Hashtable ht, string client_id)
        {
            try
            {
                // insert service id
                const char TILD = '~';
                string baseid = string.Empty;
                string serviceIds = string.Empty;
                string systemId = string.Empty;
                string securityGroupId = string.Empty;
                string redirMemberId = string.Empty;

                if (ht.ContainsKey("SYSTEM_ID"))
                {
                    systemId = ht["SYSTEM_ID"].ToString();
                }
                if (ht.ContainsKey("BaseId"))
                {
                    baseid = ht["BaseId"].ToString();
                }
                if (ht.ContainsKey("SECURITY_GROUP_ID"))
                {
                    securityGroupId = ht["SECURITY_GROUP_ID"].ToString();
                }
                if (ht.ContainsKey("SERVICE_ID"))
                {
                    serviceIds = ht["SERVICE_ID"].ToString();
                }
                string memberID = GetMemberID(client_id, baseid);
                if (!string.IsNullOrEmpty(memberID))
                {
                    if (!string.IsNullOrEmpty(serviceIds))
                    {
                        string[] serviceIdList = serviceIds.Split(TILD);
                        StringBuilder sb;
                        foreach (string serviceId in serviceIdList)
                        {
                            if(!isExistService(serviceId,memberID))
                            {
                            sb = new StringBuilder();
                            sb.Append("CALL iss3_admin.addService2Member(")
                                .Append(client_id)
                                .Append(",'")
                                .Append(baseid)
                                .Append("',")
                                .Append(systemId)
                                .Append(",")
                                .Append(securityGroupId)
                                .Append(",")
                                .Append(serviceId)
                                .Append(",'")
                                .Append(baseid)
                                .Append("')");
                            string strSql = sb.ToString();
                            OracleDBOperations dataOp = new OracleDBOperations();
                            dataOp.ExecuteQueryforOracle(strSql);// add service id
                            }
                        }
                    }
                    
                }
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
       
        }

        public bool isExistService(string serviceid, string memberid)
        {
            try
            {
                string count = string.Empty;
                if (string.IsNullOrEmpty(serviceid.Trim()) || memberid.ToString().Equals("0"))
                {
                    return false;
                }
                string sqlStr = "SELECT count(*) from ISS3_MEMBER_SECURITY_GROUPS where member_id=:member_id and service_id=:service_id";
                Hashtable paramList = new Hashtable();
                paramList.Add(":member_id", memberid);
                paramList.Add(":service_id", serviceid);
                OracleDBOperations dbOp = new OracleDBOperations();
                count = dbOp.GetOracleResult(sqlStr, paramList);
                bool result = Convert.ToInt32(count) > 0 ? true : false;
                return result;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
        public bool AddMemberService(string clientId, string baseId, string serviceId)
        {
            try
            {
                string strSql = string.Empty;
                OracleDBOperations dbOp = new OracleDBOperations();
                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder = new StringBuilder();
                queryBuilder.Append("CALL Iss3_Admin.addService2Member(")
                .Append(clientId)
                .Append(",'")
                .Append(baseId)
                .Append("',2,1,")
                .Append(serviceId)
                .Append(",'")
                .Append(baseId)
                .Append("')");
                strSql = queryBuilder.ToString();
                dbOp.ExecuteQueryforOracle(strSql);
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }

        }

        //Add Service


        public bool IsExistProfile(string clientId, string baseId,string myorder)
        {
            try
            {
                string count = string.Empty;
                bool isExist = false;
                if (string.IsNullOrEmpty(baseId.Trim()) || baseId.ToString().Equals("0"))
                {
                    return true;
                }
                string sqlStr = "select count(*) as numbers from  Iss4_member_profile where client_id=:CLIENT_ID and my_order=:my_order and base_id=:BASEID and profile_id=:profile_id";
                Hashtable paramList = new Hashtable();
                paramList.Add(":CLIENT_ID", clientId);
                paramList.Add(":my_order", myorder);
                paramList.Add(":BASEID", baseId);
                paramList.Add(":profile_id", "12");
                OracleDBOperations dbOp = new OracleDBOperations();
                count = dbOp.GetOracleResult(sqlStr, paramList);

                isExist = Convert.ToInt32(count) > 0 ? true : false;
                return isExist;
            }
            catch (Exception ex) 
            {
                return false;
            }

        }
        public bool InsertMemberProfile(string clientId, string profileId, string baseId, string myOdrer, string fieldText)
        {
            try
            {
                string sqlstr = "INSERT into Iss4_member_profile (client_id, profile_id, base_id, my_order, field_text) values (:client_id,:profile_id,:base_id,:my_order,:field_text)";
                Hashtable paramList = new Hashtable();
                paramList.Add(":client_id", clientId);
                paramList.Add(":profile_id", profileId);
                paramList.Add(":base_id", baseId);
                paramList.Add(":my_order", myOdrer);
                paramList.Add(":field_text", fieldText);

                OracleDBOperations dbOp = new OracleDBOperations();
                bool result = dbOp.ExecuteQueryforOracle(sqlstr, paramList);
                if (myOdrer == "0")
                {
                    
                    var lastSpaceIndex = fieldText.LastIndexOf(" ");
                    var firstString = fieldText.Substring(0, lastSpaceIndex); // INAGX4
                    string first_name = firstString;
                    string lastWord = fieldText.Split(' ').Last();
                    string last_name = lastWord;
                     sqlstr = "INSERT into Iss4_member_profile (client_id, profile_id, base_id, my_order, field_text) values (:client_id,:profile_id,:base_id,:my_order,:field_text)";
                    Hashtable paramList1 = new Hashtable();
                    paramList1.Add(":client_id", clientId);
                    paramList1.Add(":profile_id", profileId);
                    paramList1.Add(":base_id", baseId);
                    paramList1.Add(":my_order", "6");
                    paramList1.Add(":field_text", first_name);

                    OracleDBOperations dbOp1 = new OracleDBOperations();
                    if (!IsExistProfile(clientId, baseId, "6"))
                    {
                        bool result1 = dbOp.ExecuteQueryforOracle(sqlstr, paramList1);
                    }
                     sqlstr = "INSERT into Iss4_member_profile (client_id, profile_id, base_id, my_order, field_text) values (:client_id,:profile_id,:base_id,:my_order,:field_text)";
                    Hashtable paramList2 = new Hashtable();
                    paramList2.Add(":client_id", clientId);
                    paramList2.Add(":profile_id", profileId);
                    paramList2.Add(":base_id", baseId);
                    paramList2.Add(":my_order", "5");
                    paramList2.Add(":field_text", last_name);

                    OracleDBOperations dbOp2 = new OracleDBOperations();
                    if (!IsExistProfile(clientId, baseId, "5"))
                    {
                        bool result2 = dbOp.ExecuteQueryforOracle(sqlstr, paramList2);
                    }
                }
                return result;
            }
            catch (Exception Exception){
                throw;
            }

        }

        public bool UpdateMemberProfile(string clientId, string profileId, string baseId, string myOdrer, string fieldText)
        {
            try
            {
                
                //fieldText = RemoveSpecialCharacters(fieldText);
                string sqlstr = "UPDATE Iss4_member_profile set field_text=:field_text where client_id=:client_id and profile_id=:profile_id  and base_id=:base_id and my_order=:my_order";
                Hashtable paramList = new Hashtable();
                paramList.Add(":client_id", clientId);
                paramList.Add(":profile_id", profileId);
                paramList.Add(":base_id", baseId);
                paramList.Add(":my_order", myOdrer);
                paramList.Add(":field_text", fieldText);

                OracleDBOperations dbOp = new OracleDBOperations();
                bool result = dbOp.ExecuteQueryforOracle(sqlstr, paramList);
                if (myOdrer == "0")
                {
                    var lastSpaceIndex = fieldText.LastIndexOf(" ");
                    var firstString = fieldText.Substring(0, lastSpaceIndex); // INAGX4
                    string first_name = firstString;
                    string lastWord = fieldText.Split(' ').Last();
                    string last_name = lastWord;
                    sqlstr = "UPDATE Iss4_member_profile set field_text=:field_text where client_id=:client_id and profile_id=:profile_id  and base_id=:base_id and my_order=:my_order";
                    Hashtable paramList1 = new Hashtable();
                    paramList1.Add(":client_id", clientId);
                    paramList1.Add(":profile_id", profileId);
                    paramList1.Add(":base_id", baseId);
                    paramList1.Add(":my_order", "6");
                    paramList1.Add(":field_text", first_name);

                    OracleDBOperations dbOp1 = new OracleDBOperations();
                    bool result1 = dbOp.ExecuteQueryforOracle(sqlstr, paramList1);
                    sqlstr = "UPDATE Iss4_member_profile set field_text=:field_text where client_id=:client_id and profile_id=:profile_id  and base_id=:base_id and my_order=:my_order";
                    Hashtable paramList2 = new Hashtable();
                    paramList2.Add(":client_id", clientId);
                    paramList2.Add(":profile_id", profileId);
                    paramList2.Add(":base_id", baseId);
                    paramList2.Add(":my_order", "5");
                    paramList2.Add(":field_text", last_name);

                    OracleDBOperations dbOp2 = new OracleDBOperations();
                    bool result2 = dbOp.ExecuteQueryforOracle(sqlstr, paramList2);
                }
               

                return result;
            }
            catch (Exception ex)
            {
                throw;
            }

        }
        public  bool InsertISS4ImportSummary(string client_id, string run_mode, string profile_id,string jobNumber)
        {
            try
            {
                OracleDBOperations dataOp = new OracleDBOperations();
                StringBuilder sb = new StringBuilder();
                sb.Append("Insert into ISS4_IMPORT_SUMMARY (JOB_NUMBER, CLIENT_ID, CREATION_DATE, COMMENTS_TXT) ")
                .Append(" Values( ")
                .Append(jobNumber)
                .Append(",")
                .Append(client_id)
                .Append(", sysdate , '")
                .Append("Auto update with run mode - ")
                .Append(run_mode)
                .Append(" and profile id - ")
                .Append(profile_id)
                .Append("')");
                dataOp.ExecuteQueryforOracle(sb.ToString());
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public  bool UpdateMemberSystems(Hashtable ht, string client_id)
        {
            try
            {
                OracleDBOperations dataOp = new OracleDBOperations();
                string systemId = string.Empty;
                string strSql = string.Empty;
                string base_id = string.Empty;
                string memberId = string.Empty;
                DataSet memberSystemsDS;
                string challengeQuestion = string.Empty;
                string challengeResponse = string.Empty;
                string challengeQuestionDb = string.Empty;
                string challengeResponseDb = string.Empty;
                if (ht.ContainsKey("CHALLENGE_QUESTION"))
                {
                    challengeQuestion = ht["CHALLENGE_QUESTION"].ToString();
                }
                if (ht.ContainsKey("CHALLENGE_RESPONSE"))
                {
                    challengeResponse = ht["CHALLENGE_RESPONSE"].ToString();
                }
                if (ht.ContainsKey("SYSTEM_ID"))
                {
                    systemId = ht["SYSTEM_ID"].ToString();
                }
                if (ht.ContainsKey("BASE_ID"))
                {
                    base_id = ht["BASE_ID"].ToString();
                }
                DataSet memberDS = new DataSet();
                memberDS = GetMemberData(client_id, base_id);
                if (memberDS.Tables.Count > 0 && memberDS.Tables[0].Rows.Count > 0) //member exists
                {
                    memberId = memberDS.Tables[0].Rows[0]["MEMBER_ID"].ToString();
                }
                if (!string.IsNullOrEmpty(memberId) && !string.IsNullOrEmpty(systemId))
                {
                    strSql = "select CHALLENGE_QUESTION, CHALLENGE_RESPONSE  FROM ISS3_MEMBER_SYSTEMS WHERE MEMBER_ID=" + memberId + " AND SYSTEM_ID=" + systemId;
                    memberSystemsDS = dataOp.GetDSResult(strSql);
                    if (memberSystemsDS.Tables.Count > 0 && memberSystemsDS.Tables[0].Rows.Count > 0) //
                    {
                        challengeQuestionDb = memberSystemsDS.Tables[0].Rows[0]["CHALLENGE_QUESTION"].ToString();
                        challengeResponseDb = memberSystemsDS.Tables[0].Rows[0]["CHALLENGE_RESPONSE"].ToString();
                        if (!string.IsNullOrEmpty(challengeQuestion) && !string.IsNullOrEmpty(challengeResponse))//only if new values are not null
                        {
                            if (!challengeQuestionDb.Equals(challengeQuestion) || !challengeResponseDb.Equals(challengeResponse)) // compares new values with existing
                            {
                                strSql = "UPDATE ISS3_MEMBER_SYSTEMS SET CHALLENGE_QUESTION='" + challengeQuestion + "',CHALLENGE_RESPONSE='" + challengeResponse + "' WHERE MEMBER_ID=" + memberId + " AND SYSTEM_ID=" + systemId;
                                dataOp.ExecuteQueryforOracle(strSql);

                            }
                        }

                    }

                }
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        protected static DataSet GetMemberData(string clientid, string baseid)
        {
            OracleDBOperations dataOp = new OracleDBOperations();
            StringBuilder sb = new StringBuilder();
            sb.Append("select CLIENT_ID, BASE_ID, LAST_NAME, FIRST_NAME, ACCESS_CONTROL_GROUP_ID, ADDR1, ADDR2, CITY, STATE_CD, POSTAL_CD, COUNTRY_CD,a.MEMBER_ID,ms.CHALLENGE_QUESTION,ms.CHALLENGE_RESPONSE ")
            .Append(" from iss3_members a,iss3_member_security_groups b,iss3_member_systems ms where a.member_id = b.member_id and b.system_id = 2 and b.service_id = 25 and ms.MEMBER_ID= A.MEMBER_ID AND ms.SYSTEM_ID=b.SYSTEM_ID AND a.client_id =")
            .Append(clientid)
            .Append(" and a.base_id = '")
            .Append(baseid.Trim())
            .Append("'");
            DataSet memberDS = new DataSet();
            string strSql = sb.ToString();
            memberDS = dataOp.GetDSResult(strSql);
            return memberDS;
        }
        #endregion

    }
}
