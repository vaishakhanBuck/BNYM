using System;
using System.IO;
using System.Data;
using System.Configuration;
using System.Collections.Generic;
using System.Text;
namespace BNYM_Process2
{

    class Program
    {

        static string jobNumber = string.Empty;
        static string clientId = "1771";
        
        static string creation_date = string.Empty;
        static string inputFile = string.Empty;
        static string processMode = string.Empty;
        static string logFile = string.Empty;
        static string logfileCsv = string.Empty;
        public static string schedEnvironment = string.Empty;
        Logs WriteLog = new Logs();
        public struct EnvironmentVal
        {
            public string JobNumber;
            public string InputFile;
            
            public string LogFile;
            public string logCsv;
           // public string ProcessMode;
            public string Environment;

        }

        static void Main(string[] args)
        {

            EnvironmentVal ei = new EnvironmentVal();

            try
            {

                string commandLine = UtilityClass.ConvertStringArrayToString(args);
                bool val = ProcessCommandLine(commandLine, ref ei);

                if (val)
                {
                    jobNumber = ei.JobNumber;
                    jobNumber = DateTime.Now.ToString("yyyyMMdd") + jobNumber;
                    schedEnvironment = ei.Environment;
                    logFile = ei.LogFile;
                    logfileCsv = ei.logCsv;
                    
                    processMode = "A";
                   
                        Logs.WriteLog(logFile, "BNYM Severenace Second exe  process started- Wave upload from Processing SRA selected list");
                        inputFile = ei.InputFile;
                        
                        Logs.WriteLog(logFile, "Job No:" + jobNumber + "   Input File (Processing SRA selected list) :" + inputFile);





                    UploadWave();

                        Logs.WriteLog(logFile, "Client Id " + "1771");
                        
                        //Logs.WriteLog(logFile, "Input File " + inputFile);
                        Logs.WriteLog(logFile, "Log File Path " + logFile);
                        Logs.WriteLog(logFile, "Bad Records " + logfileCsv);
                        Logs.WriteLog(logFile, " Environment -  " + schedEnvironment);
                        Logs.WriteLog(logFile, "Process has been completed.");
                        

                    
                    
                }
            }
            catch (Exception ex)
            {
                 Logs.WriteLog(logFile, ex.Message);
            }


        }
        public static bool InsertLogFile(string client_id, string userid, string doc_id, string member_id, string targetDirReg, FileInfo doc, string targetFile, string jobNumber, string logFile,string parent_docdesc)
        {
            try
            {
                string COMMA = ",";
                OracleDBOperations dataOp = new OracleDBOperations();
                string sqlStr = "select count(*) from iss3_custdoc where client_id = " + client_id + " and doc_id = " + doc_id;
                DataSet ds = new DataSet();
                ds = dataOp.GetDSResult(sqlStr);
                int rowCount = Convert.ToInt32(ds.Tables[0].Rows[0][0].ToString());
                string memberId = string.Empty;
                if (rowCount > 0)
                {

                    sqlStr = "select base_id from iss3_members where client_id = " + client_id + " and  member_id = '" + member_id + "'";
                    ds = new DataSet();
                    ds = dataOp.GetDSResult(sqlStr);
                    string base_id = ds.Tables[0].Rows[0]["BASE_ID"].ToString();
                    memberId = member_id;
                    if (!Directory.Exists(targetDirReg))
                    {
                        Directory.CreateDirectory(targetDirReg);
                    }
                    string targetFilePath = System.IO.Path.Combine(targetDirReg, targetFile);
                    if (File.Exists(targetFilePath))
                    {
                        Random rnd = new Random();
                        string tmpFilename = "t_" + rnd.Next().ToString() + "_" + targetFile; //t_434343_last_frst_8888.PDF
                        targetFilePath = System.IO.Path.Combine(targetDirReg, tmpFilename);
                        targetFile = tmpFilename;
                    }
                    File.Copy(logFile, targetFilePath, true);
                    string fileDate = doc.CreationTime.ToShortDateString();
                    long fileSize = doc.Length;
                    sqlStr = "SELECT iss3_doc_number_seq.NEXTVAL FROM dual";
                    ds = new DataSet();
                    ds = dataOp.GetDSResult(sqlStr);
                    string docNumber_current = ds.Tables[0].Rows[0][0].ToString();
                    sqlStr = "select doc_desc from iss3_custdoc where client_id = " + client_id + " and doc_id =" + doc_id;
                    ds = new DataSet();
                    ds = dataOp.GetDSResult(sqlStr);
                    string docDesc1 = ds.Tables[0].Rows[0][0].ToString();
                    StringBuilder sb = new StringBuilder();
                    sb.Append("INSERT INTO ISS3_MEMDOC ( MEMBER_ID, CLIENT_ID, SERVICE_ID, DOC_ID, DOC_NUMBER, DOC_PATH, DOC_NAME, CREATION_DATE, CYCLE_DATE, USER_ID, DOC_DESC, LANGUAGE_ID, FOLDER_ID, FILESIZE, ACCESS_CONTROL_GROUP_ID, STATUS ) VALUES ")
                        .Append("(")
                        .Append(memberId)
                        .Append(COMMA)
                        .Append(client_id)
                        .Append(COMMA)
                        .Append(13)
                        .Append(COMMA)
                        .Append(doc_id)
                        .Append(COMMA)
                        .Append(docNumber_current)
                        .Append(COMMA)
                        .Append("'" + targetDirReg + "\\',")
                        .Append("'" + targetFile + "'")
                        .Append(COMMA)
                        .Append("sysdate")
                        .Append(COMMA)
                        .Append("TO_DATE(replace(replace('" + fileDate + "', ',', ''), ' ', '') , 'MM/DD/YY'),")
                        .Append("'")
                        .Append(base_id) //modified on 23-03-2011 by ratheesh // userId
                        .Append("'")
                        .Append(COMMA)
                        .Append("'" + inputFile + "- " + parent_docdesc + " log file" + " '")
                        .Append(COMMA)
                        .Append("1")
                        .Append(COMMA)
                        .Append("0")
                        .Append(COMMA)
                        .Append("replace(replace(" + fileSize + ",',', ''), ' ', '')")
                        .Append(COMMA)
                        .Append("1")
                        .Append(COMMA)
                        .Append("NULL)");
                    sqlStr = sb.ToString();
                    //WriteLog("sqlmem.txt", sqlStr);
                    bool result = dataOp.ExecuteQueryforOracle(sqlStr);
                    Logs.WriteLog(logFile, "Registered in MEMDOC - " + targetFile);
                    //Call stored proc iss4_docreg.do_doc_tasknet - 15-03-2011 
                    sqlStr = "call iss4_docreg.do_doc_tasknet(" + client_id + ", " + docNumber_current + ", '" + userid + "')";
                     result = dataOp.ExecuteQueryforOracle(sqlStr);
                    return true;

                }
                else
                {
                    Logs.WriteLog(logFile, "Job# " + jobNumber + ". Invalid document id." + doc.FullName);
                    return false;
                }
            }
            catch (Exception ex)
            {
                Logs.WriteLog(logFile, ex.Message + " - " + doc.FullName + "Job# " + jobNumber + ". ");
                return false;
            }
        }
        private static void SetAsposeCellsLicense()
        {
            // Set license
            string licenseFile = @"Aspose.Total.lic";
            if (File.Exists(licenseFile))
            {
                Aspose.Cells.License lic2 = new Aspose.Cells.License();
                lic2.SetLicense(licenseFile);
            }
        }

        private static void UploadWave()
        {
            int totalRows = 0;
              int minCount = 0;
                int maxCount = 0;
            DataTable dtClientData = new DataTable();
            

            Operations op = new Operations();
            string splitByString = string.Empty;
            splitByString = ConfigurationManager.AppSettings["splitBy"].ToString().Trim();
           
            //op.InsertISS4ImportSummary(clientId,processMode, "12", jobNumber);
            Member Member = new Member();
            Profile profile = new Profile();
            //added by Jiji
            // Logs.Init(logFile);
           
                dtClientData = ProcessFileSRA(inputFile, char.Parse(splitByString));
                dtClientData = StripEmptyRows(dtClientData);


            XmlReader xmlReader = new XmlReader();
            DataTable dtbFieldStrucure = xmlReader.ReadXML(clientId, 1, logFile);
            string valueEmpid = Convert.ToString(dtbFieldStrucure.Rows[0]["value"]);
            RemoveRowsWithEmptyColumn(dtClientData, valueEmpid);
            creation_date = DateTime.Now.ToString("yyyy-MM-dd");
            if (dtClientData.Rows.Count > 0 )
            {

                minCount = 0;
                
                totalRows = dtClientData.Rows.Count;
                maxCount = totalRows;
                for (int i = minCount; i < totalRows; i++)
                {
                    
                    
                  
                  
                     Member.insert_SEV_EMP_DETAILS(dtClientData, dtClientData.Rows[i], clientId, logFile, logfileCsv, inputFile, jobNumber, creation_date);

                   //op.RegisterDocuments(dtClientData, dtClientData.Rows[i], clientId, logFile, logfileCsv, "", jobNumber, creation_date);
                   
                    Console.WriteLine("Processing Rows from selected list from Processing SRA " + i);
                }
                

            }
            else 
            {
                 Logs.WriteLog(logFile, "No input found in file :" + inputFile);
            }



           

            Member.ShowEmpDetailsTableStatus(logFile);
            

        }
        public static void RemoveRowsWithEmptyColumn(DataTable dataTable, string columnName)
        {
            // Create a list to store rows that need to be removed
            var rowsToRemove = new List<DataRow>();

            // Loop through each row in the DataTable
            foreach (DataRow row in dataTable.Rows)
            {
                // Check if the column is empty
                if (string.IsNullOrEmpty(row[columnName].ToString()))
                {
                    // If the column is empty, add the row to the list of rows to be removed
                    rowsToRemove.Add(row);
                }
            }

            // Remove rows from the DataTable
            foreach (DataRow row in rowsToRemove)
            {
                dataTable.Rows.Remove(row);
            }

            // Reset the row state to unchanged
            dataTable.AcceptChanges();
        }
        private static DataTable StripEmptyRows(DataTable dt)
        {
            List<int> rowIndexesToBeDeleted = new List<int>();
            int indexCount = 0;
            foreach (var row in dt.Rows)
            {
                var r = (DataRow)row;
                int emptyCount = 0;
                int itemArrayCount = r.ItemArray.Length;
                foreach (var i in r.ItemArray) if (IsNullOrWhiteSpace(i.ToString())) emptyCount++;

                if (emptyCount == itemArrayCount) rowIndexesToBeDeleted.Add(indexCount);

                indexCount++;
            }

            int count = 0;
            foreach (var i in rowIndexesToBeDeleted)
            {
                dt.Rows.RemoveAt(i - count);
                count++;
            }

            return dt;
        }
        public static bool IsNullOrWhiteSpace(string value)
        {
            if (value != null)
            {
                for (int i = 0; i < value.Length; i++)
                {
                    if (!char.IsWhiteSpace(value[i]))
                    {
                        return false;
                    }
                }
            }
            return true;
        }
        private static DataTable ProcessFile(string inputFile,char splitBy=',')
        {
            DataTable dt = new DataTable();
            dt.Rows.Clear();
            if (File.Exists(inputFile))
            {
                ProcessData FileProcess = new ProcessData();
                FileInfo finfo = new FileInfo(inputFile);
                switch (finfo.Extension.ToLower())
                {
                    case ".xls":
                        dt = FileProcess.ProcessExcel(inputFile, logFile);
                        break;
                    case ".xlsx":
                        dt = FileProcess.ProcessExcel(inputFile, logFile);
                        break;
                    case ".csv":
                        dt = FileProcess.ProcessCsvSingleColumn(inputFile, logFile, splitBy);
                        break;
                    default:
                         Logs.WriteLog(logFile, "Invalid data filetype. Only XLS/XLSX/CSV files are supported.");
                        break;
                }
            }
            else
            {
                 Logs.WriteLog(logFile, "Doc Number missing." + inputFile);
            }
            return dt;
        }


        private static DataTable ProcessFileML(string inputFile, char splitBy = ',')
        {
            DataTable dt = new DataTable();
            dt.Rows.Clear();
            if (File.Exists(inputFile))
            {
                ProcessData FileProcess = new ProcessData();
                FileInfo finfo = new FileInfo(inputFile);
                switch (finfo.Extension.ToLower())
                {
                    case ".xls":
                        dt = FileProcess.ProcessExcelML(inputFile, logFile);
                        break;
                    case ".xlsx":
                        dt = FileProcess.ProcessExcelML(inputFile, logFile);
                        break;
                    case ".csv":
                        dt = FileProcess.ProcessCsvSingleColumn(inputFile, logFile, splitBy);
                        break;
                    default:
                        Logs.WriteLog(logFile, "Invalid data filetype. Only XLS/XLSX/CSV files are supported.");
                        break;
                }
            }
            else
            {
                Logs.WriteLog(logFile, "File missing." + inputFile);
            }
            return dt;
        }

        private static DataTable ProcessFileSRA(string inputFile, char splitBy = ',')
        {
            DataTable dt = new DataTable();
            dt.Rows.Clear();
            if (File.Exists(inputFile))
            {
                ProcessData FileProcess = new ProcessData();
                FileInfo finfo = new FileInfo(inputFile);
                switch (finfo.Extension.ToLower())
                {
                    case ".xls":
                        dt = FileProcess.ProcessExcelBPR(inputFile, logFile);
                        break;
                    case ".xlsx":
                        dt = FileProcess.ProcessExcelBPR(inputFile, logFile);
                        break;
                    case ".csv":
                        dt = FileProcess.ProcessCsvSingleColumn(inputFile, logFile, splitBy);
                        break;
                    default:
                        Logs.WriteLog(logFile, "Invalid data filetype. Only XLS/XLSX/CSV files are supported.");
                        break;
                }
            }
            else
            {
                Logs.WriteLog(logFile, "D missing." + inputFile);
            }
            return dt;
        }



        private static bool ProcessCommandLine(string commandLine, ref EnvironmentVal ev)
        {
            string[] para = commandLine.Split(';');

            if (para.Length != 5)
                return false;

            for (int i = 0; i < para.Length; i++)
            {
                if (para[i].Length == 0)
                    return false;
                else
                {
                    string[] temp = para[i].Trim().Split('=');
                    
                    switch (temp[0].ToLower())
                    {
                        case "jn":
                            ev.JobNumber = temp[1];
                            break;
                        
                        case "if":
                            ev.InputFile = temp[1];
                            break;  
                        case "env":
                            ev.Environment = temp[1];
                            break;
                        case "lf":
                            ev.LogFile = temp[1];
                            break;
                        case "lf2":
                            ev.logCsv = temp[1];
                            break;
                        default:
                            break;
                    }
                }
            }
            return true;
        }


    }
}
