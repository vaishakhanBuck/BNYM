using System;
using System.Data;
using System.IO;
using System.Xml;
using Aspose.Cells;
using Microsoft.VisualBasic.FileIO;

namespace  BNYM_Process2
{
    class ProcessData
    {

        public DataTable ProcessExcel(string inputFile, string logFile)
        {
            System.Data.DataTable dt = new System.Data.DataTable();
            try
            {
                
                if (File.Exists(inputFile))
                {


                    using (FileStream fstream = new FileStream(inputFile, FileMode.Open))
                    {
                        Workbook workbook = new Workbook(fstream);
                        Worksheet worksheet = workbook.Worksheets[2];
                        
                        dt = worksheet.Cells.ExportDataTableAsString(0, 0, worksheet.Cells.MaxDisplayRange.RowCount, worksheet.Cells.MaxDisplayRange.ColumnCount, true);
                       
                    }
                }
                else
                {
                     Logs.WriteLog(logFile, "No file found");
                    
                }

            }
            catch (Exception ex)
            {
                 Logs.WriteLog(logFile, "Some error has occured." + ex.Message);
            }
            return dt;
        }

        public DataTable ProcessExcelBPR(string inputFile, string logFile)
        {
            System.Data.DataTable dt = new System.Data.DataTable();
            try
            {

                if (File.Exists(inputFile))
                {


                    using (FileStream fstream = new FileStream(inputFile, FileMode.Open))
                    {
                        Workbook workbook = new Workbook(fstream);
                        XmlReader xmlReader = new XmlReader();
                        DataTable dtBNYMFieldStructure = xmlReader.ReadXML("1771", 3, logFile);
                        DataRow drSheetId = dtBNYMFieldStructure.Rows[0];
                        int sheet_id = Convert.ToInt32(drSheetId["value"]);
                        Worksheet worksheet = workbook.Worksheets[sheet_id];

                        dt = worksheet.Cells.ExportDataTableAsString(0, 0, worksheet.Cells.MaxDisplayRange.RowCount, worksheet.Cells.MaxDisplayRange.ColumnCount, true);

                    }
                }
                else
                {
                    Logs.WriteLog(logFile, "No file found");

                }

            }
            catch (Exception ex)
            {
                Logs.WriteLog(logFile, "Some error has occured." + ex.Message);
            }
            return dt;
        }
        public DataTable ProcessExcelML(string inputFile, string logFile)
        {
            System.Data.DataTable dt = new System.Data.DataTable();
            try
            {

                if (File.Exists(inputFile))
                {


                    using (FileStream fstream = new FileStream(inputFile, FileMode.Open))
                    {
                        Workbook workbook = new Workbook(fstream);
                        //Get excel index starts
                        XmlReader xmlReader = new XmlReader();
                        DataTable dtBNYMFieldStructure = xmlReader.ReadXML("1771", 3, logFile);
                        DataRow drSheetId = dtBNYMFieldStructure.Rows[3];
                        int sheet_id = Convert.ToInt32(drSheetId["value"]);
                        Worksheet worksheet = workbook.Worksheets[sheet_id];

                        dt = worksheet.Cells.ExportDataTableAsString(0, 0, worksheet.Cells.MaxDisplayRange.RowCount, worksheet.Cells.MaxDisplayRange.ColumnCount, true);

                    }
                }
                else
                {
                    Logs.WriteLog(logFile, "No file found");

                }

            }
            catch (Exception ex)
            {
                Logs.WriteLog(logFile, "Some error has occured." + ex.Message);
            }
            return dt;
        }
        public DataTable ProcessCsv(string inputFile, string logFile)
        {
            int header = 0;
            int dataRowCount = 0;
            DataTable dt = new DataTable();
            try
            {
                CsvReader reader = new CsvReader(inputFile);
                DataColumn column;
                DataRow row;
                column = new DataColumn();
                column.DataType = System.Type.GetType("System.String");
                column.ColumnName = "BASE_ID";
                dt.Columns.Add(column);

                foreach (string[] dataArray in reader.RowEnumerator)
                {

                    if (header == 0)
                    {
                        header++;
                        continue;
                    }

                    row = dt.NewRow();
                    row["BASE_ID"] = dataArray[0].Trim();
                    dt.Rows.Add(row);
                    dataRowCount++;

                }

            }
            catch (Exception ex)
            {
                string message = "Record rejected for row : " + (dataRowCount + 1).ToString() + ", " + ex.Message;
                 Logs.WriteLog(logFile, message);
            }
            return dt;
        }
        public DataTable ProcessCsvSingleColumn(string inputFile, string logFile, char splitBy)
        {
            DataTable dt = new DataTable();
            try
            {
                //StreamReader reader = new StreamReader(File.OpenRead(inputFile));
                using (var reader = new StreamReader(inputFile))
                {

                    string line = null;
                    string[] totalCount = reader.ReadLine().Split(splitBy);
                    if (totalCount != null)
                    {
                        Logs.WriteLog(logFile, "Total Number Of Records :" + totalCount[1]);
                    }

                    string[] DocumentDate = reader.ReadLine().Split(splitBy);
                    if (DocumentDate != null)
                    {
                        Logs.WriteLog(logFile, "File Created Date :" + DocumentDate[1]);
                    }
                    string[] headers = reader.ReadLine().Split(splitBy);

                    foreach (string header in headers)
                    {
                        dt.Columns.Add(header);
                    }
                    int rowNum = 0;
                    DataRow dr;
                    while ((line = reader.ReadLine()) != null)
                    {
                        try
                        {
                            //string line = reader.ReadLine();
                            rowNum++;

                            if (!string.IsNullOrEmpty(line.Trim()))
                            {
                                string[] rows = line.Split(splitBy);
                                dr = dt.NewRow();
                                for (int i = 0; i < headers.Length; i++)
                                {
                                    if (i < rows.Length)
                                        dr[i] = rows[i];
                                    else
                                        dr[i] = "";
                                }
                                dt.Rows.Add(dr);
                            }
                            else
                            {

                            }
                        }
                        catch (Exception ex)
                        {
                             Logs.WriteLog(logFile, "Exception at Row :" + rowNum + "Exception :" + ex.Message);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                 Logs.WriteLog(logFile, ex.Message);
            }
            return dt;
        }
        public DataTable ProcessCsvNew(string inputFilePassed, string logFile)
        {

            DataTable csvData = new DataTable();

            try
            {

                using (TextFieldParser csvReader = new TextFieldParser(inputFilePassed))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }

                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        //Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                           //  Logs.WriteLog(logFile, "Exception occoured in processing file" +i+ inputFilePassed);
                         

                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
                 Logs.WriteLog(logFile, "Exception occoured in processing file" + inputFilePassed);
                 Logs.WriteLog(logFile, ex.Message);

            }
            return csvData;
        }
    }
}
