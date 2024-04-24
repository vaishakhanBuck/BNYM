using System;
using System.Text;
using System.IO;
namespace BNYM_Process2
{
    class Logs
    {
        private static StreamWriter writer = null;
        private static string LogFilePath = null;
        private static int counter = 0;
        private static string consoleText = null;

        public static void Init(string FilePath)
        {
            LogFilePath = FilePath;
        }
        public static void InitCounter(int Counter)
        {
            counter = Counter;
        }
        public static void WriteLine(string LogText)
        {
            // create a writer if one does not exist
            if (writer == null)
            {
                //writer = new StreamWriter(File.Open(LogFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite));
                writer = new StreamWriter(LogFilePath, true, Encoding.UTF8, 65536);
                writer.AutoFlush = false;
            }
            try
            {
                // do the actual work
                
                writer.WriteLine(LogText);
                counter = counter + 1;
                if (counter > 1995)
                {
                    if (LogText.Contains("Processing Row"))
                    {
                        consoleText = LogText.Replace("Processing Row", "");
                    }
                }
                if (counter == 2000)
                {
                    Console.WriteLine("Processed Rows till: " + consoleText);
                    writer.Flush();
                    writer.Close();
                    writer = null;
                    counter = 0;
                }
                
            }
            catch (Exception ex)
            {
                // very simplified exception logic... Might want to expand this
                if (writer != null)
                {
                    writer.Dispose();
                }
            }
        }

        // Make sure you call this before you end
        public static void Close()
        {
            if (writer != null)
            {
                writer.Flush();
                writer.Close();
                writer = null;
            }
        }
        public static void WriteLog(string logFile, string logMessage)
        {
            string logTime = DateTime.Now.ToShortDateString().ToString() + " " + DateTime.Now.ToLongTimeString().ToString() + " => ";
            using (StreamWriter sw = new StreamWriter(logFile,true))
            {
                sw.WriteLine(logTime + logMessage);
                sw.Flush();
               // sw.Close();
            }
        }
        public static void WriteLogCsv(string logFile, StringBuilder logMessage)
        {
            // string logTime = DateTime.Now.ToShortDateString().ToString() + " " + DateTime.Now.ToLongTimeString().ToString() + " => ";
            using (StreamWriter sw = new StreamWriter(logFile, true, Encoding.UTF8, 65536))
            {
                sw.WriteLine(logMessage);
               // sw.Flush();
               // sw.Close();
            }
            //StreamWriter sw = new StreamWriter(logFile, true, Encoding.UTF8, 65536);
            //sw.AutoFlush = false;
            //for (int i = 0;i<201; i++)
            //{
            //    if (i == 0)
            //    {
                    
            //    }
            //    sw.WriteLine(logMessage);
            //    if (i == 200)
            //    {
            //        sw.Flush();
            //        i = 0;
            //        sw.Close();
            //    }
            //}
            
        }
        public static void PrintFunction(StreamWriter writer)
        {
            //SOME CODE                 
            writer.Write("Some string...");
            //SOME CODE
        }
        public static void WriteLogWithoutTime(string logFile, string logMessage)
        {
            using (StreamWriter sw = new StreamWriter(logFile, true, Encoding.UTF8, 65536))
            {
                sw.WriteLine(logMessage);
                //sw.Flush();
                //sw.Close();
            }
        }

    }
}
