using System;
using System.Data;
using System.IO;
namespace  BNYM_Process2
{
    class XmlReader
    {
        public DataTable ReadXML(string clientId,int Type,string logFile)
        {
            DataTable table = new DataTable();
            
            try
            {
                string yourPath = GeneratePath(clientId,logFile);
                DataSet lstNode = new DataSet();
                lstNode.ReadXml(yourPath);
                if (Type == 1)
                {
                    table = lstNode.Tables["Bfield"];
                }
                else if (Type == 2)
                {
                    table = lstNode.Tables["MLfield"];
                }
                else if (Type == 3)
                {
                    table = lstNode.Tables["settings"];
                }
                else 
                {
                 
                }
                
                return table;
            }
            catch (Exception ex)
            {
                return table;
            }
        }
        private string GeneratePath(string clientId,string logFile)
        {
            string path = string.Empty;
            var debugDir = AppDomain.CurrentDomain.BaseDirectory;
            path =Path.Combine(debugDir.ToString(),@"XmlFiles\" + clientId + "_Mapping.xml");
          //  path = @"P:\dev\Amrutha\Work\applications\USSTEEL_MemberLoad\MultiClientMemberLoad\XmlFiles\" + clientId + "_Mapping.xml";
            if (!File.Exists(path))
            {
                 Logs.WriteLog(logFile, "XML Not Found");
            }
          
            return path;
        }
    }
}
