using System;
using System.Data;
using System.Globalization;
namespace  BNYM_Process2
{
    public class UtilityClass
    {
        public static string ConvertStringArrayToString(string[] array)
        {
            return string.Join("", array).Trim();
        }

        public static string MaskValue(string value)
        {
            var builder = new System.Text.StringBuilder();

            for (int i = 0; i < value.Length; i++)
            {
                builder.Append("*");
            }
            return builder.ToString();
        }

        public static string PrepareConnectionString(string conStringParam)
        {
            string[] conStringParts = conStringParam.Split("/@".ToCharArray());
            string conString = string.Empty;
            if (conStringParts.Length == 3)
                conString = "User ID=" + conStringParts[0] + ";Password=" + conStringParts[1] + ";Data Source=" + conStringParts[2];
            return conString;
        }
        private static string PrepareOracleConnectionString(string conStringParam)
        {
            string[] conStringParts = conStringParam.Split("/@".ToCharArray());
            string conString = string.Empty;
            if (conStringParts.Length == 3)
                conString = "User ID=" + conStringParts[0] + ";Password=" + conStringParts[1] + ";Data Source=" + conStringParts[2];
            return conString;
        }
        private static string PrepareOracleConnectionString(string server, string username, string password)
        {
            string conString = string.Empty;
            conString = "User ID=" + username + ";Password=" + password + ";Data Source=" + server;
            return conString;
        }
      public  bool AreAllColumnsEmpty(DataRow dr)
        {
            if (dr == null)
            {
                return true;
            }
            else
            {
                foreach (var value in dr.ItemArray)
                {
                    if (value != null)
                    {
                        return false;
                    }
                }
                return true;
            }
        }
      public string GetDateFormat(string clientId, string logFile,string type)
      {
          try
          {
              DataTable dtSettings = new DataTable();
              XmlReader xmlReader = new XmlReader();
              string dateFormat = string.Empty;
              string default_Value = string.Empty;
              dtSettings = xmlReader.ReadXML(clientId, 3, logFile);
              string ColumnName = string.Empty;
              if (dtSettings.Rows.Count > 0)
              {
                  
                  if (type.ToUpper().Equals("IN"))
                  {
                      ColumnName = "DATEFORMAT";
                  }
                  else if (type.ToUpper().Equals("OUT"))
                  {
                      ColumnName = "OUTDATEFORMAT";
                  }
                  for (int i = 0; i < dtSettings.Rows.Count; i++)
                  {
                      if (dtSettings.Rows[i]["key"].ToString().ToUpper().Equals(ColumnName))
                      {
                          dateFormat = dtSettings.Rows[i]["value"].ToString();
                          break;
                      }
                  }
              }

              return dateFormat;
          }
          catch (Exception ex)
          {
              return string.Empty;
          }
      }
        public static bool IsValidDate(string inputDate, string format)
        {
            DateTime date;
            return DateTime.TryParseExact(inputDate, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out date);
        }
        public string FormatDate(string client_id, string logFile, string dateText)
        {
            try
            {

                CultureInfo provider = CultureInfo.InvariantCulture;
                string dateString = dateText;
                string formatIN = GetDateFormat(client_id, logFile, "IN");
                if (!string.IsNullOrEmpty(formatIN) && (IsValidDate(dateString, formatIN)))
                {
                    
                   
                      string  formatOUT = GetDateFormat(client_id, logFile, "OUT");
                    DateTime date = DateTime.ParseExact(dateString, formatIN, null);
                    string outputDate = date.ToString(formatOUT);
                    return outputDate;

                     
                }
                else
                {
                    return "INVALID";
                }
            }
            catch (Exception ex)
            {

                return dateText;
            }
        }
        public string FormatDate1(string client_id, string logFile, string dateText)
      {
          try
          {

              CultureInfo provider = CultureInfo.InvariantCulture;
              string dateString = dateText;
              string format = GetDateFormat(client_id, logFile,"IN");
              if (!string.IsNullOrEmpty(format))
              {
                  DateTime result;
                  DateTime.TryParseExact(dateText, format, provider, DateTimeStyles.None, out result);

                  if (result == DateTime.MinValue)
                  {
                      return dateText;
                  }
                  else
                  {
                      format=GetDateFormat(client_id, logFile,"OUT");
                      return result.Date.ToString(format);

                  }

                    //DateTime date = DateTime.ParseExact(inputDate, "MM/dd/yyyy", null);
                   // string outputDate = date.ToString("yyyy-MM-dd");
                }
              else
              {
                  return dateText;
              }
          }
          catch (Exception ex)
          {

              return dateText;
          }
      }
    }


}
