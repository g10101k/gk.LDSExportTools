using System;
using System.IO;
using System.Configuration;
using System.Data;
using System.Collections.Generic;
using System.Collections.Specialized;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using NDesk.Options;

namespace gk.LDSExportTools
{
    class Program
    {
        static string ServerName = "";
        static string IntegratedSecurity = "";
        static string User = "";
        static string Password = "";
        static string DataBaseName = "";
        static string ImporterName = "";
        static string Action = "importer";
        static bool full = false;
        static bool show_help = false;
        public static void Main(string[] args)
        {

            try
            {
                OptionSet p = new OptionSet() {
                { "f|full", "Use default scheme of scripting.", v => full = v != null },
                { "s|srv=", "Server name.", value => ServerName = value },
                { "a|aut=", "IntegratedSecurity (true|false).", value => IntegratedSecurity = value },
                { "u|usr=", "SQL user name.",  value => User = value },
                { "p|pwd=", "SQL passwor.", value => Password = value },
                { "d|db=", "Database name.", value => DataBaseName = value },
                { "i|imp=", "Importer mask.", value => ImporterName = value },
                { "t|type=", "Type action.", value => Action = value },

                { "h|help", "Show this message and exit.", v => show_help = v != null },
            };
                p.Parse(args);
                if (show_help) { ShowHelp(p); return; }
                GenerateTableScript(args);
            }
            catch (OptionException e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                Console.WriteLine("Try 'i-lds-imp-exp --help' for more information.");
                return;
            }
        }

        static void ShowHelp(OptionSet p)
        {
            Console.WriteLine("Usage: i-lds-imp-exp [OPTIONS]");
            Console.WriteLine("Export importer from db I-LDS");
            Console.WriteLine();
            Console.WriteLine("Options:");
            p.WriteOptionDescriptions(Console.Out);
        }

        private static void GenerateTableScript(string[] args)
        {
            if (string.IsNullOrEmpty(ServerName))
            {
                string DefaultServerName = ConfigurationManager.AppSettings["DefaultServerName"].ToString();
                Console.Write("ServerName [" + DefaultServerName + "]: ");
                ServerName = Console.ReadLine();
                ServerName = (string.IsNullOrEmpty(ServerName)) ? DefaultServerName : ServerName;
            }
            else
            {
                Console.WriteLine("ServerName: {0}", ServerName);
            }

            Server myServer = new Server(ServerName);
            try
            {
                if (string.IsNullOrEmpty(IntegratedSecurity))
                {
                    string DefaultIntegratedSecurity = ConfigurationManager.AppSettings["DefaultIntegratedSecurity"].ToString();
                    Console.Write("IntegratedSecurity [" + DefaultIntegratedSecurity + "]: ");
                    string SqlSecurity = Console.ReadLine();
                    bool SqlSecurityBool = (Boolean.TryParse(SqlSecurity, out SqlSecurityBool)) ? SqlSecurityBool : Convert.ToBoolean(DefaultIntegratedSecurity);
                    myServer.ConnectionContext.LoginSecure = SqlSecurityBool;
                }
                else
                {
                    Console.WriteLine("Integrated Security: {0}", IntegratedSecurity);
                    bool SqlSecurityBool = (Boolean.TryParse(IntegratedSecurity, out SqlSecurityBool)) ? SqlSecurityBool : Convert.ToBoolean(IntegratedSecurity);
                    myServer.ConnectionContext.LoginSecure = SqlSecurityBool;
                }

                if (!myServer.ConnectionContext.LoginSecure)
                {
                    if (string.IsNullOrEmpty(User))
                    {
                        string DefaultUser = ConfigurationManager.AppSettings["DefaultUser"].ToString();
                        Console.Write("User [" + DefaultUser + "]: ");
                        User = Console.ReadLine();
                        User = (string.IsNullOrEmpty(User)) ? DefaultUser : User;
                    }
                    else
                    {
                        Console.WriteLine("User: {0}", User);
                    }
                    myServer.ConnectionContext.Login = User;

                    if (string.IsNullOrEmpty(Password))
                    {
                        string DefaultPassword = ConfigurationManager.AppSettings["DefaultPassword"].ToString(); ;
                        Console.Write("Password [{0}]: ", DefaultPassword);
                        Password = Console.ReadLine();
                        Password = (string.IsNullOrEmpty(Password)) ? DefaultPassword : Password;
                    }
                    else
                    {
                        Console.WriteLine("Password: {0}", Password);
                    }
                    myServer.ConnectionContext.Password = Password;
                }
                myServer.ConnectionContext.Connect();

                //Создаем экземпляр класса базы данных
                string DefaultDataBaseName = ConfigurationManager.AppSettings["DefaultDataBaseName"].ToString();
                if (string.IsNullOrEmpty(DataBaseName))
                {
                    int i = 0;
                    Dictionary<int, string> d = new Dictionary<int, string>();
                    foreach (var db in myServer.Databases)
                    {
                        d.Add(i, db.ToString());
                        Console.WriteLine(string.Format("{0}: {1}", i, db));
                        i++;
                    }
                    Console.Write("DefaultDataBaseName [{0}]: ", DefaultDataBaseName);
                    DataBaseName = Console.ReadLine();

                    if (int.TryParse(DataBaseName, out i))
                    {
                        DataBaseName = d[i];
                    }
                    else
                    {
                        DataBaseName = (string.IsNullOrEmpty(DataBaseName)) ? DefaultDataBaseName : DataBaseName;
                    }
                    DataBaseName = DataBaseName.Replace("[", "").Replace("]", "");
                }
                else
                {
                    Console.WriteLine("DataBaseName: {0}", DataBaseName);
                }

                Database myAdventureWorks = myServer.Databases[DataBaseName];

                if (myAdventureWorks == null)
                {
                    Console.Write("DB not Exist [" + DataBaseName + "]: ");
                }
                else
                {
                    string path = AppDomain.CurrentDomain.BaseDirectory + "\\script\\";
                    if (!Directory.Exists(path))
                    {
                        try { Directory.CreateDirectory(path); }
                        catch { throw; }
                    }


                    if (Action == "importer")
                    {
                        if (full)
                        {
                            string sql = "";
                            //Создаем экземпляр класса, который будет генерировать скрипты
                            Scripter scripter = new Scripter(myServer);
                            scripter.Options.ScriptDrops = false;
                            scripter.Options.ScriptSchema = false;
                            scripter.Options.ScriptData = true;
                            foreach (Table table in myAdventureWorks.Tables)
                            {
                                if (table.Name.Contains("Importer"))
                                {
                                    Console.WriteLine(table.Name);
                                    foreach (string current2 in scripter.EnumScript(new Urn[] { table.Urn }))
                                    {
                                        sql += current2 + "\r\nGO\r\n";
                                    }
                                    File.WriteAllText(path + "\\" + DataBaseName + "Importers.sql", sql);
                                }
                            }
                        }
                        else
                            ImporterSql(myAdventureWorks, path);
                    }

                    if (Action == "report")
                    {
                        ReportSql(myAdventureWorks, path);
                    }

                    if (Action == "meta")
                    {
                        MetaObjectSql(myAdventureWorks, path);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
            //Закрыть соединение
            myServer.ConnectionContext.Disconnect();
        }

        private static void MetaObjectSql(Database myAdventureWorks, string path)
        {

            string text = "";
            Console.Write("MetaObject name like: ");
            string ObjName = Console.ReadLine();
            //(Отчет) Журнал некондиционных продуктов
            string tblName = "MetaObject";
            string sql = string.Format(@"SELECT * FROM [dbo].[MetaObject] WHERE [Name] like '%{0}%'", ObjName);
            DataSet ds = myAdventureWorks.ExecuteWithResults(sql);
            ds.Tables[0].TableName = tblName;
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                //text = "";
                string id = row["ObjectUid"].ToString();
                string name = row["Name"].ToString();
                Console.WriteLine("Exporting MetaObject: {0}", name);
                text += string.Format(@"DISABLE TRIGGER [TR_MetaObject_Rollback] ON [MetaObject];
DISABLE TRIGGER [TR_MetaObjectCommand_Rollback] ON [MetaObjectCommand];

GO

DECLARE @ObjectUid uniqueidentifier;
SET @ObjectUid = '{0}';
IF EXISTS (SELECT * FROM [MetaObjectCommand] WHERE [ObjectUid] = @ObjectUid)
DELETE FROM [MetaObjectCommand] where ObjectUid=@ObjectUid
IF EXISTS (SELECT * FROM [MetaObject] WHERE [ObjectUid] = @ObjectUid)
DELETE FROM [MetaObject] where ObjectUid=@ObjectUid
IF NOT EXISTS (SELECT * FROM [MetaObject] WHERE [ObjectUid] = @ObjectUid)
BEGIN

", id);


                text += GenerateSql(row, "") + "\r\n\r\n";
                text += NotRefTableSql(myAdventureWorks, "MetaObjectCommand", "ObjectUid", id);
                name = deleteProhibitedСharacter(name);

                text += @"

END;
GO
ENABLE TRIGGER [TR_MetaObject_Rollback] ON [MetaObject];
ENABLE TRIGGER [TR_MetaObjectCommand_Rollback] ON [MetaObjectCommand];
GO
";


            }
            string fPath = string.Format("{0}{1}({2}).sql", path, "Export", "");
            try { File.Delete(fPath); } catch { }
            File.AppendAllText(fPath, text);

        }

        private static void ReportSql(Database myAdventureWorks, string path)
        {

            string text = "";
            Console.Write("Report name like: ");
            string ReportName = Console.ReadLine();
            //(Отчет) Журнал некондиционных продуктов
            string tblName = "FeatureNode";
            string sql = string.Format(@"SELECT [FeatureNodeId]
      ,[FeatureTypeId]
      ,[Name]
      ,[Flags]
      ,[NodeOrder]
  FROM [dbo].[FeatureNode]  
  WHERE [Name] like '%{0}%'", ReportName);
            DataSet ds = myAdventureWorks.ExecuteWithResults(sql);
            ds.Tables[0].TableName = tblName;
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                //text = "";
                string id = row["FeatureNodeId"].ToString();
                string name = row["Name"].ToString();
                Console.WriteLine("Exporting report: {0}", name);

                text += GenerateSql(row, "") + "\r\n\r\n";
                text += NotRefTableSql(myAdventureWorks, "FeatureNodeRights", "FeatureNodeId", id);
                text += NotRefTableSql(myAdventureWorks, "FeatureNodeContent", "FeatureNodeId", id);
                text += NotRefTableSql(myAdventureWorks, "FeatureNodeGraphics", "FeatureNodeId", id);
                name = deleteProhibitedСharacter(name);

            }

            string fPath = string.Format("{0}{1}.sql", path, "FeatureNodeExport");
            try { File.Delete(fPath); } catch { }
            File.AppendAllText(fPath, text);

        }

        public static string ImporterSql(Database myAdventureWorks, string path)
        {
            try
            {
                string text = "";
                string DefaultImporterName = ConfigurationManager.AppSettings["DefaultImporterName"].ToString();
                if (string.IsNullOrEmpty(ImporterName))
                {
                    Console.Write("Importer name like  [{0}]: ", DefaultImporterName);
                    ImporterName = Console.ReadLine();
                    ImporterName = (string.IsNullOrEmpty(ImporterName)) ? DefaultImporterName : ImporterName;
                }
                else
                {
                    Console.WriteLine("Importer name like: {0}", ImporterName);
                }

                string tblName = "Importer";
                string sql = string.Format("select * from [{0}] where Name like '%{1}%'", tblName, ImporterName);
                DataSet ds = myAdventureWorks.ExecuteWithResults(sql);
                ds.Tables[0].TableName = tblName;
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    text = "";
                    string id = row["ImporterUid"].ToString();
                    string name = row["Name"].ToString();
                    Console.WriteLine("Exporting importer: {0}", name);

                    text += GenerateSql(row, "") + "\r\n\r\n";
                    text += ImporterInstancerSql(myAdventureWorks, "ImporterInstance", "ImporterUid", id);
                    text += ImporterTestSql(myAdventureWorks, "ImporterTest", "ImporterUid", id);
                    name = deleteProhibitedСharacter(name);
                    string fPath = string.Format("{0}{1}({2}).sql", path, name, id);
                    try { File.Delete(fPath); } catch { }
                    File.AppendAllText(fPath, text);
                }
                return text;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                return "";
            }
        }

        static string deleteProhibitedСharacter(string s)
        {
            return s.Replace(":", "_").Replace("?", "_").Replace("[", "_").Replace("]", "_");
        }

        public static string ImporterInstancerSql(Database myAdventureWorks, string tblName, string whereclm, string findId)
        {
            string sql = string.Format("select * from [dbo].[{0}] where [{1}] = '{2}'", tblName, whereclm, findId);
            DataSet ds = myAdventureWorks.ExecuteWithResults(sql);
            ds.Tables[0].TableName = tblName;
            string text = "";
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                string id = row["ImporterInstanceUid"].ToString();
                text += GenerateSql(row, "") + "\r\n\r\n";
                text += NotRefTableSql(myAdventureWorks, "ImporterInstanceSubdivision", "ImporterInstanceUid", id);
                text += NotRefTableSql(myAdventureWorks, "ImporterInstanceCP", "ImporterInstanceUid", id);
                text += NotRefTableSql(myAdventureWorks, "ImporterInstanceProduct", "ImporterInstanceUid", id);
                text += NotRefTableSql(myAdventureWorks, "ImporterInstanceEqpType", "ImporterInstanceUid", id);
                text += NotRefTableSql(myAdventureWorks, "ImporterInstanceEqp", "ImporterInstanceUid", id);
                text += NotRefTableSql(myAdventureWorks, "ImporterMapping", "ImporterInstanceUid", id);
            }
            return text;
        }

        public static string ImporterTestSql(Database myAdventureWorks, string tblName, string whereclm, string findId)
        {
            string sql = string.Format("select * from [dbo].[{0}] where [{1}] = '{2}'", tblName, whereclm, findId);
            DataSet ds = myAdventureWorks.ExecuteWithResults(sql);
            ds.Tables[0].TableName = tblName;
            string text = "";
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                string id = row["ImporterTestUid"].ToString();
                text += GenerateSql(row, "") + "\r\n\r\n";
                text += NotRefTableSql(myAdventureWorks, "ImporterTestTechTest", "ImporterTestUid", id);
            }
            return text;
        }

        public static string NotRefTableSql(Database myAdventureWorks, string tblName, string whereclm, string id)
        {
            try
            {
                string sql = string.Format("select * from [dbo].[{0}] where [{1}] = '{2}'", tblName, whereclm, id);
                DataSet ds = myAdventureWorks.ExecuteWithResults(sql);
                ds.Tables[0].TableName = tblName;
                string text = "";
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    text += GenerateSql(row, "") + "\r\n\r\n";
                }
                return text;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                return "";
            }

        }

        public static string GenerateSql(DataRow row, string where)
        {
            string tblName = row.Table.TableName;

            string sql = string.Format("INSERT [dbo].[{0}] (", tblName);

            foreach (DataColumn col in row.Table.Columns)
            {
                sql += string.Format("[{0}], ", col.Caption);
            }

            sql = sql.TrimEnd(new char[] { ' ', ',' });
            sql += ") VALUES (";

            foreach (DataColumn col in row.Table.Columns)
            {
                object o = row[col];
                string s = o.ToString();
                string _null = "null, ";
                if (col.DataType == typeof(Guid))
                {
                    sql += (s != "") ? string.Format("N'{0}', ", o) : _null;
                }
                else if (col.DataType == typeof(String) || col.DataType == typeof(string))
                {
                    sql += (s != "") ? string.Format("N'{0}', ", s.Replace("'", "''")) : _null;
                }
                else if (col.DataType == typeof(Boolean))
                {
                    s = (s != "") ? (o.ToString() == "False") ? "0" : "1" : _null;
                    sql += string.Format("{0}, ", s);
                }
                else if (col.DataType == typeof(Int32))
                {
                    sql += (s != "") ? string.Format("{0}, ", s) : _null;
                }
                else if (col.DataType == typeof(Double))
                {
                    sql += (s != "") ? string.Format("{0}, ", row[col]) : _null;
                }
                else if (col.DataType == typeof(byte[]))
                {
                    if (col.Caption.ToLower() == "rv")
                    {
                        sql += "default, ";
                    }
                    else
                    {
                        sql += (s != "") ? "0x" + BitConverter.ToString((byte[])row[col]).Replace("-", "") + ", " : _null;
                    }
                }
            }
            sql = sql.TrimEnd(new char[] { ' ', ',' });
            sql += ")";
            return sql;
        }
    }
}