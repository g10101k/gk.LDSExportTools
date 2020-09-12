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
        private const string ActionImporter = "importer";
        private const string ActionFeature = "feature";
        private const string ActionMeta = "meta";
        private const string ActionProc = "proc";
        static string ServerName = "";
        static string IntegratedSecurity = "";
        static string User = "";
        static string Password = "";
        static string DataBaseName = "";
        static string ObjectName = "";
        static string Action = "";
        static string DefaultOutput = "Export.sql";
        static string Output = DefaultOutput;

        static bool show_help = false;
        public static void Main(string[] args)
        {

            try
            {
                OptionSet p = new OptionSet() {
                    { "s|srv=", "Server name.", value => ServerName = value },
                    { "a|aut=", "IntegratedSecurity (true|false).", value => IntegratedSecurity = value },
                    { "u|usr=", "SQL user name.",  value => User = value },
                    { "p|pwd=", "SQL passwor.", value => Password = value },
                    { "d|db=", "Database name.", value => DataBaseName = value },
                    { "n|name=", "Mask of name export object.", value => ObjectName = value },
                    { "t|type=", "Type action. "+ActionImporter+"|"+ActionFeature+"|"+ActionMeta+"|"+ActionProc+"", value => Action = value },
                    { "o|output=", "Name of output file, default "+DefaultOutput, value => Output = value },
                    { "h|help", "Show this message and exit.", v => show_help = v != null },
                };
                p.Parse(args);
                if (show_help) { ShowHelp(p); return; }
                ConnectAndExecute(args);
            }
            catch (OptionException e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                Console.WriteLine("Try 'i-lds-imp-exp --help' for more information.");
                return;
            }
        }

        /// <summary>
        /// Выводит справку в консоль
        /// </summary>
        /// <param name="p"></param>
        static void ShowHelp(OptionSet p)
        {
            Console.WriteLine("Использование: gk.LDSExportTools [ОПЦИИ]");
            Console.WriteLine("");
            Console.WriteLine();
            Console.WriteLine("Например выгрузка процедуры или пользовательской функции: ");
            Console.WriteLine();
            Console.WriteLine("gk.LDSExportTools.exe -t proc -s 192.168.37.40\\MSSQL2014 -u sa -p strongpassword -a false -d \"I-LDS\" -n TechSync -o Script.sql");
            Console.WriteLine("Options:");
            p.WriteOptionDescriptions(Console.Out);
        }


        /// <summary>
        /// Подключаемся к БД и запускаем выполнение
        /// </summary>
        /// <param name="args"></param>
        private static void ConnectAndExecute(string[] args)
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
            Server server = new Server(ServerName);
            try
            {
                if (string.IsNullOrEmpty(IntegratedSecurity))
                {
                    string DefaultIntegratedSecurity = ConfigurationManager.AppSettings["DefaultIntegratedSecurity"].ToString();
                    Console.Write("IntegratedSecurity [" + DefaultIntegratedSecurity + "]: ");
                    string SqlSecurity = Console.ReadLine();
                    bool SqlSecurityBool = (Boolean.TryParse(SqlSecurity, out SqlSecurityBool)) ? SqlSecurityBool : Convert.ToBoolean(DefaultIntegratedSecurity);
                    server.ConnectionContext.LoginSecure = SqlSecurityBool;
                }
                else
                {
                    Console.WriteLine("Integrated Security: {0}", IntegratedSecurity);
                    bool SqlSecurityBool = (Boolean.TryParse(IntegratedSecurity, out SqlSecurityBool)) ? SqlSecurityBool : Convert.ToBoolean(IntegratedSecurity);
                    server.ConnectionContext.LoginSecure = SqlSecurityBool;
                }

                if (!server.ConnectionContext.LoginSecure)
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
                    server.ConnectionContext.Login = User;

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
                    server.ConnectionContext.Password = Password;
                }
                server.ConnectionContext.Connect();

                //Создаем экземпляр класса базы данных
                string DefaultDataBaseName = ConfigurationManager.AppSettings["DefaultDataBaseName"].ToString();
                if (string.IsNullOrEmpty(DataBaseName))
                {
                    int i = 0;
                    Dictionary<int, string> d = new Dictionary<int, string>();
                    foreach (var db in server.Databases)
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

                Database dbLds = server.Databases[DataBaseName];

                if (dbLds == null)
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

                    if (string.IsNullOrEmpty(Action))
                    {
                        string DefaultAction = ConfigurationManager.AppSettings["DefaultAction"].ToString();
                        Console.Write("Action (importer|feature|meta|proc) [{0}]: ", DefaultAction);
                        Action = Console.ReadLine();
                        Action = (string.IsNullOrEmpty(Password)) ? DefaultAction : Action;
                    }
                    else
                    {
                        Console.WriteLine("Action: {0}", Password);
                    }


                    if (Action == ActionImporter)
                    {
                        ImporterSql(dbLds, path);
                    }
                    else if (Action == ActionFeature)
                    {
                        FeatureNodeSql(dbLds, path);
                    }
                    else if (Action == ActionMeta)
                    {
                        MetaObjectSql(dbLds, path);
                    }
                    else if (Action == ActionProc)
                    {
                        ProcedureAndObject(server, dbLds, path);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
            //Закрыть соединение
            server.ConnectionContext.Disconnect();
        }

        /// <summary>
        /// Выгрузка процедур и функций из MS SQL
        /// </summary>
        /// <param name="server"></param>
        /// <param name="db"></param>
        /// <param name="path"></param>
        public static void ProcedureAndObject(Server server, Database db, string path)
        {
            string sql = "";

            Console.Write("Object name like: ");
            string ObjName;
            if (string.IsNullOrEmpty(ObjectName))
            {
                ObjName = Console.ReadLine();
            }
            else
            {
                ObjName = ObjectName;
                Console.WriteLine(ObjectName);
            }

            //Создаем экземпляр класса, который будет генерировать скрипты
            Scripter scripter = new Scripter(server);

            foreach (StoredProcedure proc in db.StoredProcedures)
            {
                sql = "";
                if (proc.Name.Contains(ObjName))
                {
                    Console.WriteLine(proc.Name);
                    scripter.Options.ScriptDrops = true;

                    foreach (string current2 in scripter.EnumScript(new Urn[] { proc.Urn }))
                    {
                        sql += current2 + "\r\nGO\r\n";
                    }
                    scripter.Options.ScriptDrops = false;

                    foreach (string current2 in scripter.EnumScript(new Urn[] { proc.Urn }))
                    {
                        sql += current2 + "\r\nGO\r\n";
                    }

                    scripter.Options.IncludeHeaders = true;
                    scripter.Options.SchemaQualify = true;

                    string fPath = string.Format("{0}{1}", path, Output);
                    //try { File.Delete(fPath); } catch { }
                    File.AppendAllText(fPath, sql, System.Text.Encoding.UTF8);
                }
            }

            foreach (UserDefinedFunction func in db.UserDefinedFunctions)
            {
                sql = "";
                string objName = func.ToString();
                if (objName.Contains(ObjName))
                {
                    Console.WriteLine(objName);
                    scripter.Options.ScriptDrops = true;

                    foreach (string current2 in scripter.EnumScript(new Urn[] { func.Urn }))
                    {
                        sql += current2 + "\r\nGO\r\n";
                    }
                    scripter.Options.ScriptDrops = false;

                    foreach (string current2 in scripter.EnumScript(new Urn[] { func.Urn }))
                    {
                        sql += current2 + "\r\nGO\r\n";
                    }

                    scripter.Options.IncludeHeaders = true;
                    scripter.Options.SchemaQualify = true;

                    string fPath = string.Format("{0}{1}", path, Output);
                    File.AppendAllText(fPath, sql, System.Text.Encoding.UTF8);
                }
            }
        }

        /// <summary>
        /// Создание скрипта SQL для MetaObject
        /// </summary>
        /// <param name="db"></param>
        /// <param name="path"></param>
        private static void MetaObjectSql(Database db, string path)
        {
            string text = "";
            Console.Write("MetaObject name like: ");
            string ObjName;
            if (string.IsNullOrEmpty(ObjectName))
            {
                ObjName = Console.ReadLine();
            }
            else
            {
                ObjName = ObjectName;
                Console.WriteLine(ObjectName);
            }

            string tblName = "MetaObject";
            string sql = string.Format(@"SELECT * FROM [dbo].[MetaObject] WHERE [Name] like '%{0}%'", ObjName);
            DataSet ds = db.ExecuteWithResults(sql);
            ds.Tables[0].TableName = tblName;
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                string id = row["ObjectUid"].ToString();
                Console.WriteLine("Exporting MetaObject: {0}", row["Name"]);
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


                text += GenerateSql(db, row, "") + "\r\n\r\n";
                text += NotRefTableSql(db, "MetaObjectCommand", "ObjectUid", id);

                text += @"

END;
GO
ENABLE TRIGGER [TR_MetaObject_Rollback] ON [MetaObject];
ENABLE TRIGGER [TR_MetaObjectCommand_Rollback] ON [MetaObjectCommand];
GO
";


            }
            string fPath = string.Format("{0}{1}", path, Output);
            File.AppendAllText(fPath, text);

        }

        /// <summary>
        /// Выгрузка экземпляра функциональной возможности
        /// </summary>
        /// <param name="db"></param>
        /// <param name="path"></param>
        private static void FeatureNodeSql(Database db, string path)
        {
            string text = "";
            Console.Write("FeatureNode name like: ");
            string ReportName;
            if (string.IsNullOrEmpty(ObjectName))
            {
                ReportName = Console.ReadLine();
            }
            else
            {
                ReportName = ObjectName;
                Console.WriteLine(ObjectName);
            }

            string tblName = "FeatureNode";
            string sql = string.Format(@"SELECT [FeatureNodeId]
      ,[FeatureTypeId]
      ,[Name]
      ,[Flags]
      ,[NodeOrder]
  FROM [dbo].[FeatureNode]  
  WHERE [Name] like '%{0}%'", ReportName);
            DataSet ds = db.ExecuteWithResults(sql);
            ds.Tables[0].TableName = tblName;
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                string id = row["FeatureNodeId"].ToString();
                Console.WriteLine("Exporting report: {0}", row["Name"]);

                text += GenerateSqlWithDelete(row, "") + "\r\n\r\n";
                text += NotRefTableSql(db, "FeatureNodeRights", "FeatureNodeId", id);
                text += NotRefTableSql(db, "FeatureNodeContent", "FeatureNodeId", id);
                text += NotRefTableSql(db, "FeatureNodeGraphics", "FeatureNodeId", id);
                text += "\r\nGO\r\n";
            }

            string fPath = string.Format("{0}{1}", path, Output);
            File.AppendAllText(fPath, text);

        }

        /// <summary>
        /// Выгрузка импортера из БД ЛИМС
        /// </summary>
        /// <param name="db"></param>
        /// <param name="path"></param>
        /// <returns></returns>
        public static string ImporterSql(Database db, string path)
        {
            try
            {
                string text = "";
                string DefaultImporterName = ConfigurationManager.AppSettings["DefaultImporterName"].ToString();
                if (string.IsNullOrEmpty(ObjectName))
                {
                    Console.Write("Importer name like  [{0}]: ", DefaultImporterName);
                    ObjectName = Console.ReadLine();
                    ObjectName = (string.IsNullOrEmpty(ObjectName)) ? DefaultImporterName : ObjectName;
                }
                else
                {
                    Console.WriteLine("Importer name like: {0}", ObjectName);
                }

                string tblName = "Importer";
                string sql = string.Format("select * from [{0}] where Name like '{1}'", tblName, ObjectName);
                DataSet ds = db.ExecuteWithResults(sql);
                ds.Tables[0].TableName = tblName;
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    text = "";
                    string id = row["ImporterUid"].ToString();
                    Console.WriteLine("Exporting importer: {0}", row["Name"]);

                    text += GenerateSqlWithUpdate(db, row) + "\r\n\r\n";
                    text += ImporterInstancerSql(db, "ImporterInstance", "ImporterUid", id);
                    text += ImporterTestSql(db, "ImporterTest", "ImporterUid", id);

                    string fPath = string.Format("{0}{1}", path, Output);
                    File.AppendAllText(fPath, text, System.Text.Encoding.UTF8);
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

        /// <summary>
        /// Выгружаем таблицу ImporterInstancer
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tblName"></param>
        /// <param name="whereclm"></param>
        /// <param name="findId"></param>
        /// <returns></returns>
        public static string ImporterInstancerSql(Database db, string tblName, string whereclm, string findId)
        {
            string sql = string.Format("select * from [dbo].[{0}] where [{1}] = '{2}'", tblName, whereclm, findId);
            DataSet ds = db.ExecuteWithResults(sql);
            ds.Tables[0].TableName = tblName;
            string text = "";
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                string id = row["ImporterInstanceUid"].ToString();
                text += GenerateSqlWithUpdate(db, row) + "\r\n\r\n";
                text += NotRefTableSql(db, "ImporterInstanceSubdivision", "ImporterInstanceUid", id);
                text += NotRefTableSql(db, "ImporterInstanceCP", "ImporterInstanceUid", id);
                text += NotRefTableSql(db, "ImporterInstanceProduct", "ImporterInstanceUid", id);
                text += NotRefTableSql(db, "ImporterInstanceEqpType", "ImporterInstanceUid", id);
                text += NotRefTableSql(db, "ImporterInstanceEqp", "ImporterInstanceUid", id);
                text += NotRefTableSql(db, "ImporterMapping", "ImporterInstanceUid", id);
            }
            return text;
        }

        /// <summary>
        /// Выгружаем таблицу ImporterTest
        /// </summary>
        /// <param name="db"></param>
        /// <param name="tblName"></param>
        /// <param name="whereclm"></param>
        /// <param name="findId"></param>
        /// <returns></returns>
        public static string ImporterTestSql(Database db, string tblName, string whereclm, string findId)
        {
            string sql = string.Format("select * from [dbo].[{0}] where [{1}] = '{2}'", tblName, whereclm, findId);
            DataSet ds = db.ExecuteWithResults(sql);
            ds.Tables[0].TableName = tblName;
            string text = "";
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                string id = row["ImporterTestUid"].ToString();
                text += GenerateSqlWithUpdate(db, row) + "\r\n\r\n";
                text += NotRefTableSql(db, "ImporterTestTechTest", "ImporterTestUid", id);
            }
            return text;
        }

        /// <summary>
        /// Получаем запрос на вставку зависимых записей из таблицы tblName
        /// </summary>
        /// <param name="db">База данных</param>
        /// <param name="tblName">Таблица</param>
        /// <param name="whereColumn"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public static string NotRefTableSql(Database db, string tblName, string whereColumn, string id)
        {
            try
            {
                string sql = string.Format("select * from [dbo].[{0}] where [{1}] = '{2}'", tblName, whereColumn, id);
                DataSet ds = db.ExecuteWithResults(sql);
                ds.Tables[0].TableName = tblName;
                string text = "";
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    text += GenerateSqlWithUpdate(db, row) + "\r\nGO\r\n";
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

        /// <summary>
        /// Возвращает запрос на вставку для строки row
        /// </summary>
        /// <param name="row"></param>
        /// <param name="where"></param>
        /// <returns></returns>
        public static string GenerateSql(Database db, DataRow row, string where)
        {
            string tblName = row.Table.TableName;
            string[] keys = GetKeysColumns(db.Tables[tblName, "dbo"]);
            string sql = string.Format("INSERT [dbo].[{0}] (", tblName);

            foreach (DataColumn col in row.Table.Columns)
            {
                sql += string.Format("[{0}], ", col.Caption);
            }

            sql = sql.TrimEnd(new char[] { ' ', ',' });
            sql += ") VALUES (";

            foreach (DataColumn col in row.Table.Columns)
            {
                if (col.Caption.ToLower() == "rv" && col.DataType == typeof(byte[]))
                {
                    sql += "default, ";
                }
                else
                {
                    sql += string.Format("{0}, ", GetValueForQuery(row[col]));
                }
            }
            sql = sql.TrimEnd(new char[] { ' ', ',' });
            sql += ")";
            return sql;
        }

        /// <summary>
        /// Возвращает запрос на удаление и вставку для записи
        /// </summary>
        /// <param name="row"></param>
        /// <param name="where"></param>
        /// <returns></returns>
        public static string GenerateSqlWithDelete(DataRow row, string where)
        {
            string tblName = row.Table.TableName;

            string sql = "";

            sql += string.Format("DELETE FROM [dbo].[{0}] WHERE 1=1 ", tblName);

            foreach (DataColumn col in row.Table.Columns)
            {


                object o = row[col];
                string s = o.ToString();
                string _null = "null ";
                if (col.Caption.ToLower().EndsWith("id"))
                {
                    if (col.DataType == typeof(Guid))
                    {
                        sql += string.Format("AND [{0}] = {1}", col.Caption, (s != "") ? string.Format("N'{0}' ", o) : _null);
                        //sql += ;
                    }
                    else if (col.DataType == typeof(Int32))
                    {
                        sql += string.Format("AND [{0}] = {1}", col.Caption, (s != "") ? string.Format("{0} ", s) : _null);
                    }
                }
            }

            sql += "\r\ngo\r\n";
            sql += string.Format("INSERT [dbo].[{0}] (", tblName);

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

        /// <summary>
        /// Генерирует SQL скрипт Update и  Insert для записи в таблице
        /// </summary>
        /// <param name="db"></param>
        /// <param name="row">Строка</param>
        /// <param name="uidColumn">Колонки идентификаторы</param>
        /// <returns></returns>
        public static string GenerateSqlWithUpdate(Database db, DataRow row)
        {
            string tblName = row.Table.TableName;

            string whereUidColumns = string.Empty;
            string[] uidColumn = GetKeysColumns(db.Tables[tblName]);
            foreach (string clm in uidColumn)
            {
                whereUidColumns += string.Format(@" AND {0} = {1}", clm, GetValueForQuery(row[clm]));
            }
            whereUidColumns = whereUidColumns.Substring(5);

            string sql = string.Empty;
            string sqlInsert = string.Empty;
            string sqlUpdate = "\tUPDATE " + tblName + " \r\n\tSET \r\n{0} \r\n\tWHERE \r\n\t\t{1}";


            sql += string.Format(@"IF EXISTS (SELECT * FROM [{0}] WHERE {1})
BEGIN
{2}
END
ELSE
BEGIN
{3}
END", tblName, whereUidColumns, "{0}", "{1}");
            string updateSet = string.Empty;

            foreach (DataColumn col in row.Table.Columns)
            {
                if (!whereUidColumns.Contains(col.Caption) && !(col.Caption.ToLower() == "rv"))
                {
                    updateSet += string.Format("\t\t[{0}] = {1}, \r\n", col.Caption, GetValueForQuery(row[col]));
                }
            }
            updateSet = updateSet.Substring(0, updateSet.Length - 4);

            sqlUpdate = string.Format(sqlUpdate, updateSet, whereUidColumns);

            sqlInsert += string.Format("\tINSERT [dbo].[{0}] (", tblName);

            foreach (DataColumn col in row.Table.Columns)
            {
                sqlInsert += string.Format("[{0}], ", col.Caption);
            }

            sqlInsert = sqlInsert.TrimEnd(new char[] { ' ', ',' });
            sqlInsert += ") \r\n\tVALUES (";

            foreach (Column col in db.Tables[tblName].Columns)
            {
                if (col.DataType.Name == DataType.Timestamp.Name)
                {
                    sqlInsert += "default, ";
                }
                else
                {
                    sqlInsert += string.Format("{0}, ", GetValueForQuery(row[col.Name]));
                }
            }
            sqlInsert = sqlInsert.TrimEnd(new char[] { ' ', ',' });
            sqlInsert += ")";

            sql = string.Format(sql, sqlUpdate, sqlInsert);
            return sql;
        }

        /// <summary>
        /// Возвращает строковое представление значения для запроса
        /// </summary>
        /// <param name="val">Значение</param>
        /// <returns></returns>
        public static string GetValueForQuery(object val)
        {
            string sql = string.Empty;
            string s = val.ToString();
            string _null = "null";
            Type type = val.GetType();
            if (type == typeof(DBNull))
            {
                sql += _null;
            }
            if (type == typeof(Guid))
            {
                sql += (s != string.Empty) ? string.Format("'{0}'", val) : _null;
            }
            else if (type == typeof(String) || type == typeof(string))
            {
                sql += (s != string.Empty) ? string.Format("N'{0}'", s.Replace("'", "''")) : _null;
            }
            else if (type == typeof(Boolean))
            {
                sql = (s != string.Empty) ? ((bool)val) ? "1" : "0" : _null;
            }
            else if (type == typeof(Int32))
            {
                sql += (s != "") ? string.Format("{0}", s) : _null;
            }
            else if (type == typeof(Double))
            {
                sql += (s != "") ? string.Format("{0}", val) : _null;
            }
            else if (type == typeof(byte[]))
            {
                sql += (s != "") ? "0x" + BitConverter.ToString((byte[])val).Replace("-", "") : _null;
            }
            return sql;
        }

        /// <summary>
        /// Получить перечень ключевых полей в таблице
        /// </summary>
        /// <param name="tbl"></param>
        /// <returns></returns>
        public static string[] GetKeysColumns(Table tbl)
        {
            List<string> list = new List<string>();

            foreach (Column column in tbl.Columns)
            {
                if ((bool)column.Properties["InPrimaryKey"].Value == true)
                {
                    list.Add(column.Name);
                }
            }
            return list.ToArray();
        }
    }
}