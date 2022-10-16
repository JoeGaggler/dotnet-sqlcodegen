using Pingmint.CodeGen.Sql.Model;
using System.Data;

namespace Pingmint.CodeGen.Sql;

public class Generator
{
    public async Task Generate(Config config, TextWriter textWriter)
    {
        var cs = config.CSharp ?? throw new NullReferenceException();
        var className = cs.ClassName ?? throw new NullReferenceException();
        var fileNs = cs.Namespace ?? throw new NullReferenceException();
        var dbs = config.Databases?.Items ?? throw new NullReferenceException();

        var code = new CodeWriter();
        code.UsingNamespace("System.Data");
        code.UsingNamespace("Microsoft.Data.SqlClient");
        code.Line();
        code.FileNamespace(fileNs);
        code.Line();
        using (code.PartialClass("public static", className))
        {
            foreach (var database in dbs)
            {
                var databaseName = database.Name ?? throw new NullReferenceException();
                var statements = database.Statements?.Items ?? throw new NullReferenceException();

                var statementMemos = new List<StatementMemo>();

                foreach (var statement in statements)
                {
                    var name = statement.Name ?? throw new NullReferenceException();
                    var resultSet = statement.ResultSet ?? throw new NullReferenceException();
                    var commandText = statement.Text ?? throw new NullReferenceException();

                    var memo = new StatementMemo()
                    {
                        CommandText = commandText,
                        MethodName = $"{name}",
                        RowClassName = $"{name}Row",
                        RowClassRef = $"{databaseName}.{name}Row",
                        DatabaseName = databaseName,
                        Columns = statement.ResultSet.Columns.Select(i => new ColumnMemo()
                        {
                            OrdinalVarName = $"ord_{i.Name}",
                            ColumnName = i.Name,
                            PropertyTypeName = GetStringForType(GetDotnetType(i.Type), i.IsNullable),
                            PropertyName = i.Name,
                        }).ToList(),
                    };
                    statementMemos.Add(memo);

                    CodeSqlStatement(code, memo);
                }

                foreach (var memo in statementMemos)
                {
                    CodeSqlStatementResultSet(code, memo);
                }
            }
        }

        textWriter.Write(code.ToString());
    }

    private static void CodeSqlStatement(CodeWriter code, StatementMemo statementMemo)
    {
        var rowClassRef = statementMemo.RowClassRef ?? throw new NullReferenceException();

        var commandText = statementMemo.CommandText ?? throw new NullReferenceException();

        var resultType = String.Format("List<{0}>", rowClassRef);
        var returnType = String.Format("Task<{0}>", resultType);
        var methodName = (statementMemo.MethodName ?? throw new NullReferenceException()) + "Async";
        var args = "SqlConnection connection"; // TODO: parameters, then transaction

        using (code.Method("public static async", returnType, methodName, args))
        {
            code.Line("using SqlCommand cmd = connection.CreateCommand();");
            code.Line("cmd.CommandType = CommandType.Text;");
            code.Line("cmd.CommandText = \"{0}\";", commandText);
            code.Line();
            code.Line("var result = new {0}();", resultType);
            using (code.Using("var reader = await cmd.ExecuteReaderAsync()"))
            {
                using (code.If("await reader.ReadAsync()"))
                {
                    foreach (var column in statementMemo.Columns)
                    {
                        code.Line("int {0} = reader.GetOrdinal(\"{1}\");", column.OrdinalVarName, column.ColumnName);
                    }
                    code.Line();
                    using (code.DoWhile("await reader.ReadAsync()"))
                    {
                        code.Line("var row = new {0}", rowClassRef);
                        using (code.CreateBraceScope(null, ";"))
                        {
                            foreach (var column in statementMemo.Columns)
                            {
                                code.Line("{0} = reader.IsDBNull({1}) ? null! : reader.GetFieldValue<{2}>({1}),", column.PropertyName, column.OrdinalVarName, column.PropertyTypeName);
                            }
                        }
                        code.Line("result.Add(row);");
                    }
                }
            }
            code.Return("result");
        }
    }

    private static void CodeSqlStatementResultSet(CodeWriter code, StatementMemo statementMemo)
    {
        var rowClassName = statementMemo.RowClassName ?? throw new NullReferenceException();
        var columns = statementMemo.Columns ?? throw new NullReferenceException();

        using (code.PartialClass("public", statementMemo.DatabaseName))
        {
            using (code.PartialClass("public", rowClassName))
            {
                foreach (var column in columns)
                {
                    code.Line($"public {column.PropertyTypeName} {column.PropertyName} {{ get; set; }}");
                }
            }
        }
    }

    public static Type GetDotnetType(SqlDbType type) => type switch
    {
        SqlDbType.Char or
        SqlDbType.NText or
        SqlDbType.NVarChar or
        SqlDbType.Text or
        SqlDbType.VarChar or
        SqlDbType.Xml
        => typeof(String),

        SqlDbType.Int => typeof(Int32),

        SqlDbType.DateTimeOffset => typeof(DateTimeOffset),

        SqlDbType.Bit => typeof(Boolean),

        _ => throw new InvalidOperationException("Unexpected SqlDbType: " + type.ToString()),
    };

    public static String GetStringForType(Type type, Boolean isColumnNullable) => (isColumnNullable) ?
        $"{GetShortestNameForType(type)}?" :
        $"{GetShortestNameForType(type)}";

    public static String GetShortestNameForType(Type type) => type switch
    {
        var x when x == typeof(DateTime) => "DateTime",
        var x when x == typeof(DateTimeOffset) => "DateTimeOffset",
        var x when x == typeof(Int16) => "Int16",
        var x when x == typeof(Int32) => "Int32",
        var x when x == typeof(String) => "String",
        var x when x == typeof(Boolean) => "Boolean",
        _ => throw new ArgumentException($"GetShortestNameForType({type.FullName}) not defined."),
    };

    private class StatementMemo
    {
        public String CommandText { get; set; }
        public String MethodName { get; set; }
        public String RowClassName { get; set; }
        public String RowClassRef { get; set; }
        public String DatabaseName { get; set; }

        public List<ColumnMemo> Columns { get; set; }
    }

    private class ColumnMemo
    {
        public String OrdinalVarName { get; set; }
        public String ColumnName { get; set; }
        public String PropertyTypeName { get; set; }
        public String PropertyName { get; set; }
    }
}
