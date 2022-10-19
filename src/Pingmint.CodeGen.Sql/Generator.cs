using Pingmint.CodeGen.Sql.Model;
using System.Data;

namespace Pingmint.CodeGen.Sql;

public static class Generator
{
    public static async Task GenerateAsync(Config config, TextWriter textWriter)
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
            WriteHelperMethods(code);
            code.Line();

            foreach (var database in dbs)
            {
                var databaseName2 = database.Name ?? throw new NullReferenceException();
                var databaseMemo = new DatabaseMemo
                {
                    Name = databaseName2,
                    ClassName = GetPascalCase(database.ClassName ?? databaseName2),
                };

                var commandMemos = new List<ICommandMemo>();

                var procs = database.Procedures?.Items ?? new();
                foreach (var proc in procs)
                {
                    var name = proc.Name ?? throw new NullReferenceException();
                    var resultSet = proc.ResultSet ?? throw new NullReferenceException();
                    var commandText = proc.Text ?? throw new NullReferenceException();
                    var parameters = proc.Parameters?.Items ?? throw new NullReferenceException();
                    var columns = proc.ResultSet.Columns ?? throw new NullReferenceException();

                    var memo = new ProcedureMemo()
                    {
                        CommandText = proc.Text,
                        MethodName = $"{name}",
                        RowClassName = $"{name}Row",
                        RowClassRef = $"{databaseMemo.ClassName}.{name}Row",
                        DatabaseName = databaseMemo.Name,
                        Parameters = GetCommandParameters(proc.Parameters?.Items ?? new List<Parameter>()),
                        Columns = GetCommandColumns(columns),
                    };

                    commandMemos.Add(memo);
                    CodeSqlStatement(code, memo);
                    code.Line();
                }

                var statements = database.Statements?.Items ?? new();

                foreach (var statement in statements)
                {
                    var name = statement.Name ?? throw new NullReferenceException();
                    var resultSet = statement.ResultSet ?? throw new NullReferenceException();
                    var commandText = statement.Text ?? throw new NullReferenceException();
                    var columns = statement.ResultSet.Columns ?? throw new NullReferenceException();

                    var memo = new StatementMemo()
                    {
                        CommandText = commandText.ReplaceLineEndings(" ").Trim(),
                        MethodName = $"{name}",
                        RowClassName = $"{name}Row",
                        RowClassRef = $"{databaseMemo.ClassName}.{name}Row",
                        DatabaseName = databaseMemo.Name,
                        Parameters = GetCommandParameters(statement.Parameters?.Items ?? new List<Parameter>()),
                        Columns = GetCommandColumns(columns),
                    };
                    commandMemos.Add(memo);

                    CodeSqlStatement(code, memo);
                    code.Line();
                }

                using (code.PartialClass("public", databaseMemo.ClassName))
                {
                    foreach (var memo in commandMemos.OrderBy(i => i.RowClassName))
                    {
                        CodeSqlStatementResultSet(code, memo);
                        code.Line();
                    }
                }
            }
        }

        textWriter.Write(code.ToString());
        await textWriter.FlushAsync();
    }

     private static List<ParametersMemo> GetCommandParameters(List<Parameter> parameters) =>
        parameters.Select(i => new ParametersMemo()
        {
            ParameterType = i.SqlDbType,
            ParameterName = i.Name ?? throw new NullReferenceException(),
            ArgumentType = GetShortestNameForType(GetDotnetType(i.SqlDbType)),
            ArgumentName = GetCamelCase(i.Name),
        }).ToList();

    private static List<ColumnMemo> GetCommandColumns(List<Column> columns) =>
        columns.Select(i => new ColumnMemo()
        {
            OrdinalVarName = $"ord{GetPascalCase(i.Name)}",
            ColumnName = i.Name,
            ColumnIsNullable = i.IsNullable,
            PropertyType = GetDotnetType(i.Type),
            PropertyTypeName = GetStringForType(GetDotnetType(i.Type), i.IsNullable),
            PropertyName = GetPascalCase(i.Name),
            FieldTypeName = GetShortestNameForType(GetDotnetType(i.Type)),
        }).ToList();

    private static void WriteHelperMethods(CodeWriter code)
    {
        code.Text(
"""
    private static SqlParameter CreateParameter(String parameterName, Object? value, SqlDbType sqlDbType, Int32 size = -1, ParameterDirection direction = ParameterDirection.Input) => new SqlParameter(parameterName, value ?? DBNull.Value)
    {
        Size = size,
        Direction = direction,
        SqlDbType = sqlDbType,
    };

    private static SqlParameter CreateParameter(String parameterName, Object? value, SqlDbType sqlDbType, String typeName, Int32 size = -1, ParameterDirection direction = ParameterDirection.Input) => new SqlParameter(parameterName, value ?? DBNull.Value)
    {
        Size = size,
        Direction = direction,
        TypeName = typeName,
        SqlDbType = sqlDbType,
    };

    private static T? GetField<T>(SqlDataReader reader, int ordinal) where T : class => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
    private static T? GetFieldValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
    private static T GetNonNullField<T>(SqlDataReader reader, int ordinal) where T : class => reader.IsDBNull(ordinal) ? throw new NullReferenceException() : reader.GetFieldValue<T>(ordinal);
    private static T GetNonNullFieldValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.IsDBNull(ordinal) ? throw new NullReferenceException() : reader.GetFieldValue<T>(ordinal);

    private static SqlCommand CreateStatement(SqlConnection connection, String text) => new() { Connection = connection, CommandType = CommandType.Text, CommandText = text, };
    private static SqlCommand CreateStoredProcedure(SqlConnection connection, String text) => new() { Connection = connection, CommandType = CommandType.StoredProcedure, CommandText = text, };

""");
    }

    private static void CodeSqlStatement<TCommandMemo>(CodeWriter code, TCommandMemo commandMemo) where TCommandMemo : ICommandMemo
    {
        var rowClassRef = commandMemo.RowClassRef ?? throw new NullReferenceException();

        var commandText = commandMemo.CommandText ?? throw new NullReferenceException();

        var resultType = String.Format("List<{0}>", rowClassRef);
        var returnType = String.Format("Task<{0}>", resultType);
        var methodName = (commandMemo.MethodName ?? throw new NullReferenceException()) + "Async";

        var args = "SqlConnection connection"; // TODO: parameters, then transaction
        if (commandMemo.Parameters is { } parameters && parameters.Count > 0)
        {
            var args1 = String.Join(", ", parameters?.Select(i => $"{i.ArgumentType} {i.ArgumentName}"));
            args += ", " + args1;
        }

        using (code.Method("public static async", returnType, methodName, args))
        {
            var commandType = commandMemo.CommandType switch
            {
                CommandType.Text => CommandType.Text.ToString(),
                CommandType.StoredProcedure => CommandType.StoredProcedure.ToString(),
                var x => throw new InvalidOperationException($"unsupported command type: {x}")
            };

            var commandMethod = commandMemo.CommandType switch {
                CommandType.Text => "CreateStatement",
                CommandType.StoredProcedure => "CreateStoredProcedure",
                var x => throw new InvalidOperationException($"unsupported command type: {x}")
            };

            code.Line($"using SqlCommand cmd = {commandMethod}(connection, \"{commandText}\");");
            code.Line();

            if (commandMemo.Parameters is { } parameters2 && parameters2.Count > 0) // TODO: reuse parameters
            {
                foreach (var parameter in parameters2)
                {
                    code.Line($"cmd.Parameters.Add(CreateParameter(\"@{parameter.ParameterName.TrimStart('@')}\", {parameter.ArgumentName}, SqlDbType.{parameter.ParameterType}));");
                }
                code.Line();
            }

            code.Line("var result = new {0}();", resultType);
            code.Line("using var reader = await cmd.ExecuteReaderAsync();");
            using (code.If("await reader.ReadAsync()"))
            {
                foreach (var column in commandMemo.Columns)
                {
                    code.Line("int {0} = reader.GetOrdinal(\"{1}\");", column.OrdinalVarName, column.ColumnName);
                }
                code.Line();
                using (code.DoWhile("await reader.ReadAsync()"))
                {
                    code.Line("result.Add(new {0}", rowClassRef);
                    using (code.CreateBraceScope(null, ");"))
                    {
                        foreach (var column in commandMemo.Columns)
                        {
                            var line = (column.PropertyType.IsValueType, column.ColumnIsNullable) switch
                            {
                                (false, true) => String.Format("{0} = GetField<{2}>(reader, {1}),", column.PropertyName, column.OrdinalVarName, column.FieldTypeName),
                                (true, true) => String.Format("{0} = GetFieldValue<{2}>(reader, {1}),", column.PropertyName, column.OrdinalVarName, column.FieldTypeName),
                                (false, false) => String.Format("{0} = GetNonNullField<{2}>(reader, {1}),", column.PropertyName, column.OrdinalVarName, column.FieldTypeName),
                                (true, false) => String.Format("{0} = GetNonNullFieldValue<{2}>(reader, {1}),", column.PropertyName, column.OrdinalVarName, column.FieldTypeName),
                            };
                            code.Line(line);
                        }
                    }
                }
            }
            code.Return("result");
        }
    }

    private static void CodeSqlStatementResultSet(CodeWriter code, ICommandMemo commandMemo)
    {
        var rowClassName = commandMemo.RowClassName ?? throw new NullReferenceException();
        var columns = commandMemo.Columns ?? throw new NullReferenceException();

        using (code.PartialClass("public", rowClassName))
        {
            foreach (var column in columns)
            {
                code.Line($"public {column.PropertyTypeName} {column.PropertyName} {{ get; set; }}");
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

        SqlDbType.DateTimeOffset => typeof(DateTimeOffset),

        SqlDbType.Bit => typeof(Boolean),
        SqlDbType.Int => typeof(Int32),
        SqlDbType.TinyInt => typeof(Byte),
        SqlDbType.SmallInt => typeof(Int16),

        _ => throw new InvalidOperationException("Unexpected SqlDbType: " + type.ToString()),
    };

    private static String GetDBNullExpression(Type type, Boolean isColumnNullable) => (type.IsValueType, isColumnNullable) switch
    {
        (_, true) => "null",
        (false, _) => "throw new NullReferenceException()",
        (true, false) => "default",
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
        var x when x == typeof(Byte) => "Byte",
        var x when x == typeof(Int16) => "Int16",
        _ => throw new ArgumentException($"GetShortestNameForType({type.FullName}) not defined."),
    };

    public static String GetCamelCase(String originalName)
    {
        var sb = new System.Text.StringBuilder(originalName.Length);

        bool firstChar = true;
        bool firstWord = true;
        foreach (var ch in originalName)
        {
            if (firstChar)
            {
                if (!Char.IsLetter(ch)) { continue; }

                if (firstWord)
                {
                    sb.Append(Char.ToLowerInvariant(ch));
                    firstWord = false;
                }
                else
                {
                    sb.Append(Char.ToUpperInvariant(ch));
                }
                firstChar = false;
            }
            else if (Char.IsLetterOrDigit(ch))
            {
                sb.Append(ch);
            }
            else
            {
                firstChar = true;
            }
        }

        return sb.ToString();
    }

    public static String GetPascalCase(String originalName)
    {
        var sb = new System.Text.StringBuilder(originalName.Length);

        bool firstChar = true;
        foreach (var ch in originalName)
        {
            if (firstChar)
            {
                if (!Char.IsLetter(ch)) { continue; }

                sb.Append(Char.ToUpperInvariant(ch));
                firstChar = false;
            }
            else if (Char.IsLetterOrDigit(ch))
            {
                sb.Append(ch);
            }
            else
            {
                firstChar = true;
            }
        }

        return sb.ToString();
    }

    private class DatabaseMemo
    {
        public String Name { get; set; }
        public String ClassName { get; set; }
    }

    private interface ICommandMemo
    {
        CommandType CommandType { get; }
        String CommandText { get; set; }
        String MethodName { get; set; }
        String RowClassName { get; set; }
        String RowClassRef { get; set; }
        String DatabaseName { get; set; }
        List<ColumnMemo> Columns { get; set; }
        List<ParametersMemo> Parameters { get; set; }
    }

    private class ProcedureMemo : ICommandMemo
    {
        public CommandType CommandType => CommandType.StoredProcedure;
        public String CommandText { get; set; }
        public String MethodName { get; set; }
        public String RowClassName { get; set; }
        public String RowClassRef { get; set; }
        public String DatabaseName { get; set; }
        public List<ColumnMemo> Columns { get; set; }
        public List<ParametersMemo> Parameters { get; set; }
    }

    private class StatementMemo : ICommandMemo
    {
        public CommandType CommandType => CommandType.Text;
        public String CommandText { get; set; }
        public String MethodName { get; set; }
        public String RowClassName { get; set; }
        public String RowClassRef { get; set; }
        public String DatabaseName { get; set; }

        public List<ColumnMemo> Columns { get; set; }
        public List<ParametersMemo> Parameters { get; set; }
    }

    private class ColumnMemo
    {
        public String OrdinalVarName { get; set; }
        public String ColumnName { get; set; }
        public Boolean ColumnIsNullable { get; set; }
        public Type PropertyType { get; set; }
        public String PropertyTypeName { get; set; }
        public String PropertyName { get; set; }
        public String FieldTypeName { get; set; }
    }

    private class ParametersMemo
    {
        public SqlDbType ParameterType { get; set; }
        public String ParameterName { get; set; }
        public String ArgumentType { get; set; }
        public String ArgumentName { get; set; }
    }
}
