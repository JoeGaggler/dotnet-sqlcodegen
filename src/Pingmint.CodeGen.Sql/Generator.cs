using Pingmint.CodeGen.Sql.Model;
using System.Data;

namespace Pingmint.CodeGen.Sql;

file static class Ext
{
    public static T NotNull<T>(this T? obj) where T : class => obj ?? throw new NullReferenceException();
}

public static class Generator
{
    private static SortedDictionary<String, DatabaseMemo> PopulateDatabaseScopeMemos(List<DatabasesItem> databases)
    {
        var databaseMemos = new SortedDictionary<String, DatabaseMemo>();
        foreach (var database in databases)
        {
            var sqlDatabaseName = database.Name.NotNull();

            var databaseMemo = databaseMemos[database.Name] = new DatabaseMemo()
            {
                SqlName = sqlDatabaseName,
                ClassName = database.ClassName ?? GetPascalCase(sqlDatabaseName),
            };

            PopulateTableTypes(database, databaseMemo);
            PopulateStatements(database, databaseMemo);
            PopulateProcedures(database, databaseMemo);
        }
        return databaseMemos;
    }

    private static void PopulateRecordProperties(RecordMemo recordMemo, List<Column> columns)
    {
        var props = recordMemo.Properties;
        foreach (var column in columns)
        {
            var prop = new PropertyMemo
            {
                IsNullable = column.IsNullable,
                Name = GetPascalCase(column.Name),
                Type = GetDotnetType(column.Type),
            };
            props.Add(prop);
        }
    }

    private static void CodeRecords(CodeWriter code, SortedDictionary<String, DatabaseMemo> databaseScopeMemos)
    {
        foreach (var dbItem in databaseScopeMemos)
        {
            var db = dbItem.Value;

            foreach (var schemaItem in db.Schemas)
            {
                var schema = schemaItem.Value;

                foreach (var record in schema.Records.Values)
                {
                    CodeRecord(code, record);
                }

                foreach (var tableType in schema.TableTypes.Values)
                {
                    CodeRecord(code, tableType);
                }
            }
            foreach (var recordItem in db.Records)
            {
                var record = recordItem.Value;
                CodeRecord(code, record);
            }
        }
    }

    private static void CodeRecord(CodeWriter code, RecordMemo recordMemo)
    {
        using (code.PartialClass("public", recordMemo.Name))
        {
            foreach (var prop in recordMemo.Properties)
            {
                var typeString = GetStringForType(prop.Type, prop.IsNullable);
                code.Line($"public {typeString} {prop.Name} {{ get; set; }}");
            }
        }
    }

    private static void CodeRecord(CodeWriter code, TableTypeMemo memo)
    {
        var dataTableClassName = memo.DataTableClassName ?? throw new NullReferenceException();
        var rowClassName = memo.RowClassName ?? throw new NullReferenceException();

        using (code.PartialClass("public sealed", dataTableClassName, "DataTable"))
        {
            code.Line("public {0}() : this(new List<{1}>()) {{ }}", dataTableClassName, rowClassName);
            code.Line("public {0}(List<{1}> rows) : base()", dataTableClassName, rowClassName);
            using (code.CreateBraceScope())
            {
                code.Line("ArgumentNullException.ThrowIfNull(rows);");
                code.Line();
                foreach (var col in memo.Columns)
                {
                    var allowDbNull = col.ColumnIsNullable ? "true" : "false";
                    var maxLength = (col.MaxLength is short s) ? $", MaxLength = {s}" : String.Empty; // the default is already "-1", so we do not have to emit this in code.
                    code.Line("base.Columns.Add(new DataColumn() {{ ColumnName = \"{0}\", DataType = typeof({1}), AllowDBNull = {2}{3} }});", col.ColumnName, col.PropertyTypeName, allowDbNull, maxLength);
                }
                using (code.ForEach("var row in rows"))
                {
                    var parameterBuilder = String.Empty;
                    foreach (var col in memo.Columns)
                    {
                        var localName = GetCamelCase(col.PropertyName);
                        var propName = col.PropertyName;

                        if (col.PropertyType == typeof(String) && col.MaxLength is { } maxLength && maxLength > 1)
                        {
                            code.Line("var {0} = String.IsNullOrEmpty(row.{1}) || row.{1}.Length <= {2} ? row.{1} : row.{1}.Remove({2});", localName, propName, maxLength.ToString());
                        }
                        else
                        {
                            code.Line("var {0} = row.{1};", localName, propName);
                        }
                        parameterBuilder += (parameterBuilder == String.Empty) ? localName : ", " + localName;
                    }
                    code.Line("base.Rows.Add({0});", parameterBuilder);
                }
            }
        }
    }

    public static async Task GenerateAsync(Config config, TextWriter textWriter)
    {
        var cs = config.CSharp ?? throw new NullReferenceException();
        var className = cs.ClassName ?? throw new NullReferenceException();
        var fileNs = cs.Namespace ?? throw new NullReferenceException();
        var dbs = config.Databases?.Items ?? throw new NullReferenceException();

        var databaseMemos = PopulateDatabaseScopeMemos(dbs);

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

            foreach (var databaseMemo in databaseMemos.Values)
            {
                foreach (var schemaMemo in databaseMemo.Schemas.Values)
                {
                    foreach (var procMemo in schemaMemo.Procedures.Values)
                    {
                        CodeSqlStatement(code, procMemo);
                        code.Line();
                    }
                }

                foreach (var memo in databaseMemo.Statements)
                {
                    CodeSqlStatement(code, memo.Value);
                    code.Line();
                }
            }
        }

        CodeRecords(code, databaseMemos);

        textWriter.Write(code.ToString());
        await textWriter.FlushAsync();
    }

    private static void PopulateProcedures(DatabasesItem database, DatabaseMemo databaseMemo)
    {
        foreach (var proc in database.Procedures?.Items ?? new())
        {
            var schemaName = proc.Schema;
            if (!databaseMemo.Schemas.TryGetValue(schemaName, out var schemaMemo)) { schemaMemo = databaseMemo.Schemas[schemaName] = new SchemaMemo() { SqlName = schemaName, ClassName = GetPascalCase(schemaName) }; }

            var name = proc.Name ?? throw new NullReferenceException();
            var text = proc.Text ?? throw new NullReferenceException();
            var columns = proc.ResultSet.Columns ?? throw new NullReferenceException();

            var rowClassName = GetPascalCase(name + "_Row");
            var recordMemo = schemaMemo.Records[rowClassName] = new RecordMemo()
            {
                Name = rowClassName,
            };
            PopulateRecordProperties(recordMemo, proc.ResultSet.Columns);

            var memo = new CommandMemo()
            {
                CommandType = CommandType.StoredProcedure,
                CommandText = text,
                MethodName = GetPascalCase(name),
                Parameters = GetCommandParameters(proc.Parameters?.Items ?? new List<Parameter>(), databaseMemo),
                Columns = GetCommandColumns(columns),
                RowClassName = rowClassName,
                // RowClassRef = $"{databaseMemo.ClassName}.{schemaMemo.ClassName}.{rowClassName}",
                RowClassRef = rowClassName,
            };

            schemaMemo.Procedures[name] = memo;
        }
    }

    private static void PopulateTableTypes(DatabasesItem database, DatabaseMemo databaseMemo)
    {
        foreach (var tableType in database.TableTypes?.Items ?? new())
        {
            var schemaName = tableType.SchemaName;
            if (!databaseMemo.Schemas.TryGetValue(schemaName, out var schemaMemo)) { schemaMemo = databaseMemo.Schemas[schemaName] = new SchemaMemo() { SqlName = schemaName, ClassName = GetPascalCase(schemaName) }; }
            var tableTypeName = tableType.TypeName;

            var memo = new TableTypeMemo()
            {
                TypeName = tableType.TypeName,
                SchemaName = tableType.SchemaName,
                Columns = GetCommandColumns(tableType.Columns),
            };
            memo.RowClassName = GetPascalCase(tableType.TypeName) + "Row";
            memo.DataTableClassName = GetPascalCase(tableType.TypeName) + "RowDataTable";
            // memo.RowClassRef = $"{databaseMemo.ClassName}.{schemaMemo.ClassName}.{memo.RowClassName}";
            memo.RowClassRef = memo.RowClassName;
            // memo.DataTableClassRef = $"{databaseMemo.ClassName}.{schemaMemo.ClassName}.{memo.DataTableClassName}";
            memo.DataTableClassRef = memo.DataTableClassName;

            schemaMemo.TableTypes[tableTypeName] = memo;

            var recordMemo = new RecordMemo
            {
                Name = memo.RowClassName,
            };
            PopulateRecordProperties(recordMemo, tableType.Columns);
            schemaMemo.Records[memo.RowClassName] = recordMemo;
        }
    }

    private static void PopulateStatements(DatabasesItem database, DatabaseMemo databaseMemo)
    {
        foreach (var statement in database.Statements?.Items ?? new())
        {
            var name = statement.Name ?? throw new NullReferenceException();
            var resultSet = statement.ResultSet ?? throw new NullReferenceException();
            var commandText = statement.Text ?? throw new NullReferenceException();
            var columns = statement.ResultSet.Columns ?? throw new NullReferenceException();

            var rowClassName = GetPascalCase(statement.Name + "_Row");
            var recordMemo = databaseMemo.Records[rowClassName] = new RecordMemo()
            {
                Name = rowClassName,
            };
            PopulateRecordProperties(recordMemo, statement.ResultSet.Columns);

            var memo = new CommandMemo()
            {
                CommandType = CommandType.Text,
                CommandText = commandText.ReplaceLineEndings(" ").Trim(),
                MethodName = $"{name}",
                RowClassName = rowClassName,
                // RowClassRef = $"{databaseMemo.ClassName}.{rowClassName}",
                RowClassRef = rowClassName,
                Parameters = GetCommandParameters(statement.Parameters?.Items ?? new List<Parameter>(), databaseMemo),
                Columns = GetCommandColumns(columns),
            };
            databaseMemo.Statements[name] = memo;
        }
    }

    private static List<ParametersMemo> GetCommandParameters(List<Parameter> parameters, DatabaseMemo databaseMemo)
    {
        var memos = new List<ParametersMemo>();
        foreach (var i in parameters)
        {
            var memo = new ParametersMemo()
            {
                ParameterName = i.Name ?? throw new NullReferenceException(),
                ParameterType = i.SqlDbType,
                ArgumentName = GetCamelCase(i.Name),
            };

            if (i.SqlDbType == SqlDbType.Structured)
            {
                foreach (var schema in databaseMemo.Schemas.Values)
                {
                    if (schema.TableTypes.Values.FirstOrDefault(j => j.TypeName == i.Type) is not { } tableType)
                    {
                        throw new InvalidOperationException($"Unable to find table type: {i.Type}");
                    }
                    memo.ArgumentType = $"List<{tableType.RowClassRef}>";
                    memo.ArgumentExpression = $"new {tableType.DataTableClassRef}({GetCamelCase(i.Name)})";
                    memo.ParameterTableRef = $"{tableType.SchemaName}.{tableType.TypeName}";
                }
            }
            else
            {
                memo.ArgumentType = GetShortestNameForType(GetDotnetType(i.SqlDbType));
                memo.ArgumentExpression = GetCamelCase(i.Name);
            }

            memos.Add(memo);
        }

        return memos;
    }

    private static List<ColumnMemo> GetCommandColumns(List<Column> columns) =>
        columns.Select(i => new ColumnMemo()
        {
            MaxLength = i.MaxLength,
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

    private static void CodeSqlStatement(CodeWriter code, CommandMemo commandMemo)
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

            var commandMethod = commandMemo.CommandType switch
            {
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
                    var withTableTypeName = (parameter.ParameterTableRef is String tableTypeName) ? $", \"{tableTypeName}\"" : "";
                    code.Line($"cmd.Parameters.Add(CreateParameter(\"@{parameter.ParameterName.TrimStart('@')}\", {parameter.ArgumentExpression}, SqlDbType.{parameter.ParameterType}{withTableTypeName}));");
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
        public String SqlName { get; set; }
        public String ClassName { get; set; }

        public SortedDictionary<String, RecordMemo> Records { get; } = new();
        public SortedDictionary<String, SchemaMemo> Schemas { get; } = new();
        public SortedDictionary<String, CommandMemo> Statements { get; } = new();
    }

    private class SchemaMemo
    {
        public String SqlName { get; set; }
        public String ClassName { get; set; }

        public SortedDictionary<String, CommandMemo> Procedures { get; } = new();
        public SortedDictionary<String, RecordMemo> Records { get; } = new();
        public SortedDictionary<String, TableTypeMemo> TableTypes { get; } = new();
    }

    private class RecordMemo
    {
        public String Name { get; set; }

        public List<PropertyMemo> Properties { get; } = new();
    }

    private class PropertyMemo
    {
        public Boolean IsNullable { get; set; }
        public Type Type { get; set; }
        public String Name { get; set; }
    }

    private class TableTypeMemo
    {
        public String TypeName { get; set; }
        public String SchemaName { get; set; }
        public String RowClassName { get; set; }
        public String RowClassRef { get; set; }
        public String DataTableClassName { get; set; }
        public String DataTableClassRef { get; set; }
        public List<ColumnMemo> Columns { get; set; }
    }

    private class CommandMemo
    {
        public CommandType CommandType { get; set; }
        public String CommandText { get; set; }
        public String MethodName { get; set; }
        public String RowClassName { get; set; }
        public String RowClassRef { get; set; }
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
        public short? MaxLength { get; set; }
    }

    private class ParametersMemo
    {
        public String ParameterName { get; set; }
        public SqlDbType ParameterType { get; set; }
        public String ParameterTableRef { get; set; }
        public String ArgumentType { get; set; }
        public String ArgumentName { get; set; }
        public String ArgumentExpression { get; set; }
    }
}
