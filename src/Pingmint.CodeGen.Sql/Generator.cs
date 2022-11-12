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
                    if (record.ParentTableType is { } tableType && !tableType.IsReferenced) { continue; }
                    CodeRecord(code, record);
                }

                foreach (var tableType in schema.TableTypes.Values)
                {
                    if (!tableType.IsReferenced) { continue; }
                    CodeRecord(code, tableType);
                }
            }
            foreach (var recordItem in db.Records)
            {
                var record = recordItem.Value;
                if (record.ParentTableType is { } tableType && !tableType.IsReferenced) { continue; }
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

    public static void Generate(Config config, CodeWriter code)
    {
        var cs = config.CSharp ?? throw new NullReferenceException();
        var className = cs.ClassName ?? throw new NullReferenceException();
        var fileNs = cs.Namespace ?? throw new NullReferenceException();
        var dbs = config.Databases?.Items ?? throw new NullReferenceException();

        var databaseMemos = PopulateDatabaseScopeMemos(dbs);

        code.UsingNamespace("System");
        code.UsingNamespace("System.Collections.Generic");
        code.UsingNamespace("System.Data");
        code.UsingNamespace("System.Threading");
        code.UsingNamespace("System.Threading.Tasks");
        code.UsingNamespace("Microsoft.Data.SqlClient");
        code.Line();
        code.FileNamespace(fileNs);
        code.Line();

        code.Line("public partial interface I{0}", className);
        using (code.CreateBraceScope())
        {
            foreach (var databaseMemo in databaseMemos.Values)
            {
                foreach (var schemaMemo in databaseMemo.Schemas.Values)
                {
                    foreach (var procMemo in schemaMemo.Procedures.Values)
                    {
                        CodeSqlStatementInterface(code, procMemo);
                    }
                }

                foreach (var memo in databaseMemo.Statements)
                {
                    CodeSqlStatementInterface(code, memo.Value);
                }
            }
        }
        code.Line();

        CodeRecords(code, databaseMemos);
        code.Line();

        using (code.PartialClass("public", className, "I" + className))
        {
            WriteConstructor(code, className);
            code.Line();

            foreach (var databaseMemo in databaseMemos.Values)
            {
                foreach (var schemaMemo in databaseMemo.Schemas.Values)
                {
                    foreach (var procMemo in schemaMemo.Procedures.Values)
                    {
                        CodeSqlStatementInstance(code, procMemo);
                    }
                }

                foreach (var memo in databaseMemo.Statements)
                {
                    CodeSqlStatementInstance(code, memo.Value);
                }
            }
            code.Line();

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
    }

    private static void PopulateProcedures(DatabasesItem database, DatabaseMemo databaseMemo)
    {
        foreach (var proc in database.Procedures?.Items ?? new())
        {
            var schemaName = proc.Schema;
            if (!databaseMemo.Schemas.TryGetValue(schemaName, out var schemaMemo)) { schemaMemo = databaseMemo.Schemas[schemaName] = new SchemaMemo() { SqlName = schemaName, ClassName = GetPascalCase(schemaName) }; }

            var name = proc.Name ?? throw new NullReferenceException();
            var columns = proc.ResultSet?.Columns ?? throw new NullReferenceException();

            Boolean isNonQuery;
            String? rowClassName;
            if (columns.Count != 0)
            {
                isNonQuery = false;
                rowClassName = GetPascalCase(name + "_Row");
                var recordMemo = schemaMemo.Records[rowClassName] = new RecordMemo()
                {
                    Name = rowClassName,
                };
                PopulateRecordProperties(recordMemo, proc.ResultSet.Columns);
            }
            else
            {
                isNonQuery = true;
                rowClassName = null;
            }

            var memo = new CommandMemo()
            {
                CommandType = CommandType.StoredProcedure,
                CommandText = $"{database.Name}.{schemaName}.{name}",
                MethodName = GetPascalCase(name),
                Parameters = GetCommandParameters(schemaName, proc.Parameters?.Items ?? new List<Parameter>(), databaseMemo),
                Columns = GetCommandColumns(columns),
                RowClassName = rowClassName,
                // RowClassRef = $"{databaseMemo.ClassName}.{schemaMemo.ClassName}.{rowClassName}",
                RowClassRef = rowClassName,
                IsNonQuery = isNonQuery,
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
                Name = memo.RowClassName + $" // {schemaName}.{tableTypeName}",
                ParentTableType = memo,
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

            Boolean isNonQuery;
            String? rowClassName;
            if (columns.Count != 0)
            {
                isNonQuery = false;
                rowClassName = GetPascalCase(name + "_Row");
                var recordMemo = databaseMemo.Records[rowClassName] = new RecordMemo()
                {
                    Name = rowClassName,
                };
                PopulateRecordProperties(recordMemo, statement.ResultSet.Columns);
            }
            else
            {
                isNonQuery = true;
                rowClassName = null;
            }

            var memo = new CommandMemo()
            {
                CommandType = CommandType.Text,
                CommandText = commandText.ReplaceLineEndings(" ").Trim(),
                MethodName = $"{name}",
                RowClassName = rowClassName,
                // RowClassRef = $"{databaseMemo.ClassName}.{rowClassName}",
                RowClassRef = rowClassName,
                Parameters = GetCommandParameters(null, statement.Parameters?.Items ?? new List<Parameter>(), databaseMemo),
                Columns = GetCommandColumns(columns),
                IsNonQuery = isNonQuery,
            };
            databaseMemo.Statements[name] = memo;
        }
    }

    private static List<ParametersMemo> GetCommandParameters(String? hostSchema, List<Parameter> parameters, DatabaseMemo databaseMemo)
    {
        var memos = new List<ParametersMemo>();
        foreach (var i in parameters)
        {
            var memo = new ParametersMemo()
            {
                ParameterName = i.Name ?? throw new NullReferenceException(),
                ParameterType = i.SqlDbType,
                ArgumentName = GetCamelCase(i.Name),
                MaxLength = i.MaxLength,
            };

            if (i.SqlDbType == SqlDbType.Structured)
            {
                var (schemaName, typeName) = Program.ParseSchemaItem(i.Type);
                schemaName ??= hostSchema;
                foreach (var schema in databaseMemo.Schemas.Values)
                {
                    if (schema.SqlName != schemaName) { continue; }
                    if (schema.TableTypes.Values.FirstOrDefault(j => j.TypeName == i.Type) is not { } tableType)
                    {
                        throw new InvalidOperationException($"Unable to find table type: {i.Type} {schema.SqlName}");
                    }
                    tableType.IsReferenced = true;
                    memo.ArgumentType = $"List<{tableType.RowClassRef}>";
                    memo.ArgumentExpression = $"new {tableType.DataTableClassRef}({GetCamelCase(i.Name)})";
                    memo.ParameterTableRef = $"{tableType.SchemaName}.{tableType.TypeName}";
                    break;
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

    private static void WriteConstructor(CodeWriter code, String className)
    {
        code.Line("private readonly Func<Task<SqlConnection>> connectionFunc;");
        code.Line();
        code.Line("public {0}({1})", className, "Func<Task<SqlConnection>> connectionFunc");
        using (code.CreateBraceScope())
        {
            code.Line("this.connectionFunc = connectionFunc;");
        }
    }

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

    private struct StatementSignature
    {
        public String MethodName;
        public String ReturnType;
        public String ResultType;
        public String ParametersRaw;
        public String Parameters;
        public String ArgumentsRaw;
        public String Arguments;
        public String? RowClassRef;
    }

    private static StatementSignature GetStatementSignature(CommandMemo commandMemo)
    {
        String resultType;
        if (!commandMemo.IsNonQuery && commandMemo.RowClassRef is { } rowClassRef)
        {
            resultType = String.Format("List<{0}>", rowClassRef);
        }
        else
        {
            resultType = "Int32";
            rowClassRef = null;
        }
        var returnType = String.Format("Task<{0}>", resultType);
        var methodName = (commandMemo.MethodName ?? throw new NullReferenceException()) + "Async";

        var prms1 = "";
        var args1 = "";
        var prms = "connection";
        var args = "SqlConnection connection"; // TODO: parameters, then transaction
        if (commandMemo.Parameters is { } parameters && parameters.Count > 0)
        {
            prms1 = String.Join(", ", parameters?.Select(i => $"{i.ArgumentName}"));
            args1 = String.Join(", ", parameters?.Select(i => $"{i.ArgumentType} {i.ArgumentName}"));
            prms += ", " + prms1;
            args += ", " + args1;
        }

        return new()
        {
            MethodName = methodName,
            ReturnType = returnType,
            ResultType = resultType,
            Parameters = prms,
            ParametersRaw = prms1,
            Arguments = args,
            ArgumentsRaw = args1,
            RowClassRef = rowClassRef,
        };
    }

    private static void CodeSqlStatementInterface(CodeWriter code, CommandMemo commandMemo)
    {
        var sig = GetStatementSignature(commandMemo);
        code.Line("{0} {1}({2});", sig.ReturnType, sig.MethodName, sig.ArgumentsRaw);
    }

    private static void CodeSqlStatementInstance(CodeWriter code, CommandMemo commandMemo)
    {
        var sig = GetStatementSignature(commandMemo);
        code.Line("public async {0} {1}({2}) => await {1}(await connectionFunc(){3});", sig.ReturnType, sig.MethodName, sig.ArgumentsRaw, sig.ParametersRaw is {} raw && raw.Length > 0 ? ", " + raw : "");
    }

    private static void CodeSqlStatement(CodeWriter code, CommandMemo commandMemo)
    {
        var commandText = commandMemo.CommandText ?? throw new NullReferenceException();

        var sig = GetStatementSignature(commandMemo);
        var methodName = sig.MethodName;
        var returnType = sig.ReturnType;
        var resultType = sig.ResultType;
        var args = sig.Arguments;
        var prms = sig.Parameters;
        var rowClassRef = sig.RowClassRef;

        // args += ", CancellationToken cancellationToken";

        code.Line("public static {0} {1}({2}) => {1}({3}, CancellationToken.None);", returnType, methodName, args, prms);
        using (code.Method("public static async", returnType, methodName, args + ", CancellationToken cancellationToken"))
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
                    var withSize = (parameter.MaxLength is { } maxLength and not -1) ? $", {maxLength}" : "";
                    code.Line($"cmd.Parameters.Add(CreateParameter(\"@{parameter.ParameterName.TrimStart('@')}\", {parameter.ArgumentExpression}, SqlDbType.{parameter.ParameterType}{withTableTypeName}{withSize}));");
                }
                code.Line();
            }

            if (commandMemo.IsNonQuery)
            {
                code.Return("await cmd.ExecuteNonQueryAsync(cancellationToken)");
            }
            else if (commandMemo.Columns is { } columns && columns.Count > 0 && rowClassRef is { } rowClassRef2)
            {
                code.Line("var result = new {0}();", resultType);
                code.Line("using var reader = await cmd.ExecuteReaderAsync(cancellationToken);");
                using (code.If("await reader.ReadAsync(cancellationToken)"))
                {
                    foreach (var column in commandMemo.Columns)
                    {
                        code.Line("int {0} = reader.GetOrdinal(\"{1}\");", column.OrdinalVarName, column.ColumnName);
                    }
                    code.Line();
                    using (code.DoWhile("await reader.ReadAsync(cancellationToken)"))
                    {
                        code.Line("result.Add(new {0}", rowClassRef2);
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
            else
            {
                throw new InvalidOperationException("unable to determine expected results");
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

        SqlDbType.DateTime or
        SqlDbType.DateTime2
        => typeof(DateTime),

        SqlDbType.Bit => typeof(Boolean),
        SqlDbType.Int => typeof(Int32),
        SqlDbType.TinyInt => typeof(Byte),
        SqlDbType.SmallInt => typeof(Int16),
        SqlDbType.BigInt => typeof(Int64),

        SqlDbType.UniqueIdentifier => typeof(Guid),

        SqlDbType.VarBinary => typeof(Byte[]),

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
        var x when x == typeof(Int64) => "Int64",
        var x when x == typeof(String) => "String",
        var x when x == typeof(Boolean) => "Boolean",
        var x when x == typeof(Byte) => "Byte",
        var x when x == typeof(Guid) => "Guid",
        var x when x == typeof(Byte[]) => "Byte[]",
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
                if (!Char.IsLetter(ch))
                {
                    if (sb.Length == 0)
                    {
                        continue;
                    }
                    else
                    {
                        sb.Append(Char.ToUpperInvariant(ch));
                        continue;
                    }
                }

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
        public TableTypeMemo? ParentTableType { get; set; } = null;
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
        public Boolean IsReferenced { get; set; } = false;
    }

    private class CommandMemo
    {
        public CommandType CommandType { get; set; }
        public String CommandText { get; set; }
        public String MethodName { get; set; }
        public String? RowClassName { get; set; }
        public String? RowClassRef { get; set; }
        public Boolean IsNonQuery { get; set; }
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
        public Int32? MaxLength { get; set; }
    }
}
