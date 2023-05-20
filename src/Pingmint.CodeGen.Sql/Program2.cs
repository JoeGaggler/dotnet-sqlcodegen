using Pingmint.CodeGen.Sql.Model;
using Pingmint.CodeGen.Sql.Model.Yaml;
//using Pingmint.CodeGen.Sql.Refactor;
using Microsoft.Data.SqlClient;
using System.Data;

using static System.Console;
using static Pingmint.CodeGen.Sql.Globals;

namespace Pingmint.CodeGen.Sql.Refactor;

internal sealed class Program2
{
    public static async Task Run(Config config, string[] args)
    {
        const int chunkSize = 20;

        // TODO: Add CancellationToken to all async methods
        // TODO: Add optional Transaction to all methods

        var sync = new ConsoleSynchronizationContext();
        sync.Go(async () =>
        {
            var codeFile = new CodeFile();
            codeFile.Namespace = config.CSharp.Namespace;
            codeFile.ClassName = config.CSharp.ClassName;

            if (config.Databases?.Items is { } databases)
            {
                foreach (var database in databases)
                {
                    var databaseName = database.SqlName ?? throw new InvalidOperationException("Database name is required.");
                    var analyzer = new Analyzer(databaseName, codeFile, config);

                    var tasks = new Task<int>[chunkSize];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = Task.FromResult(i);
                    }

                    if (database.Statements?.Items is { } statements)
                    {
                        foreach (var statement in statements)
                        {
                            var index = await await Task.WhenAny(tasks);
                            var parameters = statement.Parameters?.Items.Select(p => new SqlStatementParameter(p.Name, p.Type)).ToList() ?? new();
                            tasks[index] = analyzer.AnalyzeStatementAsync(databaseName, statement.Name, statement.Text, parameters).ContinueWith(_ => index);
                        }
                    }

                    if (database.Procedures?.Included is { } included)
                    {
                        List<(String Schema, String Name)> excludeSchemaProcList = new();
                        if (database.Procedures?.Excluded is { } excludeProcs)
                        {
                            foreach (var item in excludeProcs)
                            {
                                var (schema, procName) = ParseSchemaItem(item.Text);
                                if (schema is not null && procName is not null)
                                {
                                    excludeSchemaProcList.Add((schema, procName));
                                }
                            }
                        }
                        Boolean IsExcluded(String procSchema, String procName)
                        {
                            foreach (var (exSchema, exName) in excludeSchemaProcList)
                            {
                                if (procSchema != exSchema) continue;
                                if (procName == exName) return true;
                                if (exName == "*") return true;
                            }
                            return false;
                        }

                        var actualIncluded = new List<(String, String, Int32)>();
                        foreach (var include in included)
                        {
                            if (String.IsNullOrEmpty(include.Text)) { continue; }
                            using var sql = new SqlConnection(config.Connection.ConnectionString);
                            await sql.OpenAsync();
                            await sql.ChangeDatabaseAsync(databaseName);
                            var (schema, procName) = ParseSchemaItem(include.Text);
                            if (procName == "*")
                            {
                                foreach (var row in await Proxy.GetProceduresForSchemaAsync(sql, schema))
                                {
                                    if (IsExcluded(schema, row.Name)) { continue; }
                                    var newProc = new Procedure();
                                    actualIncluded.Add((schema, row.Name, row.ObjectId));
                                }
                            }
                            else
                            {
                                if (IsExcluded(schema, procName)) { continue; }
                                WriteLine("Proxy.GetProcedureForSchemaAsync");
                                if ((await Proxy.GetProcedureForSchemaAsync(sql, schema, procName)).FirstOrDefault() is not { } row) { continue; }
                                actualIncluded.Add((schema, procName, row.ObjectId));
                            }
                        }

                        for (int i = 0; i < tasks.Length; i++)
                        {
                            tasks[i] = Task.FromResult(i);
                        }
                        foreach (var (schema, procName, objectId) in actualIncluded)
                        {
                            var index = await await Task.WhenAny(tasks);
                            tasks[index] = analyzer.AnalyzeProcedureAsync(databaseName, schema, procName, objectId).ContinueWith(_ => index);
                        }
                    }

                    await Task.WhenAll(tasks);
                }
            }

            using TextWriter textWriter = args.Length switch
            {
                > 1 => new StreamWriter(args[1]),
                _ => Console.Out
            };
            textWriter.Write(codeFile.GenerateCode());
            await textWriter.FlushAsync();
            textWriter.Close();
        });
    }
}

public record class SqlStatementParameter(String Name, String Type);

public class Analyzer
{
    private CodeFile codeFile;
    private Config config;
    private String database;
    private String connectionString;

    public Analyzer(String database, CodeFile codeFile, Config config)
    {
        this.database = database;
        this.codeFile = codeFile;
        this.config = config;

        this.connectionString = config.Connection.ConnectionString;
    }

    // TODO: remove stats
    private static int connectionCount = 0;
    private static int totalOpened = 0;
    private async Task<SqlConnection> OpenSqlConnectionAsync([System.Runtime.CompilerServices.CallerMemberName] String? caller = null)
    {
        var instance = ++connectionCount;

        var callerString = caller == null ? "" : $" ({caller})";

        // OpenAsync tries to consume some time on the current sync-context
        var sql = await Task.Run(async () =>
        {
            var sql = new SqlConnection(connectionString);
            var openTask = sql.OpenAsync();
            if (openTask.IsCompleted)
            {
                WriteLine($"Dequeued connection {instance}/{totalOpened}{callerString}");
            }
            else
            {
                var total = Interlocked.Increment(ref totalOpened);
                WriteLine($"Opening connection {instance}/{total}{callerString}");
                await openTask.ConfigureAwait(false);
                WriteLine($"Opened connection {instance}/{total}{callerString}");
            }
            await sql.ChangeDatabaseAsync(database).ConfigureAwait(false);
            return sql;
        }).ConfigureAwait(false);

        return sql;
    }

    public async Task AnalyzeProcedureAsync(string database, string schema, string proc, int procId)
    {
        var t0 = DateTime.UtcNow;
        WriteLine("Analyze Procedure: {0}.{1}.{2}", database, schema, (object)proc);

        // To match statements
        var commandText = database + "." + schema + "." + proc;

        using var server = await OpenSqlConnectionAsync();

        var methodParameters = new List<MethodParameter>();
        var commandParameters = new List<CommandParameter>();
        WriteLine("Proxy.GetParametersForObjectAsync");
        foreach (var procParam in await Proxy.GetParametersForObjectAsync(server, procId))
        {
            var (methodParameter, commandParameter) = await AnalyzeParameterAsync(server, procParam.Name, procParam.SystemTypeId, procParam.UserTypeId);
            methodParameters.Add(methodParameter);
            commandParameters.Add(commandParameter);
        }

        String recordName = GetUniqueName(GetPascalCase(proc + "Row"), codeFile.TypeNames);
        var record = new Record
        {
            CSharpName = recordName,
        };

        var isRecordSet = true; // TODO: other return types
        var returnType = isRecordSet ? $"List<{recordName}>" : throw new NotImplementedException("TODO: return type for statements that are not recordsets");
        var methodSync = new Method
        {
            Name = GetPascalCase(proc),
            IsStoredProc = true,
            DataType = returnType,
            CommandText = commandText,
            Record = record,
            CSharpParameters = methodParameters,
            SqlParameters = commandParameters,
        };

        var recordColumns = new List<RecordProperty>();

        WriteLine("Proxy.DmDescribeFirstResultSetForObjectAsync");
        var columnsRows = await Proxy.DmDescribeFirstResultSetForObjectAsync(server, procId);
        foreach (var (columnRow, columnIndex) in columnsRows.WithIndex())
        {
            if (await AnalyzeResultAsync(server, columnRow, columnIndex) is { } recordProperty)
            {
                record.Properties.Add(recordProperty);
            }
        }

        codeFile.Records.Add(record);
        codeFile.Methods.Add(methodSync);

        WriteLine("Analyze Done: {0}.{1}.{2} ({3:0.0}s)", database, schema, proc, (DateTime.UtcNow - t0).TotalSeconds);
    }

    public async Task AnalyzeStatementAsync(String database, String name, String commandText, List<SqlStatementParameter> statementParameters)
    {
        WriteLine("Analyze Statement: {0}", name);

        using var server = await OpenSqlConnectionAsync();

        var methodParameters = new List<MethodParameter>();
        var commandParameters = new List<CommandParameter>();
        foreach (var procParam in statementParameters)
        {
            var sysType = await GetSysTypeByNameAsync(server, procParam.Type);
            var (methodParameter, commandParameter) = await AnalyzeParameterAsync(server, procParam.Name, sysType.SystemTypeId, sysType.UserTypeId);
            methodParameters.Add(methodParameter);
            commandParameters.Add(commandParameter);
        }

        var recordName = GetUniqueName(GetPascalCase(name + "Row"), codeFile.TypeNames);
        var record = new Record
        {
            CSharpName = recordName,
        };

        var isRecordSet = true; // TODO: other return types
        var returnType = isRecordSet ? $"List<{recordName}>" : throw new NotImplementedException("TODO: return type for statements that are not recordsets");
        var methodSync = new Method
        {
            Name = GetPascalCase(name),
            IsStoredProc = false,
            DataType = returnType,
            CommandText = commandText,
            Record = record,
            CSharpParameters = methodParameters,
            SqlParameters = commandParameters,
        };

        var recordColumns = new List<RecordProperty>();

        var parametersText = String.Join(", ", statementParameters.Select(p => $"@{p.Name} {p.Type}"));
        WriteLine("Proxy.DmDescribeFirstResultSetAsync");
        var columsRows = await Proxy.DmDescribeFirstResultSetAsync(server, commandText, parametersText);
        foreach (var (columnRow, columnIndex) in columsRows.WithIndex())
        {
            if (await AnalyzeResultAsync(server, columnRow, columnIndex) is { } recordProperty)
            {
                record.Properties.Add(recordProperty);
            }
        }

        codeFile.Records.Add(record);
        codeFile.Methods.Add(methodSync);

        WriteLine("Analyze Done: {0}", name);
    }

    /***************************************************************************/

    private async Task<(MethodParameter, CommandParameter)> AnalyzeParameterAsync(SqlConnection server, String Name, int SystemTypeId, int UserTypeId)
    {
        var csharpTypeInfo = await GetCSharpTypeInfoAsync(server, SystemTypeId, UserTypeId);
        var csharpIdentifier = GetCamelCase(Name);

        var methodParameter = new MethodParameter
        {
            CSharpName = csharpIdentifier,
            CSharpType = csharpTypeInfo.TypeRef,
        };

        var commandExpression = csharpTypeInfo.IsTableType ? $"new {csharpTypeInfo.TableTypeRef}({csharpIdentifier})" : csharpIdentifier;
        var commandParameter = new CommandParameter
        {
            SqlName = Name.TrimStart('@'),
            SqlDbType = csharpTypeInfo.SqlDbType,
            CSharpExpression = commandExpression,
        };

        return (methodParameter, commandParameter);
    }

    private async Task<RecordProperty?> AnalyzeResultAsync(SqlConnection server, IDmDescribeFirstResultSetRow columnRow, int columnIndex)
    {
        if (columnRow.Name is not { } columnName)
        {
            WriteLine("WARNING: Column {0} has no name", columnIndex);
            return null;
        }
        var isNullable = columnRow.IsNullable.GetValueOrDefault(true); // nullable by default
        var propertyName = GetPascalCase(columnName);
        var csharpTypeInfo = await GetCSharpTypeInfoAsync(server, columnRow.SystemTypeId, columnRow.UserTypeId);
        var propertyType = isNullable ? csharpTypeInfo.TypeRefNullable : csharpTypeInfo.TypeRef;
        var propertyTypeWithoutNullable = csharpTypeInfo.TypeRef;
        var isValueType = csharpTypeInfo.IsValueType;

        var recordProperty = new RecordProperty
        {
            FieldName = propertyName,
            FieldType = propertyType,
            FieldTypeForGeneric = propertyTypeWithoutNullable,
            FieldTypeIsValueType = isValueType,
            ColumnName = columnName,
            ColumnIsNullable = isNullable,
        };
        return recordProperty;
    }

    /***************************************************************************/

    private List<GetSysTypesRow>? _sysTypes;
    private async Task<List<GetSysTypesRow>> GetSysTypesAsync(SqlConnection server)
    {
        if (_sysTypes is not null) return _sysTypes;
        WriteLine("Proxy.GetSysTypesAsync");
        return _sysTypes ??= await Proxy.GetSysTypesAsync(server);
    }

    private readonly SortedDictionary<String, GetSysTypesRow> _sysTypesByName = new();
    private async Task<GetSysTypesRow> GetSysTypeByNameAsync(SqlConnection server, string typeName)
    {
        if (_sysTypesByName.TryGetValue(typeName, out var sysType)) { return sysType; }

        var xxx = Proxy.DmDescribeFirstResultSet(server, $"DECLARE @x {typeName}; SELECT @x AS x;", "");
        if (xxx.Count != 1) throw new Exception("TODO: handle this");
        var yyy = xxx[0];

        sysType = (await GetSysTypesAsync(server)).FirstOrDefault(t => t.SystemTypeId == yyy.SystemTypeId && t.UserTypeId == yyy.UserTypeId) ?? throw new Exception("TODO: handle this2");
        _sysTypesByName[typeName] = sysType;
        return sysType;
    }

    private List<GetTableTypesRow>? _tableTypes;
    private async Task<List<GetTableTypesRow>> GetTableTypesAsync(SqlConnection server)
    {
        if (_tableTypes is not null) return _tableTypes;
        WriteLine("Proxy.GetTableTypesAsync");
        return _tableTypes ??= await Proxy.GetTableTypesAsync(server);
    }

    private SortedDictionary<Int32, GetTableTypesRow>? _tableTypesByObjectId;
    private async Task<GetTableTypesRow?> GetTableTypeAsync(SqlConnection server, int objectId)
    {
        var types = await GetTableTypesAsync(server);

        if (_tableTypesByObjectId is not { } tableTypesByObjectId)
        {
            _tableTypesByObjectId = tableTypesByObjectId = new(types.ToDictionary(t => t.TypeTableObjectId));
        }

        if (tableTypesByObjectId.TryGetValue(objectId, out var type))
        {
            return type;
        }
        return null;
    }

    // TODO: cache
    private async Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(SqlConnection server, int systemTypeId, int userTypeId)
    {
        var types = await GetTableTypesAsync(server);
        if (types.FirstOrDefault(i => i.SystemTypeId == systemTypeId && i.UserTypeId == userTypeId) is not { } tableType) { throw new InvalidOperationException($"Table type not found: {systemTypeId}, {userTypeId}"); }

        WriteLine("Proxy.GetTableTypeColumnsAsync");
        return await Proxy.GetTableTypeColumnsAsync(server, tableType.TypeTableObjectId);
    }

    private SortedDictionary<(Int32, Int32), GetSysTypesRow>? _sysTypesById;
    private async Task<GetSysTypesRow?> GetSysTypeAsync(SqlConnection server, int systemTypeId, int userTypeId)
    {
        var types = await GetSysTypesAsync(server);

        if (_sysTypesById is not { } sysTypesById)
        {
            _sysTypesById = sysTypesById = new(types.ToDictionary(t => ((int)t.SystemTypeId, t.UserTypeId)));
        }

        if (sysTypesById.TryGetValue((systemTypeId, userTypeId), out var type))
        {
            return type;
        }
        return null;
    }

    private SortedDictionary<(Int32, Int32), SqlDbType>? _sqlDbTypesById2;
    private async Task<SqlDbType?> GetSqlDbTypeAsync(SqlConnection server, int systemTypeId, int userTypeId)
    {
        if (_sqlDbTypesById2 is not { } sqlDbTypesById)
        {
            sqlDbTypesById = new();
            foreach (var nativeTypeRow in (await GetSysTypesAsync(server)).Where(i => i.IsFromSysSchema))
            {
                if (nativeTypeRow.Name switch
                {
                    // ints
                    "bit" => SqlDbType.Bit,
                    "tinyint" => SqlDbType.TinyInt,
                    "smallint" => SqlDbType.SmallInt,
                    "int" => SqlDbType.Int,
                    "bigint" => SqlDbType.BigInt,

                    // chars
                    "char" => SqlDbType.Char,
                    "nchar" => SqlDbType.NChar,
                    "nvarchar" => SqlDbType.NVarChar,
                    "ntext" => SqlDbType.NText,
                    "text" => SqlDbType.Text,

                    // dates
                    "date" => SqlDbType.Date,
                    "datetime" => SqlDbType.DateTime,
                    "datetime2" => SqlDbType.DateTime2,
                    "datetimeoffset" => SqlDbType.DateTimeOffset,
                    "smalldatetime" => SqlDbType.SmallDateTime,

                    // binary
                    "binary" => SqlDbType.Binary,
                    "varbinary" => SqlDbType.VarBinary,

                    // other
                    "decimal" => SqlDbType.Decimal,
                    "float" => SqlDbType.Float,
                    "money" => SqlDbType.Money,
                    "numeric" => SqlDbType.Decimal,
                    "real" => SqlDbType.Real,
                    "time" => SqlDbType.Time,
                    "sysname" => SqlDbType.VarChar,
                    "varchar" => SqlDbType.VarChar,
                    "xml" => SqlDbType.Xml,
                    "uniqueidentifier" => SqlDbType.UniqueIdentifier,
                    "image" => SqlDbType.Image,

                    _ => (SqlDbType?)null,
                } is not { } value)
                {
                    WriteLine($"Unknown native type: {nativeTypeRow.Name}");
                    continue;
                }

                sqlDbTypesById[((int)nativeTypeRow.SystemTypeId, nativeTypeRow.UserTypeId)] = value;
            }

            _sqlDbTypesById2 = sqlDbTypesById;
        }

        if (sqlDbTypesById.TryGetValue((systemTypeId, userTypeId), out var sqlDbType))
        {
            return sqlDbType;
        }

        if (await GetSysTypeAsync(server, systemTypeId, userTypeId) is { } sysType)
        {
            if (sysType.IsTableType) { return SqlDbType.Structured; }
        }

        WriteLine("Unknown type: {0} {1}", systemTypeId, userTypeId);
        return null;
    }

    private static Type? GetCSharpTypeForSqlDbType(SqlDbType type) => type switch
    {
        SqlDbType.Char or
        SqlDbType.NChar or
        SqlDbType.NText or
        SqlDbType.NVarChar or
        SqlDbType.Text or
        SqlDbType.VarChar or
        SqlDbType.Xml
        => typeof(String),

        SqlDbType.DateTimeOffset => typeof(DateTimeOffset),

        SqlDbType.Date or
        SqlDbType.DateTime or
        SqlDbType.DateTime2 or
        SqlDbType.SmallDateTime
        => typeof(DateTime),

        SqlDbType.Time => typeof(TimeSpan),

        SqlDbType.Bit => typeof(Boolean),
        SqlDbType.Int => typeof(Int32),
        SqlDbType.TinyInt => typeof(Byte),
        SqlDbType.SmallInt => typeof(Int16),
        SqlDbType.BigInt => typeof(Int64),

        SqlDbType.Money or
        SqlDbType.SmallMoney or
        SqlDbType.Decimal => typeof(Decimal),

        SqlDbType.Real => typeof(Single),
        SqlDbType.Float => typeof(Double),

        SqlDbType.UniqueIdentifier => typeof(Guid),

        SqlDbType.Binary or
        SqlDbType.Image or
        SqlDbType.Timestamp or
        SqlDbType.VarBinary => typeof(Byte[]),

        _ => null,
    };

    private readonly SortedDictionary<(Int32, Int32), CSharpTypeInfo> _csharpTypeInfosById = new(); // not null because this is generated with _sqlDbTypesById
    private class CSharpTypeInfo
    {
        public String TypeRef { get; init; }
        public String TypeRefNullable { get; init; }
        public Boolean IsValueType { get; init; }
        public Boolean IsTableType { get; init; }
        public String? TableTypeRef { get; init; }
        public SqlDbType SqlDbType { get; init; }
    }
    private async Task<CSharpTypeInfo> GetCSharpTypeInfoAsync(SqlConnection server, int systemTypeId, int userTypeId)
    {
        if (_csharpTypeInfosById.TryGetValue((systemTypeId, userTypeId), out var cachedValue)) { return cachedValue; }

        if (await GetSysTypeAsync(server, systemTypeId, userTypeId) is not { } foundSysType)
        {
            throw new InvalidOperationException("Could not find sys.types row for " + systemTypeId + ", " + userTypeId);
        }

        if (!foundSysType.IsUserDefined)
        {
            if (await GetSqlDbTypeAsync(server, systemTypeId, userTypeId) is { } sqlDbType)
            {
                var type = GetCSharpTypeForSqlDbType(sqlDbType) ?? throw new InvalidOperationException("Have SqlDbType without C# type for it: " + sqlDbType);

                _csharpTypeInfosById[(systemTypeId, userTypeId)] = cachedValue = new()
                {
                    TypeRef = type.Name,
                    TypeRefNullable = type.Name + "?",
                    IsValueType = type.IsValueType,
                    SqlDbType = sqlDbType,
                };

                return cachedValue;
            }
        }

        if (foundSysType.IsTableType)
        {
            string rowCSharpTypeName = GetUniqueName(GetPascalCase(foundSysType.Name + "Row"), codeFile.TypeNames);
            string tableCSharpTypeName = GetUniqueName(GetPascalCase(foundSysType.Name + "Table"), codeFile.TypeNames);

            var record = new Record()
            {
                CSharpName = rowCSharpTypeName,
                IsTableType = true,
                TableTypeCSharpName = tableCSharpTypeName,
            };

            var cols = await GetTableTypeColumnsAsync(server, foundSysType.SystemTypeId, foundSysType.UserTypeId);
            foreach (var columnRow in cols)
            {
                var columnName = columnRow.Name ?? throw new InvalidOperationException("Column name is null for table type column");
                var isNullable = columnRow.IsNullable.GetValueOrDefault(true); // nullable by default
                var propertyName = GetPascalCase(columnName);
                var csharpTypeInfo = await GetCSharpTypeInfoAsync(server, columnRow.SystemTypeId, columnRow.UserTypeId);
                var propertyType = isNullable ? csharpTypeInfo.TypeRefNullable : csharpTypeInfo.TypeRef;
                var propertyTypeWithoutNullable = csharpTypeInfo.TypeRef;
                var isValueType = csharpTypeInfo.IsValueType;
                var maxLength = columnRow.MaxLength;

                var recordProperty = new RecordProperty
                {
                    FieldName = propertyName,
                    FieldType = propertyType,
                    FieldTypeForGeneric = propertyTypeWithoutNullable,
                    FieldTypeIsValueType = isValueType,
                    ColumnName = columnName,
                    ColumnIsNullable = isNullable,
                    MaxLength = maxLength,
                };
                record.Properties.Add(recordProperty);
            }
            this.codeFile.Records.Add(record);

            var listName = $"List<{rowCSharpTypeName}>";
            return new()
            {
                TypeRef = listName,
                TypeRefNullable = listName + "?",
                IsValueType = false,
                IsTableType = true,
                TableTypeRef = tableCSharpTypeName,
                SqlDbType = SqlDbType.Structured,
            };
        }

        // Not a native type
        throw new NotImplementedException("User type not implemented yet: " + systemTypeId + ", " + userTypeId);
    }
}

public class CodeFile
{
    /// <summary>
    /// Name of the file namespace
    /// </summary>
    public String? Namespace { get; set; }

    /// <summary>
    /// Name of the C# type containing the commands
    /// </summary>
    public String? ClassName { get; set; }

    /// <summary>
    /// All the type names used in this file, so we can avoid collisions
    /// </summary>
    public HashSet<String> TypeNames { get; } = new();

    public List<Record> Records { get; } = new();
    public List<Method> Methods { get; } = new();

    public String GenerateCode()
    {
        var code = new CodeWriter();

        code.UsingNamespace("System");
        code.UsingNamespace("System.Collections.Generic");
        code.UsingNamespace("System.Data");
        code.UsingNamespace("System.Threading");
        code.UsingNamespace("System.Threading.Tasks");
        code.UsingNamespace("Microsoft.Data.SqlClient");
        code.Line();

        code.Line("#nullable enable");
        code.Line();

        code.FileNamespace(Namespace);
        code.Line();

        var isFirstRecord = true;
        foreach (var record in Records.OrderBy(i => i.CSharpName).ThenBy(i => i.TableTypeCSharpName))
        {
            if (isFirstRecord) { isFirstRecord = false; }
            else { code.Line(); }

            string? rowClassName = record.CSharpName;
            using (var recordClass = code.PartialRecordClass("public", rowClassName))
            {
                foreach (var property in record.Properties)
                {
                    code.Line("  public {0} {1} {{ get; set; }}", property.FieldType, property.FieldName);
                }
            }
            if (record.IsTableType)
            {
                string? dataTableClassName = record.TableTypeCSharpName;

                using var tableClass = code.PartialClass("public sealed", dataTableClassName, "DataTable");
                code.Line("public {0}() : this(new List<{1}>()) {{ }}", dataTableClassName, rowClassName);
                code.Line("public {0}(List<{1}> rows) : base()", dataTableClassName, rowClassName);
                using (code.CreateBraceScope())
                {
                    code.Line("ArgumentNullException.ThrowIfNull(rows);");
                    code.Line();
                    foreach (var col in record.Properties)
                    {
                        var allowDbNull = col.ColumnIsNullable ? "true" : "false";
                        var maxLength = (col.MaxLength is short s && col.FieldType?.ToLowerInvariant() == "string") ? $", MaxLength = {s}" : String.Empty; // the default is already "-1", so we do not have to emit this in code.
                        var propertyTypeName = col.FieldTypeForGeneric;
                        code.Line("base.Columns.Add(new DataColumn() {{ ColumnName = \"{0}\", DataType = typeof({1}), AllowDBNull = {2}{3} }});", col.ColumnName, propertyTypeName, allowDbNull, maxLength);
                    }
                    using (code.ForEach("var row in rows"))
                    {
                        var parameterBuilder = String.Empty;
                        foreach (var col in record.Properties)
                        {
                            var localName = GetCamelCase(col.FieldName);
                            var propName = col.FieldName;  // TODO: was "PropertyName"

                            if (col.FieldType?.ToLowerInvariant() == "string" && col.MaxLength is { } maxLength && maxLength > 1)
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

        code.Line();
        using (code.PartialClass("public", ClassName))
        {
            code.Text(
"""
    private static SqlParameter CreateParameter(String parameterName, Object? value, SqlDbType sqlDbType, Int32 size = -1, ParameterDirection direction = ParameterDirection.Input) => new()
    {
        Size = size,
        Direction = direction,
        SqlDbType = sqlDbType,
        ParameterName = parameterName,
        Value = value ?? DBNull.Value,
    };

    private static SqlParameter CreateParameter(String parameterName, Object? value, SqlDbType sqlDbType, String typeName, Int32 size = -1, ParameterDirection direction = ParameterDirection.Input) => new()
    {
        Size = size,
        Direction = direction,
        TypeName = typeName,
        SqlDbType = sqlDbType,
        ParameterName = parameterName,
        Value = value ?? DBNull.Value,
    };

    private static T? OptionalClass<T>(SqlDataReader reader, int ordinal) where T : class => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
    private static T? OptionalValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
    private static T RequiredClass<T>(SqlDataReader reader, int ordinal) where T : class => reader.IsDBNull(ordinal) ? throw new NullReferenceException() : reader.GetFieldValue<T>(ordinal);
    private static T RequiredValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.IsDBNull(ordinal) ? throw new NullReferenceException() : reader.GetFieldValue<T>(ordinal);

    private static SqlCommand CreateStatement(SqlConnection connection, String text) => new() { Connection = connection, CommandType = CommandType.Text, CommandText = text, };
    private static SqlCommand CreateStoredProcedure(SqlConnection connection, String text) => new() { Connection = connection, CommandType = CommandType.StoredProcedure, CommandText = text, };

""");
            code.Line();
            var isFirstMethod = true;
            foreach (var method in Methods.OrderBy(i => i.Name))
            {
                if (isFirstMethod) { isFirstMethod = false; }
                else { code.Line(); }

                var commandText = method.CommandText.ReplaceLineEndings(" ").Trim();
                var csharpParameters = method.CSharpParameters;
                var commandParameters = method.SqlParameters;

                foreach (var isAsync in new[] { false, true })
                {
                    if (isAsync) { code.Line(); } // Assumes async always comes after sync

                    var methodParameters = csharpParameters.ToList(); // NOTE THE COPY!
                    methodParameters.Insert(0, new MethodParameter() { CSharpType = "SqlConnection", CSharpName = "connection" });
                    var parametersString = String.Join(", ", methodParameters.Select(i => $"{i.CSharpType} {i.CSharpName}"));

                    var asyncKeyword = isAsync ? " async" : "";
                    var returnType = isAsync ? $"Task<{method.DataType}>" : method.DataType;
                    var actualMethodName = isAsync ? $"{method.Name}Async" : method.Name;

                    using var _ = code.Method($"public static{asyncKeyword}", returnType, actualMethodName, parametersString);
                    var cmdMethod = method.IsStoredProc ? "CreateStoredProcedure" : "CreateStatement";
                    code.Line($"using SqlCommand cmd = {cmdMethod}(connection, \"{commandText}\");");
                    code.Line();

                    // TODO: generate parameters
                    if (commandParameters.Count > 0)
                    {
                        foreach (var parameter in commandParameters)
                        {
                            code.Line($"cmd.Parameters.Add(CreateParameter(\"@{parameter.SqlName}\", {parameter.CSharpExpression}, SqlDbType.{parameter.SqlDbType}));");

                            // TODO:
                            // var withTableTypeName = (parameter.ParameterTableRef is String tableTypeName) ? $", \"{tableTypeName}\"" : "";
                            // var withSize = (parameter.MaxLength is { } maxLength and not -1) ? $", {maxLength}" : "";
                            // code.Line($"cmd.Parameters.Add(CreateParameter(\"@{parameter.ParameterName.TrimStart('@')}\", {parameter.ArgumentExpression}, SqlDbType.{parameter.ParameterType}{withTableTypeName}{withSize}));");
                        }

                        code.Line();
                    }

                    if (method.Record is { } record)
                    {
                        code.Line($"var result = new List<{record.CSharpName}>();");

                        IDisposable ifScope;
                        if (isAsync)
                        {
                            code.Line($"using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);");
                            ifScope = code.If("await reader.ReadAsync().ConfigureAwait(false)");
                        }
                        else
                        {
                            code.Line($"using var reader = cmd.ExecuteReader();");
                            ifScope = code.If("reader.Read()");
                        }

                        using (ifScope)
                        {
                            if (record.Properties.Count != 0)
                            {
                                foreach (var recordProperty in record.Properties)
                                {
                                    String columnName = recordProperty.ColumnName;
                                    code.Line($"int ord{GetPascalCase(columnName)} = reader.GetOrdinal(\"{columnName}\");");
                                }
                                code.Line();
                            }

                            IDisposable doWhileScope;
                            if (isAsync)
                            {
                                doWhileScope = code.DoWhile("await reader.ReadAsync().ConfigureAwait(false)");
                            }
                            else
                            {
                                doWhileScope = code.DoWhile("reader.Read()");
                            }

                            using (doWhileScope)
                            {
                                code.Line($"result.Add(new {record.CSharpName}");
                                using (code.CreateBraceScope(preamble: null, withClosingBrace: ");"))
                                {
                                    foreach (var property in record.Properties)
                                    {
                                        var fieldName = property.FieldName;
                                        var fieldType = property.FieldType;
                                        var fieldTypeForGeneric = property.FieldTypeForGeneric;
                                        var columnName = property.ColumnName;
                                        var IsValueType = property.FieldTypeIsValueType;
                                        var ColumnIsNullable = property.ColumnIsNullable;

                                        var ordinalVarName = $"ord{GetPascalCase(columnName)}";
                                        var line = (IsValueType, ColumnIsNullable) switch
                                        {
                                            (false, true) => String.Format("{0} = OptionalClass<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeForGeneric),
                                            (true, true) => String.Format("{0} = OptionalValue<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeForGeneric),
                                            (false, false) => String.Format("{0} = RequiredClass<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeForGeneric),
                                            (true, false) => String.Format("{0} = RequiredValue<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeForGeneric),
                                        };
                                        code.Line(line);
                                    }
                                }
                            }
                        }
                    }

                    code.Return("result");
                }
            }
        }

        return code.ToString();
    }
}

public class Method
{
    /// <summary>
    /// Name of the method
    /// </summary>
    public String? Name { get; set; }

    /// <summary>
    /// SQL command text
    /// </summary>
    public string CommandText { get; internal set; }

    /// <summary>
    /// Data type returned by the method
    /// </summary>
    public String? DataType { get; set; }

    public Boolean IsStoredProc { get; set; }

    public List<MethodParameter> CSharpParameters { get; set; } = new();

    public List<CommandParameter> SqlParameters { get; set; } = new();

    /// <summary>
    /// Type of recordset
    /// </summary>
    public Record? Record { get; internal set; }
}

public class MethodParameter
{
    /// <summary>
    /// Name of the parameter
    /// </summary>
    public String? CSharpName { get; set; }

    /// <summary>
    /// Type of the parameter
    /// </summary>
    public String? CSharpType { get; set; }
}

public class CommandParameter
{
    /// <summary>
    /// Name of the parameter in the SQL command
    /// </summary>
    public String? SqlName { get; set; }

    /// <summary>
    /// SqlDbType of the parameter in the SQL command
    /// </summary>
    public SqlDbType? SqlDbType { get; set; }

    /// <summary>
    /// Reference to the value passed in to the command
    /// </summary>
    public string CSharpExpression { get; internal set; }
}

public class Record
{
    /// <summary>
    /// Name of the C# type
    /// </summary>
    public String? CSharpName { get; set; }

    public List<RecordProperty> Properties { get; } = new();

    public Boolean IsTableType = false;
    public String? TableTypeCSharpName { get; set; }
}

public class RecordProperty
{
    /// <summary>
    /// Name of the C# property
    /// </summary>
    public String? FieldName { get; set; }

    /// <summary>
    /// Type of the C# property
    /// </summary>
    public String FieldType { get; set; }

    public String FieldTypeForGeneric { get; set; }

    /// <summary>
    /// Name of the SQL column in the recordset
    /// </summary>
    public string ColumnName { get; set; }
    public bool FieldTypeIsValueType { get; internal set; }
    public bool ColumnIsNullable { get; internal set; }
    public short? MaxLength { get; init; }
}

public static class ListExtensions
{
    public static IEnumerable<(T, int)> WithIndex<T>(this IEnumerable<T> source)
    {
        var index = 0;
        foreach (var item in source)
        {
            yield return (item, index++);
        }
    }
}
