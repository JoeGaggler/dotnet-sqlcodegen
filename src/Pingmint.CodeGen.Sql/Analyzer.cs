using Pingmint.CodeGen.Sql.Model.Yaml;
//using Pingmint.CodeGen.Sql.Refactor;
using Microsoft.Data.SqlClient;
using System.Data;

using static System.Console;
using static Pingmint.CodeGen.Sql.Globals;

namespace Pingmint.CodeGen.Sql;

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
        WriteLine("Database.GetParametersForObjectAsync");
        foreach (var procParam in await Database.GetParametersForObjectAsync(server, procId, CancellationToken.None))
        {
            var (methodParameter, commandParameter) = await AnalyzeParameterAsync(server, procParam.Name, procParam.SystemTypeId, procParam.UserTypeId, procParam.MaxLength);
            methodParameters.Add(methodParameter);
            commandParameters.Add(commandParameter);
        }

        var methodSync = new Method
        {
            Name = GetPascalCase(proc),
            IsStoredProc = true,
            CommandText = commandText,
            CSharpParameters = methodParameters,
            SqlParameters = commandParameters,
        };

        var recordColumns = new List<RecordProperty>();

        WriteLine("Database.DmDescribeFirstResultSetForObjectAsync");
        var columnsRows = await Database.DmDescribeFirstResultSetForObjectAsync(server, procId, CancellationToken.None);
        if (columnsRows.Count == 0)
        {
            methodSync.HasResultSet = false;
            methodSync.DataType = "int";
        }
        else
        {
            String recordName = GetUniqueName(GetPascalCase(proc + "Row"), codeFile.TypeNames);
            var record = new Record
            {
                CSharpName = recordName,
            };

            foreach (var columnRow in columnsRows)
            {
                if (await AnalyzeResultAsync(server, columnRow) is { } recordProperty)
                {
                    record.Properties.Add(recordProperty);
                }
            }

            methodSync.HasResultSet = true;
            methodSync.ResultSetRecord = record;
            methodSync.DataType = $"List<{recordName}>";
            codeFile.Records.Add(record);
            if (record.Properties.Count > 0) { this.codeFile.OrdinalAliases.Add(record.Properties.Count); }
        }
        codeFile.Methods.Add(methodSync);

        WriteLine("Analyze Done: {0}.{1}.{2} ({3:0.0}s)", database, schema, proc, (DateTime.UtcNow - t0).TotalSeconds);
    }

    public async Task AnalyzeConstantAsync(string databaseName, string constName, string query, string nameAttr, string valueAttr)
    {
        await Task.CompletedTask;
        WriteLine("Analyze Constant: {0}", constName);
        try
        {
            using var server = await OpenSqlConnectionAsync();

            var constant = new Constant
            {
                Name = GetUniqueName(GetPascalCase(constName), codeFile.TypeNames),
            };

            var columnsRows = await Database.DmDescribeFirstResultSetAsync(server, query, null, CancellationToken.None);

            // TODO: error handling: no rows, wrong number of rows

            foreach (var columnRow in columnsRows)
            {
                WriteLine("Column: {0} {1}", columnRow.Name, columnRow.SqlTypeName);
            }

            if (columnsRows.FirstOrDefault(r => r.Name == nameAttr) is not { } nameRow)
            {
                WriteLine("Name column not found: {0}", nameAttr);
                return;
            }

            if (columnsRows.FirstOrDefault(r => r.Name == valueAttr) is not { } valueRow)
            {
                WriteLine("Value column not found: {0}", valueAttr);
                return;
            }

            if (await AnalyzeResultAsync(server, nameRow) is not { } nameProperty)
            {
                WriteLine("Name column not valid: {0}", nameAttr);
                return;
            }
            WriteLine("Constant Name Property: {0} {1}", nameProperty.FieldName, nameProperty.FieldType);

            if (await AnalyzeResultAsync(server, valueRow) is not { } valueProperty)
            {
                WriteLine("Value column not valid: {0}", valueAttr);
                return;
            }
            WriteLine("Constant Value Property: {0} {1}", valueProperty.FieldName, valueProperty.FieldType);

            var cmd = server.CreateCommand();
            cmd.CommandText = query;
            cmd.CommandType = CommandType.Text;
            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
            {
                WriteLine("No rows returned: {0}", constName);
                return;
            }
            var ordName = reader.GetOrdinal(nameAttr);
            var ordValue = reader.GetOrdinal(valueAttr);
            var uniqueHashSet = new HashSet<String>();
            do
            {
                if (reader.IsDBNull(ordName) || reader.IsDBNull(ordValue)) { continue; }
                var nameResult = reader.GetString(ordName);
                var valueResult = reader.GetValue(ordValue);

                var nameSetter = GetUniqueName(GetPascalCase(nameResult), uniqueHashSet);
                var valueSetter = valueProperty.FieldTypeWithoutNullable switch
                {
                    "String" => $"\"{valueResult}\"",
                    "Int32" => $"{valueResult}",
                    "Boolean" => $"{valueResult}",
                    _ => throw new NotImplementedException("Unknown constant type: " + valueProperty.FieldType),
                };

                var adding = $"public const {valueProperty.FieldType} {nameSetter} = {valueSetter};";
                WriteLine("Adding: " + adding);
                constant.Items.Add(adding);
            } while (await reader.ReadAsync());

            codeFile.Constants.Add(constant);
        }
        catch (Exception ex)
        {
            WriteLine("Analyze Error: {0}", constName);
            WriteLine(ex);
        }
    }

    public async Task AnalyzeStatementAsync(String database, String name, String commandText, List<SqlStatementParameter> statementParameters)
    {
        WriteLine("Analyze Statement: {0}", name);
        try
        {
            using var server = await OpenSqlConnectionAsync();

            var methodParameters = new List<MethodParameter>();
            var commandParameters = new List<CommandParameter>();
            foreach (var procParam in statementParameters)
            {
                var sysType = await GetSysTypeByNameAsync(server, procParam.Type);
                var (methodParameter, commandParameter) = await AnalyzeParameterAsync(server, procParam.Name, sysType.SystemTypeId, sysType.UserTypeId, sysType.MaxLength);
                methodParameters.Add(methodParameter);
                commandParameters.Add(commandParameter);
            }

            var methodSync = new Method
            {
                Name = GetPascalCase(name),
                IsStoredProc = false,
                CommandText = commandText,
                CSharpParameters = methodParameters,
                SqlParameters = commandParameters,
            };

            var recordColumns = new List<RecordProperty>();

            var parametersText = String.Join(", ", statementParameters.Select(p => $"@{p.Name} {p.Type}"));
            var columnsRows = await Database.DmDescribeFirstResultSetAsync(server, commandText, parametersText, CancellationToken.None);
            if (columnsRows.Count == 0)
            {
                methodSync.HasResultSet = false;
                methodSync.DataType = "int";
            }
            else
            {
                String recordName = GetUniqueName(GetPascalCase(name + "Row"), codeFile.TypeNames);
                var record = new Record
                {
                    CSharpName = recordName,
                };

                foreach (var columnRow in columnsRows)
                {
                    if (await AnalyzeResultAsync(server, columnRow) is { } recordProperty)
                    {
                        record.Properties.Add(recordProperty);
                    }
                }

                methodSync.HasResultSet = true;
                methodSync.ResultSetRecord = record;
                methodSync.DataType = $"List<{recordName}>";
                codeFile.Records.Add(record);
                if (record.Properties.Count > 0) { this.codeFile.OrdinalAliases.Add(record.Properties.Count); }
            }
            codeFile.Methods.Add(methodSync);
            WriteLine("Analyze Done: {0}", name);
        }
        catch (Exception ex)
        {
            WriteLine("Analyze Error: {0}", name);
            WriteLine(ex);
        }
    }

    private async Task<(MethodParameter, CommandParameter)> AnalyzeParameterAsync(SqlConnection server, String Name, int SystemTypeId, int UserTypeId, short? maxLength)
    {
        var csharpTypeInfo = await GetCSharpTypeInfoAsync(server, SystemTypeId, UserTypeId);
        var csharpIdentifier = GetCamelCase(Name);

        var methodParameter = new MethodParameter
        {
            CSharpName = csharpIdentifier,
            CSharpType = csharpTypeInfo.TypeRefNullable,
        };

        var commandExpression = csharpTypeInfo.IsTableType ? $"new {csharpTypeInfo.TableTypeRef}({csharpIdentifier})" : csharpIdentifier;
        var commandParameter = new CommandParameter
        {
            SqlName = Name.TrimStart('@'),
            SqlDbType = csharpTypeInfo.SqlDbType,
            SqlTypeName = csharpTypeInfo.SqlTableTypeRef,
            CSharpExpression = commandExpression,
            MaxLength = maxLength,
        };

        return (methodParameter, commandParameter);
    }

    private async Task<RecordProperty?> AnalyzeResultAsync(SqlConnection server, IDmDescribeFirstResultSetRow columnRow)
    {
        if (columnRow.Name is not { } columnName)
        {
            WriteLine("WARNING: Column without has no name");
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
            FieldTypeWithoutNullable = propertyTypeWithoutNullable,
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
        WriteLine("Database.GetSysTypesAsync");
        return _sysTypes ??= await Database.GetSysTypesAsync(server, CancellationToken.None);
    }

    private readonly SortedDictionary<String, GetSysTypesRow> _sysTypesByName = new();
    private async Task<GetSysTypesRow> GetSysTypeByNameAsync(SqlConnection server, string typeName)
    {
        if (_sysTypesByName.TryGetValue(typeName, out var sysType)) { return sysType; }

        var xxx = Database.DmDescribeFirstResultSet(server, $"DECLARE @x {typeName}; SELECT @x AS x;", "");
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
        WriteLine("Database.GetTableTypesAsync");
        return _tableTypes ??= await Database.GetTableTypesAsync(server, CancellationToken.None);
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

        WriteLine("Database.GetTableTypeColumnsAsync");
        return await Database.GetTableTypeColumnsAsync(server, tableType.TypeTableObjectId, CancellationToken.None);
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
        public SqlDbType SqlDbType { get; init; }
        public String? TableTypeRef { get; init; }
        public String? SqlTableTypeRef { get; init; }
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
                    FieldTypeWithoutNullable = propertyTypeWithoutNullable,
                    FieldTypeIsValueType = isValueType,
                    ColumnName = columnName,
                    ColumnIsNullable = isNullable,
                    MaxLength = maxLength,
                };
                record.Properties.Add(recordProperty);
            }
            this.codeFile.Records.Add(record);
            if (record.Properties.Count > 0) { this.codeFile.OrdinalAliases.Add(record.Properties.Count); }

            var listName = $"List<{rowCSharpTypeName}>";
            return new()
            {
                TypeRef = listName,
                TypeRefNullable = listName + "?",
                IsValueType = false,
                IsTableType = true,
                TableTypeRef = tableCSharpTypeName,
                SqlDbType = SqlDbType.Structured,
                SqlTableTypeRef = foundSysType.IsTableType ? foundSysType.SchemaName + "." + foundSysType.Name : null,
            };
        }

        // Not a native type
        throw new NotImplementedException("User type not implemented yet: " + systemTypeId + ", " + userTypeId);
    }
}
