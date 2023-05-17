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
                    if (database.Statements?.Items is { } statements)
                    {
                        foreach (var statement in statements)
                        {
                            var parameters = statement.Parameters?.Items.Select(p => new SqlStatementParameter(p.Name, p.Type)).ToList() ?? new();
                            await analyzer.AnalyzeStatementAsync(statement.Name, statement.Text, parameters);
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
                                    await analyzer.AnalyzeProcedureAsync(databaseName, schema, row.Name, row.ObjectId);
                                }
                            }
                            else
                            {
                                if (IsExcluded(schema, procName)) { continue; }
                                if ((await Proxy.GetProcedureForSchemaAsync(sql, schema, procName)).FirstOrDefault() is not { } row) { continue; }
                                await analyzer.AnalyzeProcedureAsync(databaseName, schema, procName, row.ObjectId);
                            }
                        }
                    }
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

    public static (String?, String) ParseSchemaItem(String text)
    {
        if (text.IndexOf('.') is int i and > 0)
        {
            var schema = text[..i];
            var item = text[(i + 1)..];
            return (schema, item);
        }
        else
        {
            return (null, text); // schema-less
        }
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

    private static int connectionCount = 0;
    private static int totalOpened = 0;
    private async Task<SqlConnection> OpenSqlConnectionAsync([System.Runtime.CompilerServices.CallerMemberName] String? caller = null)
    {
        var instance = ++connectionCount;
        var sql = new SqlConnection(connectionString);
        var openTask = sql.OpenAsync();
        if (openTask.IsCompleted)
        {
            WriteLine($"Dequeued connection {instance}/{totalOpened}{(caller == null ? "" : $" ({caller})")}");
        }
        else
        {
            var total = ++totalOpened;
            WriteLine($"Opening connection {instance}/{total}{(caller == null ? "" : $" ({caller})")}");
            await openTask;
            WriteLine($"Opened connection {instance}/{total}{(caller == null ? "" : $" ({caller})")}");
        }
        sql.ChangeDatabase(database);
        return sql;
    }

    public async Task AnalyzeProcedureAsync(string database, string schema, string proc, int procId)
    {
        WriteLine("Analyze Procedure: {0}.{1}.{2}", database, schema, proc);

        // To match statements
        var commandText = database + "." + schema + "." + proc;
        var name = proc;

        using var server = await OpenSqlConnectionAsync();
        server.ChangeDatabase(database);
        var procParameters = await Proxy.GetParametersForObjectAsync(server, procId);
        foreach (var procParameter in procParameters)
        {
            WriteLine("Parameter: {0} {1}", commandText, procParameter);
        }
        var statementParameters = procParameters.Select(p => new SqlStatementParameter(p.Name.TrimStart('@'), p.TypeName)).ToList();

        var parametersText = String.Join(", ", statementParameters.Select(p => $"@{p.Name} {p.Type}"));
        WriteLine("Parameters: {0}", parametersText);

        var columsRows = await Proxy.DmDescribeFirstResultSetForObjectAsync(server, procId);

        var recordName = GetPascalCase(name + "Row");

        var record = new Record
        {
            CSharpName = recordName,
        };

        // TODO: return type for statements that are not recordsets
        var isRecordSet = true;
        var returnType = isRecordSet ? $"List<{recordName}>" : throw new NotImplementedException("TODO: return type for statements that are not recordsets");

        // TODO: pull parameters from the YAML definition
        var methodParameters = new List<MethodParameter>();
        var commandParameters = new List<CommandParameter>();
        foreach (var procParam in await Proxy.GetParametersForObjectAsync(server, procId))
        {
            var csharpTypeInfo = await GetCSharpTypeInfoAsync(procParam.SystemTypeId, procParam.UserTypeId);
            var csharpIdentifier = GetCamelCase(procParam.Name);

            var methodParameter = new MethodParameter
            {
                CSharpName = csharpIdentifier,
                CSharpType = csharpTypeInfo.TypeRef,
            };
            methodParameters.Add(methodParameter);

            var commandExpression = csharpTypeInfo.IsTableType ? $"new {csharpTypeInfo.TableTypeRef}({csharpIdentifier})" : csharpIdentifier;
            var commandParameter = new CommandParameter
            {
                SqlName = procParam.Name.TrimStart('@'),
                SqlDbType = await GetSqlDbTypeAsync(procParam.SystemTypeId, procParam.UserTypeId),
                CSharpExpression = commandExpression,
            };
            commandParameters.Add(commandParameter);
        }

        var methodSync = new Method
        {
            Name = GetPascalCase(name),
            IsStoredProc = true,
            MakeSync = true,
            MakeAsync = false,
            DataType = returnType,
            CommandText = commandText,
            Record = record,
            CSharpParameters = methodParameters,
            SqlParameters = commandParameters,
        };

        var recordColumns = new List<RecordProperty>();

        foreach (var (columnRow, columnIndex) in columsRows.WithIndex())
        {
            var columnName = columnRow.Name ?? $"Column{columnIndex}"; // TODO: generated column names will not work with "GetOrdinal", use ColumnIndex as backup?
            var isNullable = columnRow.IsNullable.GetValueOrDefault(true); // nullable by default
            var propertyName = GetPascalCase(columnName);
            var csharpTypeInfo = await GetCSharpTypeInfoAsync(columnRow.SystemTypeId, columnRow.UserTypeId);
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
            record.Properties.Add(recordProperty);
        }

        codeFile.Records.Add(record);
        codeFile.Methods.Add(methodSync);

        WriteLine("Analyze Done: {0}.{1}.{2}", database, schema, proc);
    }

    public async Task AnalyzeStatementAsync(String name, String commandText, List<SqlStatementParameter> statementParameters)
    {
        WriteLine("Analyze Statement: {0}", name);

        var parametersText = String.Join(", ", statementParameters.Select(p => $"@{p.Name} {p.Type}"));
        WriteLine("Parameters: {0}", parametersText);

        using var server = await OpenSqlConnectionAsync();
        var columsRows = await Proxy.DmDescribeFirstResultSetAsync(server, commandText, parametersText);

        var recordName = GetPascalCase(name + "Row");

        var record = new Record
        {
            CSharpName = recordName,
        };

        // TODO: return type for statements that are not recordsets
        var isRecordSet = true;
        var returnType = isRecordSet ? $"List<{recordName}>" : throw new NotImplementedException("TODO: return type for statements that are not recordsets");

        var methodParameters = new List<MethodParameter>();
        var commandParameters = new List<CommandParameter>();
        foreach (var statementParameter in statementParameters)
        {
            var csharpIdentifier = GetCamelCase(statementParameter.Name);

            var methodParameter = new MethodParameter
            {
                CSharpName = csharpIdentifier,
                CSharpType = (await GetCSharpTypeInfoAsync(statementParameter.Type)).TypeRef,
            };
            methodParameters.Add(methodParameter);

            var commandParameter = new CommandParameter
            {
                SqlName = statementParameter.Name,
                SqlDbType = await GetSqlDbTypeAsync(statementParameter.Type),
                CSharpExpression = csharpIdentifier,
            };
            commandParameters.Add(commandParameter);
        }

        var methodSync = new Method
        {
            Name = GetPascalCase(name),
            IsStoredProc = false,
            MakeSync = true,
            MakeAsync = false,
            DataType = returnType,
            CommandText = commandText,
            Record = record,
            CSharpParameters = methodParameters,
            SqlParameters = commandParameters,
        };

        var recordColumns = new List<RecordProperty>();

        foreach (var (columnRow, columnIndex) in columsRows.WithIndex())
        {
            var columnName = columnRow.Name ?? $"Column{columnIndex}"; // TODO: generated column names will not work with "GetOrdinal", use ColumnIndex as backup?
            var isNullable = columnRow.IsNullable.GetValueOrDefault(true); // nullable by default
            var propertyName = GetPascalCase(columnName);
            var csharpTypeInfo = await GetCSharpTypeInfoAsync(columnRow.SystemTypeId, columnRow.UserTypeId);
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
            record.Properties.Add(recordProperty);
        }

        codeFile.Records.Add(record);
        codeFile.Methods.Add(methodSync);

        WriteLine("Analyze Done: {0}", name);
    }

    private List<GetSysTypesRow>? _sysTypes;
    private async Task<List<GetSysTypesRow>> GetSysTypesAsync()
    {
        if (_sysTypes is not null) return _sysTypes;
        using var server = await OpenSqlConnectionAsync();
        return _sysTypes ??= Proxy.GetSysTypes(server);
    }

    private List<GetTableTypesRow>? _tableTypes;
    private async Task<List<GetTableTypesRow>> GetTableTypesAsync()
    {
        if (_tableTypes is not null) return _tableTypes;
        using var server = await OpenSqlConnectionAsync();
        return _tableTypes ??= Proxy.GetTableTypes(server);
    }

    private SortedDictionary<Int32, GetTableTypesRow>? _tableTypesByObjectId;
    private async Task<GetTableTypesRow?> GetTableTypeAsync(int objectId)
    {
        var types = await GetTableTypesAsync();

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
    private async Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(int systemTypeId, int userTypeId)
    {
        var types = await GetTableTypesAsync();
        if (types.FirstOrDefault(i => i.SystemTypeId == systemTypeId && i.UserTypeId == userTypeId) is not { } tableType) { throw new InvalidOperationException($"Table type not found: {systemTypeId}, {userTypeId}"); }

        using var server = await OpenSqlConnectionAsync();
        return Proxy.GetTableTypeColumns(server, tableType.TypeTableObjectId);
    }

    private SortedDictionary<(Int32, Int32), GetSysTypesRow>? _sysTypesById;
    private async Task<GetSysTypesRow?> GetSysTypeAsync(int systemTypeId, int userTypeId)
    {
        var types = await GetSysTypesAsync();

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

    private List<GetNativeTypesRow>? _nativeTypes;
    private async Task<List<GetNativeTypesRow>> GetNativeTypesAsync()
    {
        if (_nativeTypes is not null) return _nativeTypes;
        using var server = await OpenSqlConnectionAsync();
        return _nativeTypes ??= Proxy.GetNativeTypes(server);
    }

    private SortedDictionary<(Int32, Int32), GetNativeTypesRow>? _nativeTypesById;
    private async Task<GetNativeTypesRow?> GetNativeTypeAsync(int systemTypeId, int userTypeId)
    {
        var nativeTypes = await GetNativeTypesAsync();

        if (_nativeTypesById is not { } nativeTypesById)
        {
            _nativeTypesById = nativeTypesById = new(nativeTypes.ToDictionary(t => ((int)t.SystemTypeId, t.UserTypeId)));
        }

        if (nativeTypesById.TryGetValue((systemTypeId, userTypeId), out var nativeType))
        {
            return nativeType;
        }
        return null;
    }

    private SortedDictionary<(Int32, Int32), SqlDbType>? _sqlDbTypesById2;
    private async Task<SqlDbType?> GetSqlDbTypeAsync(int systemTypeId, int userTypeId)
    {
        if (_sqlDbTypesById2 is not { } sqlDbTypesById)
        {
            sqlDbTypesById = new();
            foreach (var nativeTypeRow in await GetNativeTypesAsync())
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

        if (await GetSysTypeAsync(systemTypeId, userTypeId) is { } sysType)
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
    }
    private async Task<CSharpTypeInfo> GetCSharpTypeInfoAsync(int systemTypeId, int userTypeId)
    {
        // Determine if the type reference by the column is already known
        // If not, then try matching it to a system type
        // If not, then generate a new record type, and return it

        if (_csharpTypeInfosById.TryGetValue((systemTypeId, userTypeId), out var csharpTypeInfo2))
        {
            return csharpTypeInfo2;
        }

        if (await GetNativeTypeAsync(systemTypeId, userTypeId) is { } nativeType)
        {
            if (await GetSqlDbTypeAsync(systemTypeId, userTypeId) is { } sqlDbType)
            {
                var type = GetCSharpTypeForSqlDbType(sqlDbType) ?? throw new InvalidOperationException("Have SqlDbType without C# type for it" + sqlDbType);

                _csharpTypeInfosById[(systemTypeId, userTypeId)] = csharpTypeInfo2 = new()
                {
                    TypeRef = type.Name,
                    TypeRefNullable = type.Name + "?",
                    IsValueType = type.IsValueType,
                };

                return csharpTypeInfo2;
            }
        }

        if ((await GetSysTypesAsync()).FirstOrDefault(i => i.SystemTypeId == systemTypeId && i.UserTypeId == userTypeId) is not { } foundSysType)
        {
            throw new InvalidOperationException("Could not find sys.types row for " + systemTypeId + ", " + userTypeId);
        }

        // TODO: this is not done yet
        if (foundSysType.IsTableType)
        {
            // TODO: already cached?

            string rowCSharpTypeName = GetPascalCase(foundSysType.Name + "Row");
            string tableCSharpTypeName = GetPascalCase(foundSysType.Name + "Table");

            var record = new Record()
            {
                CSharpName = rowCSharpTypeName,
                IsTableType = true,
                TableTypeCSharpName = tableCSharpTypeName,
            };

            using var server = await OpenSqlConnectionAsync();
            var cols = await GetTableTypeColumnsAsync(foundSysType.SystemTypeId, foundSysType.UserTypeId);
            foreach (var columnRow in cols)
            {
                var columnName = columnRow.Name ?? throw new InvalidOperationException("Column name is null for table type column");
                var isNullable = columnRow.IsNullable.GetValueOrDefault(true); // nullable by default
                var propertyName = GetPascalCase(columnName);
                var csharpTypeInfo = await GetCSharpTypeInfoAsync(columnRow.SystemTypeId, columnRow.UserTypeId);
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
            };
        }

        // Not a native type
        throw new NotImplementedException("User types not implemented yet: " + systemTypeId + ", " + userTypeId);
    }

    private readonly SortedDictionary<String, DmDescribeFirstResultSetRow> _sqlTypeDeclarations = new();
    private async Task<DmDescribeFirstResultSetRow?> GetSqlTypeDeclarationAsync(String declaration)
    {
        if (_sqlTypeDeclarations.TryGetValue(declaration, out var row)) { return row; }

        using var server = await OpenSqlConnectionAsync();
        row = Proxy.DmDescribeFirstResultSet(server, $"DECLARE @x {declaration}; SELECT @x;", "").FirstOrDefault();
        if (row is not null)
        {
            _sqlTypeDeclarations[declaration] = row;
            WriteLine($"SQL type declaration: {declaration} => {row}");
        }
        return row;
    }

    private readonly SortedDictionary<String, SqlDbType> _sqlDbTypesByDeclaration = new();
    private async Task<SqlDbType?> GetSqlDbTypeAsync(String declaration)
    {
        if (_sqlDbTypesByDeclaration.TryGetValue(declaration, out var sqlDbType)) { return sqlDbType; }

        if (await GetSqlTypeDeclarationAsync(declaration) is not { } row) throw new InvalidOperationException("No result set for SQL type declaration");
        if (await GetSqlDbTypeAsync(row.SystemTypeId, row.UserTypeId) is { } sqlDbType2)
        {
            _sqlDbTypesByDeclaration[declaration] = sqlDbType2;
            return sqlDbType2;
        }
        return null;
    }

    private readonly SortedDictionary<String, CSharpTypeInfo> _csharpTypeInfoByDeclaration = new();
    private async Task<CSharpTypeInfo> GetCSharpTypeInfoAsync(String declaration)
    {
        if (_csharpTypeInfoByDeclaration.TryGetValue(declaration, out var value)) { return value; }

        if (await GetSqlTypeDeclarationAsync(declaration) is not { } row) { throw new InvalidOperationException("No result set for SQL type declaration: " + declaration); }
        if (await GetCSharpTypeInfoAsync(row.SystemTypeId, row.UserTypeId) is not { } output) { throw new InvalidOperationException("No CSharpTypeInfo for SQL type declaration: " + declaration); }
        _csharpTypeInfoByDeclaration[declaration] = output;
        return output;
    }
}

/**************************************************************************************************************/
public partial record class IntTableTypeRow
{
    public Int32 ID { get; set; }
}

public sealed partial class IntTableTypeRowDataTable : DataTable
{
    public IntTableTypeRowDataTable() : this(new List<IntTableTypeRow>()) { }
    public IntTableTypeRowDataTable(List<IntTableTypeRow> rows) : base()
    {
        ArgumentNullException.ThrowIfNull(rows);

        base.Columns.Add(new DataColumn() { ColumnName = "ID", DataType = typeof(Int32), AllowDBNull = false });
        foreach (var row in rows)
        {
            var iD = row.ID;
            base.Rows.Add(iD);
        }
    }
}
/**************************************************************************************************************/

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

        code.FileNamespace(Namespace);
        code.Line();

        foreach (var record in Records.OrderBy(i => i.CSharpName).ThenBy(i => i.TableTypeCSharpName))
        {
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
                        var propertyTypeName = col.FieldType; // TODO: col.PropertyTypeName.TrimEnd('?');
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
            foreach (var method in Methods.OrderBy(i => i.Name))
            {
                // var methodName = method.Name;
                var commandText = method.CommandText.ReplaceLineEndings(" ").Trim();
                var csharpParameters = method.CSharpParameters;
                var commandParameters = method.SqlParameters;

                foreach (var isAsync in new[] { false, true })
                {
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
                    foreach (var parameter in commandParameters)
                    {
                        code.Line($"cmd.Parameters.Add(CreateParameter(\"@{parameter.SqlName}\", {parameter.CSharpExpression}, SqlDbType.{parameter.SqlDbType}));");

                        // TODO:
                        // var withTableTypeName = (parameter.ParameterTableRef is String tableTypeName) ? $", \"{tableTypeName}\"" : "";
                        // var withSize = (parameter.MaxLength is { } maxLength and not -1) ? $", {maxLength}" : "";
                        // code.Line($"cmd.Parameters.Add(CreateParameter(\"@{parameter.ParameterName.TrimStart('@')}\", {parameter.ArgumentExpression}, SqlDbType.{parameter.ParameterType}{withTableTypeName}{withSize}));");
                    }

                    code.Line();

                    if (method.Record is { } record)
                    {
                        code.Line($"var result = new List<{record.CSharpName}>();");

                        IDisposable ifScope;
                        if (isAsync)
                        {
                            code.Line($"using var reader = await cmd.ExecuteReaderAsync();");
                            ifScope = code.If("await reader.ReadAsync()");
                        }
                        else
                        {
                            code.Line($"using var reader = cmd.ExecuteReader();");
                            ifScope = code.If("reader.Read()");
                        }

                        using (ifScope)
                        {
                            foreach (var recordProperty in record.Properties)
                            {
                                String columnName = recordProperty.ColumnName;
                                code.Line($"var ord{GetPascalCase(columnName)} = reader.GetOrdinal(\"{columnName}\");");
                            }

                            IDisposable doWhileScope;
                            if (isAsync)
                            {
                                doWhileScope = code.DoWhile("await reader.ReadAsync()");
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
    /// Instruction to make an async method
    /// </summary>
    public Boolean MakeAsync { get; set; }

    /// <summary>
    /// Instruction to make a sync method
    /// </summary>
    public Boolean MakeSync { get; set; }

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
