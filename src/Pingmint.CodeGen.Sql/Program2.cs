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
            var analyzer = new Analyzer(codeFile, config);

            // config.Databases.Items.First().Statements.Items.First().Parameters.Items.First().Name

            if (config.Databases?.Items is { } databases)
            {
                foreach (var database in databases)
                {
                    if (database.Statements?.Items is { } statements)
                    {
                        foreach (var statement in statements)
                        {
                            var parameters = statement.Parameters?.Items.Select(p => new SqlStatementParameter(p.Name, p.Type)).ToList() ?? new();
                            await analyzer.AnalyzeStatementAsync(statement.Name, statement.Text, parameters);
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
}

public record class SqlStatementParameter(String Name, String Type);

public class Analyzer
{
    private CodeFile codeFile;
    private Config config;
    private String connectionString;

    public Analyzer(CodeFile codeFile, Config config)
    {
        this.codeFile = codeFile;
        this.config = config;

        codeFile.Namespace = config.CSharp.Namespace;
        codeFile.ClassName = config.CSharp.ClassName;

        this.connectionString = config.Connection.ConnectionString;
    }

    private int connectionCount = 0;
    private async Task<SqlConnection> OpenSqlConnectionAsync()
    {
        WriteLine($"Opening connection {++connectionCount}");
        var sql = new SqlConnection(connectionString);
        await sql.OpenAsync();
        return sql;
    }

    public async Task AnalyzeStatementAsync(String name, String commandText, List<SqlStatementParameter> statementParameters)
    {
        WriteLine("Analyze Statement: {0}", name);

        var parametersText = String.Join(", ", statementParameters.Select(p => $"@{p.Name} {p.Type}"));
        WriteLine("Parameters: {0}", parametersText);

        using var server = await OpenSqlConnectionAsync();
        var columsRows = Proxy.DmDescribeFirstResultSet(server, commandText, parametersText);

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
                SqlType = statementParameter.Type,
                SqlDbType = await GetSqlDbTypeAsync(statementParameter.Type),
                CSharpExpression = csharpIdentifier,
            };
            commandParameters.Add(commandParameter);
        }

        var methodSync = new Method
        {
            Name = GetPascalCase(name),
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

    private async Task<CSharpTypeInfo> GetCSharpTypeInfoAsync(String declaration)
    {
        // TODO: CACHE

        if (await GetSqlTypeDeclarationAsync(declaration) is not { } row) throw new InvalidOperationException("No result set for SQL type declaration");
        return await GetCSharpTypeInfoAsync(row.SystemTypeId, row.UserTypeId);
    }

    /////////////////////////////////////

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

    public static (String, String?) ParseSubscript(String text)
    {
        if (text.IndexOf('(') is int i and > 0)
        {
            if (text.IndexOf(')') is int j && j > i)
            {
                return (text[..i], text[(i + 1)..j]);
            }
        }

        return (text, null);
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

        foreach (var record in Records)
        {
            using var recordClass = code.PartialRecordClass("public", record.CSharpName);
            foreach (var property in record.Properties)
            {
                code.Line("  public {0} {1} {{ get; set; }}", property.FieldType, property.FieldName);
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

    private static T? GetField<T>(SqlDataReader reader, int ordinal) where T : class => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
    private static T? GetFieldValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
    private static T GetNonNullField<T>(SqlDataReader reader, int ordinal) where T : class => reader.IsDBNull(ordinal) ? throw new NullReferenceException() : reader.GetFieldValue<T>(ordinal);
    private static T GetNonNullFieldValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.IsDBNull(ordinal) ? throw new NullReferenceException() : reader.GetFieldValue<T>(ordinal);

    private static SqlCommand CreateStatement(SqlConnection connection, String text) => new() { Connection = connection, CommandType = CommandType.Text, CommandText = text, };
    private static SqlCommand CreateStoredProcedure(SqlConnection connection, String text) => new() { Connection = connection, CommandType = CommandType.StoredProcedure, CommandText = text, };

""");
            foreach (var method in Methods)
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
                    code.Line($"using SqlCommand cmd = CreateStatement(connection, \"{commandText}\");");
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
                                            (false, true) => String.Format("{0} = GetField<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeForGeneric),
                                            (true, true) => String.Format("{0} = GetFieldValue<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeForGeneric),
                                            (false, false) => String.Format("{0} = GetNonNullField<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeForGeneric),
                                            (true, false) => String.Format("{0} = GetNonNullFieldValue<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeForGeneric),
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
    /// Type of the parameter in the SQL command
    /// </summary>
    public String? SqlType { get; set; }

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
