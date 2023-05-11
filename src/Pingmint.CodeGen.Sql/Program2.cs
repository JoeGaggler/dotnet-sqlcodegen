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
    public static async Task Run(Config config)
    {
        var sync = new ConsoleSynchronizationContext();
        sync.Go(async () =>
        {
            var codeFile = new CodeFile();
            var analyzer = new Analyzer(codeFile, config);

            // TODO: search for "wrong"

            // config.Databases.Items.First().Statements.Items.First().Parameters.Items.First().Name

            var ppp = new List<(String, String)>();
            await analyzer.AnalyzeStatementAsync("GetProcedures", "SELECT * FROM sys.procedures", new List<SqlStatementParameter>() { new SqlStatementParameter("pretend", "varchar(max)") });

            WriteLine();

            WriteLine(codeFile.GenerateCode());
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

    private async Task<SqlConnection> OpenSqlConnectionAsync()
    {
        WriteLine("Opening connection");
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
        var columsRows = await Proxy.DmDescribeFirstResultSetAsync(server, commandText, parametersText, CancellationToken.None);

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
                CSharpType = await GetCSharpTypeAsync(statementParameter.Type),
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
            var propertyName = GetPascalCase(columnName);

            WriteLine("Column: {0} {1}", columnName, columnRow.GetSqlTypeId());

            var propertyType = "TODO";
            var isValueType = typeof(Boolean).IsValueType; // TODO
            var isNullable = columnRow.IsNullable.GetValueOrDefault(true); // nullable by default

            var recordProperty = new RecordProperty
            {
                FieldName = propertyName,
                FieldType = propertyType,
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

    private async Task<String> GetCSharpTypeAsync(string sqlType)
    {
        // TODO: check cache

        var (schema, schematype) = ParseSchemaItem(sqlType);
        var (maintype, subscript) = ParseSubscript(schematype);

        if (GetSqlDbType(maintype) is SqlDbType sqlDbType)
        {
            return GetDotnetType(sqlDbType).Name;
        }

        await Task.CompletedTask;
        return "TodoType";
    }

    private async Task<SqlDbType> GetSqlDbTypeAsync(string sqlType)
    {
        // TODO: check cache

        var (schema, schematype) = ParseSchemaItem(sqlType);
        var (maintype, subscript) = ParseSubscript(schematype);

        if (GetSqlDbType(maintype) is SqlDbType sqlDbType)
        {
            return sqlDbType;
        }

        var schemas = await GetSchemasAsync();
        var sysTypes = await GetSysTypesAsync();

        // TODO: full type lookup

        await Task.CompletedTask;
        return SqlDbType.SmallMoney;
    }

    private SqlDbType? GetSqlDbType(String sqlType) => sqlType switch
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

        _ => (SqlDbType?)null,
    };

    public static Type GetDotnetType(SqlDbType type) => type switch
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

        // TODO:
        // SqlDbType.Variant => throw new NotImplementedException(),
        // SqlDbType.Udt => throw new NotImplementedException(),
        // SqlDbType.Structured => throw new NotImplementedException(),

        _ => throw new InvalidOperationException("Unexpected SqlDbType: " + type.ToString()),
    };

    private List<GetSchemasRow>? _schemas;
    private async Task<List<GetSchemasRow>> GetSchemasAsync() => _schemas ??= await Proxy.GetSchemasAsync(await OpenSqlConnectionAsync());

    private List<GetSysTypesRow>? _sysTypes;
    private SortedDictionary<SqlTypeId, SqlTypeInfo> _sysTypeInfo = new();
    private async Task<List<GetSysTypesRow>> GetSysTypesAsync()
    {
        if (_sysTypes is not null) return _sysTypes;

        _sysTypes ??= await Proxy.GetSysTypesAsync(await OpenSqlConnectionAsync());

        foreach (var sysType in _sysTypes)
        {
            var sqlTypeId = new SqlTypeId()
            {
                SchemaId = sysType.SchemaId,
                SystemTypeId = sysType.SystemTypeId,
                UserTypeId = sysType.UserTypeId,
            };

            _sysTypeInfo.Add(sqlTypeId, new SqlTypeInfo
            {
                SqlTypeId = sqlTypeId,
                SqlName = sysType.Name,
                // TODO: cache other useful info
            });
        }

        return _sysTypes;
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
            foreach (var method in Methods)
            {
                // TODO: make separate async/sync methods

                var methodName = method.Name;
                var commandText = method.CommandText;
                var csharpParameters = method.CSharpParameters;
                var commandParameters = method.SqlParameters;
                var parametersString = String.Join(", ", csharpParameters.Select(i => $"{i.CSharpType} {i.CSharpName}"));

                using var _ = code.Method("public", method.DataType, method.Name, parametersString);
                code.Line($"using SqlCommand cmd = CreateStatement(connection, \"{commandText}\");");
                code.Line();

                // TODO: generate parameters
                foreach (var parameter in commandParameters)
                {
                    code.Line($"cmd.Parameters.Add(CreateParameter(\"@{parameter.SqlName}\", {parameter.CSharpExpression}, {parameter.SqlDbType}));");

                    // TODO:
                    // var withTableTypeName = (parameter.ParameterTableRef is String tableTypeName) ? $", \"{tableTypeName}\"" : "";
                    // var withSize = (parameter.MaxLength is { } maxLength and not -1) ? $", {maxLength}" : "";
                    // code.Line($"cmd.Parameters.Add(CreateParameter(\"@{parameter.ParameterName.TrimStart('@')}\", {parameter.ArgumentExpression}, SqlDbType.{parameter.ParameterType}{withTableTypeName}{withSize}));");
                }

                code.Line();

                if (method.Record is { } record)
                {
                    code.Line($"var result = new List<{record.CSharpName}>();");
                    code.Line($"using var reader = cmd.ExecuteReader();");
                    using (code.If("reader.Read()"))
                    {
                        foreach (var recordProperty in record.Properties)
                        {
                            String columnName = recordProperty.ColumnName;
                            code.Line($"var ord{GetPascalCase(columnName)} = cmd.GetOrdinal(\"{columnName}\");");
                        }

                        using (code.DoWhile("reader.Read()"))
                        {
                            code.Line($"result.Add(new {record.CSharpName}");
                            using (code.CreateBraceScope(preamble: null, withClosingBrace: ");"))
                            {
                                foreach (var property in record.Properties)
                                {
                                    var fieldName = property.FieldName;
                                    var fieldType = property.FieldType;
                                    var columnName = property.ColumnName;
                                    var IsValueType = property.FieldTypeIsValueType;
                                    var ColumnIsNullable = property.ColumnIsNullable;

                                    var ordinalVarName = $"ord{GetPascalCase(columnName)}";
                                    var line = (IsValueType, ColumnIsNullable) switch
                                    {
                                        (false, true) => String.Format("{0} = GetField<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldType),
                                        (true, true) => String.Format("{0} = GetFieldValue<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldType),
                                        (false, false) => String.Format("{0} = GetNonNullField<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldType),
                                        (true, false) => String.Format("{0} = GetNonNullFieldValue<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldType),
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

    /// <summary>
    /// Name of the SQL column in the recordset
    /// </summary>
    public string ColumnName { get; set; }
    public bool FieldTypeIsValueType { get; internal set; }
    public bool ColumnIsNullable { get; internal set; }
}

public record struct SqlTypeId : IComparable<SqlTypeId>
{
    public Int32 SchemaId;
    public Int32 SystemTypeId;
    public Int32 UserTypeId;

    public readonly int CompareTo(SqlTypeId other)
    {
        if (SchemaId.CompareTo(other.SchemaId) is var comp1 && comp1 != 0) { return comp1; }
        if (SystemTypeId.CompareTo(other.SystemTypeId) is var comp2 && comp2 != 0) { return comp2; }
        return UserTypeId.CompareTo(other.UserTypeId);
    }
}

public static class SqlTypeIdExtensions
{
    public static Model.SqlTypeId GetSqlTypeId(this ISqlTypeId sqlTypeId) => new() { SchemaId = sqlTypeId.SchemaId, SystemTypeId = sqlTypeId.SystemTypeId, UserTypeId = sqlTypeId.UserTypeId };
}

public class SqlTypeInfo
{
    // Key
    public SqlTypeId SqlTypeId { get; set; }

    // Info
    public String SqlName { get; internal set; }
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
