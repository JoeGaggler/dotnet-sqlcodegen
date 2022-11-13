using Pingmint.CodeGen.Sql.Model;
using Microsoft.Data.SqlClient;
using System.Data;

namespace Pingmint.CodeGen.Sql;

internal sealed class Program
{
    internal static async Task Main(string[] args)
    {
        if (args.Length < 1)
        {
            throw new InvalidOperationException(); // TODO: print help message
        }

        var yaml = File.ReadAllText(args[0]);
        var config = ParseYaml(yaml);

        await MetaAsync(config);

        var code = new CodeWriter();
        Generator.Generate(config, code);

        using TextWriter textWriter = args.Length switch
        {
            > 1 => new StreamWriter(args[1]),
            _ => Console.Out
        };
        textWriter.Write(code.ToString());
        await textWriter.FlushAsync();
        textWriter.Close();

        // bootstrap test
        // using var sql = new SqlConnection();
        // sql.ConnectionString = config.Connection?.ConnectionString;
        // await sql.OpenAsync();
        // var arg = new List<ScopesRow> {
        //     new ScopesRow { Scope = "A" },
        //     new ScopesRow { Scope = "B" },
        //     new ScopesRow { Scope = "C" },
        // };
        // var echoScopes = await Proxy.EchoScopes2Async(sql, arg, arg);
        // foreach (var row in echoScopes)
        // {
        //     Console.WriteLine($"Echo Scope: {row.Scope}");
        // }
    }

    private static Config ParseYaml(String yaml)
    {
        using var stringReader = new StringReader(yaml);
        var parser = new YamlDotNet.Core.Parser(stringReader);
        var doc = new Yaml.DocumentYaml();
        var visitor = new Yaml.YamlVisitor(doc);
        while (parser.MoveNext())
        {
            parser.Current!.Accept(visitor);
        }
        var model = doc.Model;
        return model;
    }

    private static async Task<SqlConnection> OpenSqlAsync(Config config)
    {
        var sql = new SqlConnection();
        sql.ConnectionString = config.Connection?.ConnectionString;
        await sql.OpenAsync();
        return sql;
    }

    private static async Task MetaAsync(Config config)
    {
        using var sql = await OpenSqlAsync(config);

        if (config.Databases?.Items is { } databases)
        {
            foreach (var database in databases)
            {
                await sql.ChangeDatabaseAsync(database.Name);

                var allTypes = database.AllTypes = new();
                foreach (var type in await Proxy.GetSysTypesAsync(sql))
                {
                    SqlDbType sqlDbType;
                    try
                    {
                        sqlDbType = GetSqlDbType(type.Name);
                    }
                    catch
                    {
                        continue; // TODO: type not supported
                    }

                    var newType = new DatabaseTypeMeta()
                    {
                        SqlName = type.Name,
                        SqlTypeId = new() { SystemTypeId = type.SystemTypeId, UserTypeId = type.UserTypeId },
                        SqlDbType = sqlDbType,
                    };

                    newType.DotnetType = GetDotnetType(newType.SqlDbType);

                    allTypes.Types.Add(newType);
                }

                database.TableTypes = new() { Items = await GetTableTypesForDatabaseAsync(sql) };

                if (database.Procedures?.Items is { } procs)
                {
                    var insert = new List<Procedure>();
                    var remove = new List<Procedure>();
                    foreach (var proc in procs)
                    {
                        var (schema, procName) = ParseSchemaItem(proc.Text);
                        if (schema is null) { throw new InvalidOperationException($"Unable to parse schema item: {proc.Text}"); }

                        if (procName == "*")
                        {
                            remove.Add(proc);
                            foreach (var row in await Proxy.GetProceduresForSchemaAsync(sql, schema))
                            {
                                var newProc = new Procedure();
                                await MetaProcAsync(sql, newProc, schema, row.Name);
                                insert.Add(newProc);
                            }
                        }
                        else
                        {
                            await MetaProcAsync(sql, proc, schema, procName);
                        }

                        static async Task MetaProcAsync(SqlConnection sql, Procedure proc, string schema, string procName)
                        {
                            proc.Schema = schema;
                            proc.Name = procName;

                            if ((await Proxy.GetProcedureForSchemaAsync(sql, schema, procName)).FirstOrDefault() is not { } me)
                            {
                                throw new InvalidOperationException($"Unable to find procedure: {proc.Text}");
                            }

                            proc.Parameters = new() { Items = GetParametersForProcedure(await Proxy.GetParametersForObjectAsync(sql, me.ObjectId)) };

                            proc.ResultSet = new ResultSetMeta()
                            {
                                Columns = GetColumnsForResultSet(await Proxy.DmDescribeFirstResultSetForObjectAsync(sql, me.ObjectId)),
                            };
                        }
                    }
                    foreach (var item in remove) { procs.Remove(item); }
                    procs.AddRange(insert);
                }

                if (database.Statements?.Items is { } statements)
                {
                    foreach (var statement in statements)
                    {
                        statement.ResultSet = new ResultSetMeta() // TODO: same as procedure
                        {
                            Columns = GetColumnsForResultSet(await Proxy.DmDescribeFirstResultSetAsync(sql, statement.Text)),
                        };

                        if (statement.Parameters?.Items is { } parameters)
                        {
                            foreach (var parameter in parameters)
                            {
                                var found = database.AllTypes.Types.First(i => i.SqlName == parameter.Type);
                                parameter.SqlDbType = found.SqlDbType;
                                parameter.SqlTypeId = found.SqlTypeId;
                            }
                        }
                    }
                }
            }
        }
    }

    private static Type GetDotnetType(SqlDbType type) => type switch
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

    private static async Task<List<TableType>> GetTableTypesForDatabaseAsync(SqlConnection sql)
    {
        var list = new List<TableType>();
        foreach (var i in await Proxy.GetTableTypesAsync(sql))
        {
            var tableType = new TableType
            {
                TypeName = i.Name,
                SchemaName = i.SchemaName,
                Columns = new List<Column>(),
                SqlSystemTypeId = i.SystemTypeId,
                SqlUserTypeId = i.UserTypeId,
            };

            foreach (var col in await Proxy.GetTableTypeColumnsAsync(sql, i.TypeTableObjectId))
            {
                var column = new Column
                {
                    Name = col.Name ?? throw new NullReferenceException(),
                    Type = GetSqlDbType(col.TypeName), // TODO: what if this is also a table type?
                    IsNullable = col.IsNullable ?? true,
                    MaxLength = col.MaxLength,
                    SqlTypeId = new() { SystemTypeId = col.SystemTypeId, UserTypeId = col.UserTypeId },
                };
                tableType.Columns.Add(column);
            }

            list.Add(tableType);
        }
        return list;
    }

    private static List<Parameter> GetParametersForProcedure(List<GetParametersForObjectRow> parameters) =>
        parameters.Select(i => new Parameter()
        {
            Name = i.Name?.TrimStart('@') ?? throw new NullReferenceException(),
            Type = i.TypeName,
            SqlDbType = i.IsTableType ? SqlDbType.Structured : GetSqlDbType(i.TypeName),
            MaxLength = i.MaxLength,
            SqlTypeId = new() { SystemTypeId = i.SystemTypeId, UserTypeId = i.UserTypeId },
        }).ToList();

    private static List<Column> GetColumnsForResultSet<T>(List<T> resultSet) where T : IDmDescribeFirstResultSetRow =>
        resultSet.Select(i => new Column()
        {
            Name = i.Name ?? throw new NullReferenceException("missing column name"),
            Type = GetSqlDbType(i.SqlTypeName),
            // Type = i.IsTableType ? SqlDbType.Structured : GetSqlDbType(i.TypeName),
            IsNullable = i.IsNullable.GetValueOrDefault(true), // nullable by default
            SqlTypeId = new() { SystemTypeId = i.SystemTypeId, UserTypeId = i.UserTypeId },
        }).ToList();

    public static (String?, String) ParseSchemaItem(String text)
    {
        if (text.IndexOf('.') is int i and > 0)
        {
            var schema = text.Substring(0, i);
            var item = text.Substring(i + 1);
            return (schema, item);
        }
        else
        {
            return (null, text); // schema-less
        }
    }

    private static SqlDbType GetSqlDbType(String sqlTypeName) => sqlTypeName switch
    {
        // ints
        "bit" => SqlDbType.Bit,
        "tinyint" => SqlDbType.TinyInt,
        "smallint" => SqlDbType.SmallInt,
        "int" => SqlDbType.Int,
        "bigint" => SqlDbType.BigInt,

        // chars
        "char" => SqlDbType.Char,
        "nvarchar" => SqlDbType.NVarChar,
        "ntext" => SqlDbType.NText,
        "text" => SqlDbType.Text,

        // dates
        "date" => SqlDbType.Date,
        "datetime" => SqlDbType.DateTime,
        "datetime2" => SqlDbType.DateTime2,
        "datetimeoffset" => SqlDbType.DateTimeOffset,

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
        "varbinary" => SqlDbType.VarBinary,
        _ => throw new InvalidOperationException("Unexpected SQL type name: " + sqlTypeName),
    };
}
