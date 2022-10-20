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

        using TextWriter textWriter = args.Length switch
        {
            > 1 => new StreamWriter(args[1]),
            _ => Console.Out
        };

        await Generator.GenerateAsync(config, textWriter);
        textWriter.Close();

        // bootstrap test
        // using var sql = new SqlConnection();
        // sql.ConnectionString = config.Connection?.ConnectionString;
        // await sql.OpenAsync();
        // var arg = new List<Proxy.TempDb.ScopesRow> {
        //     new Proxy.TempDb.ScopesRow { Scope = "A" },
        //     new Proxy.TempDb.ScopesRow { Scope = "B" },
        //     new Proxy.TempDb.ScopesRow { Scope = "C" },
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

                database.TableTypes = new() { Items = await GetTableTypesForDatabaseAsync(sql) };

                if (database.Procedures?.Items is { } procs)
                {
                    foreach (var proc in procs)
                    {
                        var (schema, procName) = ParseSchemaItem(proc.Text);
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
                                parameter.SqlDbType = GetSqlDbType(parameter.Type);
                            }
                        }
                    }
                }
            }
        }
    }

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
            };

            foreach (var col in await Proxy.GetTableTypeColumnsAsync(sql, i.TypeTableObjectId))
            {
                var column = new Column
                {
                    Name = col.Name ?? throw new NullReferenceException(),
                    Type = GetSqlDbType(col.TypeName), // TODO: what if this is also a table type?
                    IsNullable = col.IsNullable ?? true,
                    MaxLength = col.MaxLength,
                };
                tableType.Columns.Add(column);
            }

            list.Add(tableType);
        }
        return list;
    }

    private static List<Parameter> GetParametersForProcedure(List<Proxy.TempDb.GetParametersForObjectRow> parameters) =>
        parameters.Select(i => new Parameter()
        {
            Name = i.Name?.TrimStart('@') ?? throw new NullReferenceException(),
            Type = i.TypeName,
            SqlDbType = i.IsTableType ? SqlDbType.Structured : GetSqlDbType(i.TypeName),
        }).ToList();

    private static List<Column> GetColumnsForResultSet<T>(List<T> resultSet) where T : Proxy.TempDb.IDmDescribeFirstResultSetRow =>
        resultSet.Select(i => new Column()
        {
            Name = i.Name ?? throw new NullReferenceException(),
            Type = GetSqlDbType(i.TypeName),
            // Type = i.IsTableType ? SqlDbType.Structured : GetSqlDbType(i.TypeName),
            IsNullable = i.IsNullable.GetValueOrDefault(true), // nullable by default
        }).ToList();

    private static (String, String) ParseSchemaItem(String text)
    {
        if (text.IndexOf('.') is int i and > 0)
        {
            var schema = text.Substring(0, i);
            var item = text.Substring(i + 1);
            return (schema, item);
        }
        else
        {
            throw new ArgumentException($"Unable to parse schema item: {text}", nameof(text));
        }
    }

    private static SqlDbType GetSqlDbType(String sqlTypeName) => sqlTypeName switch
    {
        "bit" => SqlDbType.Bit,
        "bigint" => SqlDbType.BigInt,
        "datetime" => SqlDbType.DateTime,
        "char" => SqlDbType.Char,
        "date" => SqlDbType.Date,
        "datetime2" => SqlDbType.DateTime2,
        "datetimeoffset" => SqlDbType.DateTimeOffset,
        "decimal" => SqlDbType.Decimal,
        "float" => SqlDbType.Float,
        "int" => SqlDbType.Int,
        "money" => SqlDbType.Money,
        "ntext" => SqlDbType.NText,
        "numeric" => SqlDbType.Decimal,
        "nvarchar" => SqlDbType.NVarChar,
        "real" => SqlDbType.Real,
        "smallint" => SqlDbType.SmallInt,
        "text" => SqlDbType.Text,
        "time" => SqlDbType.Time,
        "tinyint" => SqlDbType.TinyInt,
        "sysname" => SqlDbType.VarChar,
        "varchar" => SqlDbType.VarChar,
        "xml" => SqlDbType.Xml,
        "uniqueidentifier" => SqlDbType.UniqueIdentifier,
        "varbinary" => SqlDbType.VarBinary,
        _ => throw new InvalidOperationException("Unexpected SQL type name: " + sqlTypeName),
    };
}
