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
        using var sql = new SqlConnection();
        sql.ConnectionString = config.Connection?.ConnectionString;
        await sql.OpenAsync();
        var sysTypes = await Proxy.GetSysTypesAsync(sql);
        var dfrs = await Proxy.DmDescribeFirstResultSetAsync(sql, "SELECT name FROM sys.types where name = @name");
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

                if (database.Statements?.Items is { } statements)
                {
                    foreach (var statement in statements)
                    {
                        statement.ResultSet = new ResultSetMeta()
                        {
                            Columns = (await Proxy.DmDescribeFirstResultSetAsync(sql, statement.Text)).Select(i => new Column()
                            {
                                Name = i.Name ?? throw new NullReferenceException(),
                                Type = GetSqlDbType(i.TypeName),
                                IsNullable = i.IsNullable.GetValueOrDefault(true), // nullable by default
                            }).ToList(),
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
