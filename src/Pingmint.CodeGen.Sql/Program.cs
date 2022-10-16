using Pingmint.CodeGen.Sql.Model;
using Microsoft.Data.SqlClient;
using System.Data;

namespace Pingmint.CodeGen.Sql;

internal sealed class Program
{
    internal static async Task Main(string[] args)
    {
        var config = new Config()
        {
            Connection = new()
            {
                ConnectionString = "Data Source=localhost;Initial Catalog=tempdb;User ID=sa;Password=SqlServerIs#1;Encrypt=False;",
            },
            CSharp = new()
            {
                Namespace = "Pingmint.CodeGen.Sql",
                ClassName = "Proxy",
            },
            Databases = new()
            {
                Items = new()
                {
                    new() {
                        Name = "tempdb",
                        Statements = new()
                        {
                            Items = new()
                            {
                                new()
                                {
                                    Name = "GetSysTypes",
                                    Text = "SELECT name FROM sys.types",
                                    Parameters = new()
                                },
                                new()
                                {
                                    Name = "DmDescribeFirstResultSet",
                                    Text = "SELECT D.name, D.system_type_id, D.is_nullable, D.column_ordinal, T.name as [type_name] FROM sys.dm_exec_describe_first_result_set(@text, NULL, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id) WHERE T.name <> 'sysname' ORDER BY D.column_ordinal",
                                    Parameters = new()
                                    {
                                        Items = new()
                                        {
                                            new()
                                            {
                                                Name = "text",
                                                Type = "varchar",
                                            },
                                        }
                                    }
                                },
                            },
                        },
                    },
                },
            },
        };

        await MetaAsync(config);


        using TextWriter textWriter = args.Length switch
        {
            > 0 => new StreamWriter(args[0]),
            _ => Console.Out
        };

        var gen = new Generator();
        await gen.Generate(config, textWriter);
        textWriter.Close();

        // bootstrap test
        using var sql = new SqlConnection();
        sql.ConnectionString = config.Connection?.ConnectionString;
        await sql.OpenAsync();
        var sysTypes = await Proxy.GetSysTypesAsync(sql);
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
                if (database.Statements?.Items is { } statements)
                {
                    foreach (var statement in statements)
                    {
                        statement.ResultSet = new ResultSetMeta()
                        {
                            Columns = (await Proxy.DmDescribeFirstResultSetAsync(sql, statement.Text)).Select(i => new Column()
                            {
                                Name = i.name ?? throw new NullReferenceException(),
                                Type = GetSqlDbType(i.type_name),
                                IsNullable = i.is_nullable.GetValueOrDefault(true), // nullable by default
                            }).ToList(),
                        };
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
