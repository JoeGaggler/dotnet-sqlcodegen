using Pingmint.CodeGen.Sql.Model;
using Pingmint.CodeGen.Sql.Model.Yaml;
using Microsoft.Data.SqlClient;
using System.Data;

using static Pingmint.CodeGen.Sql.Globals;

namespace Pingmint.CodeGen.Sql;

internal static class Ext
{
    public static T NotNull<T>(this T? obj) where T : class => obj ?? throw new NullReferenceException();
}

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

        var configMemo = await MetaAsync(config);

        var code = new CodeWriter();
        Generator.Generate(configMemo, code);

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

    private static async Task<ConfigMemo> MetaAsync(Config config)
    {
        var configMemo = new ConfigMemo();

        var cs = config.CSharp ?? throw new NullReferenceException();
        configMemo.Namespace = cs.Namespace;
        configMemo.ClassName = cs.ClassName;

        using var sql = await OpenSqlAsync(config);

        if (config.Databases?.Items is { } databases)
        {
            foreach (var database in databases)
            {
                await sql.ChangeDatabaseAsync(database.SqlName);

                configMemo.Databases[database.SqlName] = await PopulateDatabaseAsync(sql, database);
            }
        }

        return configMemo;
    }

    private static async Task<DatabaseMemo> PopulateDatabaseAsync(SqlConnection sql, DatabasesItem database)
    {
        var sqlDatabaseName = database.SqlName.NotNull();

        var databaseMemo = new DatabaseMemo()
        {
            SqlName = sqlDatabaseName,
            ClassName = database.ClassName ?? GetPascalCase(sqlDatabaseName),
        };

        await PopulateTypesAsync(sql, database, databaseMemo);
        await PopulateTableTypesAsync(sql, database, databaseMemo);
        await PopulateStatementsAsync(sql, database, databaseMemo);
        await PopulateProceduresAsync(sql, database, databaseMemo);

        return databaseMemo;
    }

    private static async Task PopulateTypesAsync(SqlConnection sql, DatabasesItem database, DatabaseMemo databaseMemo)
    {
        // TODO: OLD
        var allTypes = database.TODO_AllTypes = new();
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

        // TODO: NEW
        foreach (var type in database.TODO_AllTypes.Types)
        {
            databaseMemo.Types.Add(type.SqlTypeId, new()
            {
                SqlName = type.SqlName,
                SqlDbType = type.SqlDbType,
                SqlTypeId = type.SqlTypeId,
                DotnetType = type.DotnetType,
            });
        }
    }

    private static async Task PopulateTableTypesAsync(SqlConnection sql, DatabasesItem database, DatabaseMemo databaseMemo)
    {
        // TODO: OLD
        var tableTypes = await GetTableTypesForDatabaseAsync(sql);
        database.TableTypes = new() { Items = tableTypes };

        // TODO: NEW
        foreach (var tableType in tableTypes)
        {
            var schemaName = tableType.SchemaName;
            if (!databaseMemo.Schemas.TryGetValue(schemaName, out var schemaMemo)) { schemaMemo = databaseMemo.Schemas[schemaName] = new SchemaMemo() { SqlName = schemaName, ClassName = GetPascalCase(schemaName) }; }
            var tableTypeName = tableType.TypeName;

            var memo = new TableTypeMemo()
            {
                TypeName = tableType.TypeName,
                SchemaName = tableType.SchemaName,
                Columns = GetCommandColumns(databaseMemo, tableType.Columns),
                SqlTypeId = new() { SystemTypeId = tableType.SqlSystemTypeId, UserTypeId = tableType.SqlUserTypeId },
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
            PopulateRecordProperties(databaseMemo, recordMemo, tableType.Columns);
            schemaMemo.Records[memo.RowClassName] = recordMemo;
        }
    }

    private static async Task PopulateStatementsAsync(SqlConnection sql, DatabasesItem database, DatabaseMemo databaseMemo)
    {
        if (database.Statements?.Items is { } statements)
        {
            // TODO: OLD
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
                        var found = database.TODO_AllTypes.Types.First(i => i.SqlName == parameter.Type);
                        parameter.SqlDbType = found.SqlDbType;
                        parameter.SqlTypeId = found.SqlTypeId;
                    }
                }
            }

            // TODO: NEW
            foreach (var statement in statements)
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
                    PopulateRecordProperties(databaseMemo, recordMemo, statement.ResultSet.Columns);
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
                    Columns = GetCommandColumns(databaseMemo, columns),
                    IsNonQuery = isNonQuery,
                };
                databaseMemo.Statements[name] = memo;
            }
        }
    }

    private static async Task PopulateProceduresAsync(SqlConnection sql, DatabasesItem database, DatabaseMemo databaseMemo)
    {
        if (database.Procedures?.Items is { } procs)
        {
            // TODO: OLD
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

            // TODO: NEW
            foreach (var proc in procs)
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
                    PopulateRecordProperties(databaseMemo, recordMemo, proc.ResultSet.Columns);
                }
                else
                {
                    isNonQuery = true;
                    rowClassName = null;
                }

                var memo = new CommandMemo()
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = $"{database.SqlName}.{schemaName}.{name}",
                    MethodName = GetPascalCase(name),
                    Parameters = GetCommandParameters(schemaName, proc.Parameters?.Items ?? new List<Parameter>(), databaseMemo),
                    Columns = GetCommandColumns(databaseMemo, columns),
                    RowClassName = rowClassName,
                    // RowClassRef = $"{databaseMemo.ClassName}.{schemaMemo.ClassName}.{rowClassName}",
                    RowClassRef = rowClassName,
                    IsNonQuery = isNonQuery,
                };

                schemaMemo.Procedures[name] = memo;
            }
        }
    }

    private static void PopulateRecordProperties(DatabaseMemo databaseMemo, RecordMemo recordMemo, List<Column> columns)
    {
        var props = recordMemo.Properties;
        foreach (var column in columns)
        {
            var prop = new PropertyMemo
            {
                IsNullable = column.IsNullable,
                Name = GetPascalCase(column.Name),
                Type = GetDotnetType(databaseMemo, column.SqlTypeId),
            };
            props.Add(prop);
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
                SqlTypeId = i.SqlTypeId,
            };

            if (i.SqlDbType == SqlDbType.Structured)
            {
                var (schemaName, typeName) = Program.ParseSchemaItem(i.Type);
                schemaName ??= hostSchema;

                static TableTypeMemo? FindMatch(DatabaseMemo databaseMemo, String schemaName, Parameter i)
                {
                    foreach (var schema in databaseMemo.Schemas.Values)
                    {
                        if (schema.TableTypes.Values.FirstOrDefault(j => j.SqlTypeId == i.SqlTypeId) is { } tableType)
                        {
                            return tableType;
                        }
                    }
                    return null;
                }

                if (FindMatch(databaseMemo, schemaName, i) is not { } tableType)
                {
                    throw new InvalidOperationException($"Unable to find table type: {i.Type} ({i.SqlTypeId})");
                }

                tableType.IsReferenced = true;
                memo.ArgumentType = $"List<{tableType.RowClassRef}>";
                memo.ArgumentExpression = $"new {tableType.DataTableClassRef}({GetCamelCase(i.Name)})";
                memo.ParameterTableRef = $"{tableType.SchemaName}.{tableType.TypeName}";
            }
            else
            {
                memo.ArgumentType = GetShortestNameForType(GetDotnetType(databaseMemo, memo.SqlTypeId));
                memo.ArgumentExpression = GetCamelCase(i.Name);
            }

            memos.Add(memo);
        }

        return memos;
    }

    private static List<ColumnMemo> GetCommandColumns(DatabaseMemo database, List<Column> columns) =>
        columns.Select(i => (column: i, dotnetType: GetDotnetType(database, i.SqlTypeId))).Select(i => new ColumnMemo()
        {
            MaxLength = i.column.MaxLength,
            OrdinalVarName = $"ord{GetPascalCase(i.column.Name)}",
            ColumnName = i.column.Name,
            ColumnIsNullable = i.column.IsNullable,
            PropertyType = i.dotnetType,
            PropertyTypeName = GetStringForType(i.dotnetType, i.column.IsNullable),
            PropertyName = GetPascalCase(i.column.Name),
            FieldTypeName = GetShortestNameForType(i.dotnetType),
        }).ToList();

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

    private static Type GetDotnetType(DatabaseMemo database, SqlTypeId type)
    {
        if (!database.Types.TryGetValue(type, out var found))
        {
            throw new InvalidOperationException($"SQL Type not found: {type.SystemTypeId}, {type.UserTypeId}");
        }
        return found.DotnetType;
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
