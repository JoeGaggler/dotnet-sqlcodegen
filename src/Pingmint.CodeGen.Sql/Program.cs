using Pingmint.CodeGen.Sql.Model;
using Pingmint.CodeGen.Sql.Model.Yaml;
using Microsoft.Data.SqlClient;
using System.Data;

using static Pingmint.CodeGen.Sql.Globals;

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

    private static SqlConnection OpenSql(Config config)
    {
        var sql = new SqlConnection(config.Connection?.ConnectionString);
        sql.Open();
        return sql;
    }

    private static async Task<ConfigMemo> MetaAsync(Config config)
    {
        var configMemo = new ConfigMemo();

        var cs = config.CSharp ?? throw new NullReferenceException();
        configMemo.Namespace = cs.Namespace;
        configMemo.ClassName = cs.ClassName;

        using var sql = OpenSql(config);

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
        ArgumentException.ThrowIfNullOrEmpty(database.SqlName);
        var sqlDatabaseName = database.SqlName;

        var databaseMemo = new DatabaseMemo()
        {
            SqlName = sqlDatabaseName,
        };

        await PopulateSchemasAsync(sql, databaseMemo);
        await PopulateTypesAsync(sql, databaseMemo);
        await PopulateStatementsAsync(sql, database, databaseMemo);
        await PopulateProceduresAsync(sql, database, databaseMemo);

        return databaseMemo;
    }

    private static async Task PopulateSchemasAsync(SqlConnection sql, DatabaseMemo databaseMemo)
    {
        foreach (var schema in await Proxy.GetSchemasAsync(sql))
        {
            int schemaId = schema.SchemaId;
            string name = schema.Name;
            var memo = new SchemaMemo
            {
                SqlName = name,
                SchemaId = schemaId,
                ClassName = GetPascalCase(name),
            };
            databaseMemo.Schemas[schemaId] = memo;
        }
    }

    private static async Task PopulateTypesAsync(SqlConnection sql, DatabaseMemo databaseMemo)
    {
        var tableTypes = (await Proxy.GetTableTypesAsync(sql)).ToDictionary(i => i.GetSqlTypeId());

        foreach (var type in await Proxy.GetSysTypesAsync(sql))
        {
            SqlDbType sqlDbType;
            Type? dotnetType;
            if (!type.IsUserDefined)
            {
                try
                {
                    sqlDbType = GetSqlDbType(type.Name);
                    dotnetType = GetDotnetType(sqlDbType);
                }
                catch
                {
                    // not implemented: image
                    // not implemented: sql_variant
                    // not implemented: smallmoney
                    // not implemented: hierarchyid
                    // not implemented: geometry
                    // not implemented: geography
                    // not implemented: timestamp
                    continue; // TODO: type not supported
                }
            }
            else if (tableTypes.TryGetValue(type.GetSqlTypeId(), out var tableType))
            {
                sqlDbType = SqlDbType.Structured;
                dotnetType = null;
                if (!databaseMemo.Schemas.TryGetValue(tableType.SchemaId, out var schemaMemo))
                {
                    throw new InvalidOperationException($"Missing schema {tableType.SchemaId} for table type: {tableType.SchemaName}.{tableType.Name}");
                }
                var tableTypeName = tableType.Name;

                var columns = new List<Column>();
                foreach (var col in await Proxy.GetTableTypeColumnsAsync(sql, tableType.TypeTableObjectId))
                {
                    var column = new Column
                    {
                        Name = col.Name ?? throw new NullReferenceException(),
                        Type = GetSqlDbType(col.TypeName), // TODO: what if this is also a table type?
                        IsNullable = col.IsNullable ?? true,
                        MaxLength = col.MaxLength,
                        SqlTypeId = col.GetSqlTypeId(),
                    };
                    columns.Add(column);
                }

                var memo = new TableTypeMemo
                {
                    TypeName = tableTypeName,
                    SchemaName = tableType.SchemaName,
                    Columns = GetCommandColumns(databaseMemo, columns),
                    SqlTypeId = tableType.GetSqlTypeId(),
                    RowClassName = GetPascalCase(tableTypeName) + "Row",
                    DataTableClassName = GetPascalCase(tableTypeName) + "RowDataTable"
                };
                memo.RowClassRef = memo.RowClassName;
                memo.DataTableClassRef = memo.DataTableClassName;

                schemaMemo.TableTypes[memo.SqlTypeId] = memo;

                var recordMemo = new RecordMemo
                {
                    Name = memo.RowClassName,
                    ParentTableType = memo,
                };
                PopulateRecordProperties(databaseMemo, recordMemo, columns);
                schemaMemo.Records[memo.RowClassName] = recordMemo;
            }
            else
            {
                throw new InvalidOperationException($"unknown type: {type.Name} - {type.GetSqlTypeId()}");
            }

            var sqlTypeId = type.GetSqlTypeId();

            databaseMemo.Types.Add(sqlTypeId, new TypeMemo()
            {
                SqlName = type.Name,
                SqlDbType = sqlDbType,
                SqlTypeId = sqlTypeId,
                DotnetType = dotnetType,
            });
        }
    }

    private static async Task PopulateStatementsAsync(SqlConnection sql, DatabasesItem database, DatabaseMemo databaseMemo)
    {
        if (database.Statements?.Items is not { } statements) { return; }

        foreach (var statement in statements)
        {
            if (statement.Parameters?.Items is { } parameters)
            {
                foreach (var parameter in parameters)
                {
                    var found = databaseMemo.Types.Values.First(i => i.SqlName == parameter.Type);
                    parameter.SqlTypeId = found.SqlTypeId;
                }
            }
            else
            {
                parameters = new();
            }

            var parametersString = parameters.Count == 0 ? null : string.Join(", ", parameters.Select(i => $"@{i.Name} {i.Type ?? "null"}"));
            var columns = GetColumnsForResultSet(await Proxy.DmDescribeFirstResultSetAsync(sql, statement.Text, parametersString));
            var name = statement.Name ?? throw new NullReferenceException();
            var commandText = statement.Text ?? throw new NullReferenceException();

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
                PopulateRecordProperties(databaseMemo, recordMemo, columns);
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
                MethodName = name,
                RowClassName = rowClassName,
                RowClassRef = rowClassName,
                Parameters = GetCommandParameters(null, parameters, databaseMemo),
                Columns = GetCommandColumns(databaseMemo, columns),
                IsNonQuery = isNonQuery,
            };
            databaseMemo.Statements[name] = memo;
        }
    }

    private static async Task PopulateProceduresAsync(SqlConnection sql, DatabasesItem database, DatabaseMemo databaseMemo)
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
            foreach (var exclude in excludeSchemaProcList)
            {
                if (procSchema != exclude.Schema) continue;
                if (procName == exclude.Name) return true;
                if (exclude.Name == "*") return true;
            }
            return false;
        }


        if (database.Procedures?.Included is not { } procs) { return; }

        foreach (var proc in procs)
        {
            var (schema, procName) = ParseSchemaItem(proc.Text);
            if (schema is null) { throw new InvalidOperationException($"Unable to parse schema item: {proc.Text}"); }

            if (procName == "*")
            {
                foreach (var row in await Proxy.GetProceduresForSchemaAsync(sql, schema))
                {
                    if (IsExcluded(schema, row.Name)) { continue; }
                    var newProc = new Procedure();
                    await MetaProcAsync(sql, newProc, schema, row.Name, databaseMemo);
                }
            }
            else
            {
                if (IsExcluded(schema, procName)) { continue; }
                await MetaProcAsync(sql, proc, schema, procName, databaseMemo);
            }

            static async Task MetaProcAsync(SqlConnection sql, Procedure proc, string schema, string procName, DatabaseMemo databaseMemo)
            {
                if ((await Proxy.GetProcedureForSchemaAsync(sql, schema, procName)).FirstOrDefault() is not { } me)
                {
                    throw new InvalidOperationException($"Unable to find procedure: {proc.Text}");
                }

                var iii = (await Proxy.GetParametersForObjectAsync(sql, me.ObjectId)).Select(i => new Parameter()
                {
                    Name = i.Name?.TrimStart('@') ?? throw new NullReferenceException(),
                    Type = i.TypeName,
                    IsTableType = i.IsTableType,
                    MaxLength = i.MaxLength,
                    SqlTypeId = i.GetSqlTypeId(),
                }).ToList();
                proc.Parameters = new() { Items = iii };

                // TODO: go straight to memo
                proc.Columns = GetColumnsForResultSet(await Proxy.DmDescribeFirstResultSetForObjectAsync(sql, me.ObjectId));

                var schemaMemo = databaseMemo.Schemas.Values.First(i => i.SqlName == schema); // TODO: O(n)

                var name = procName ?? throw new NullReferenceException();
                var columns = proc.Columns ?? throw new NullReferenceException();

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
                    PopulateRecordProperties(databaseMemo, recordMemo, proc.Columns);
                }
                else
                {
                    isNonQuery = true;
                    rowClassName = null;
                }

                var memo = new CommandMemo()
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = $"{databaseMemo.SqlName}.{schema}.{name}",
                    MethodName = GetPascalCase(name),
                    Parameters = GetCommandParameters(schema, proc.Parameters?.Items ?? new List<Parameter>(), databaseMemo),
                    Columns = GetCommandColumns(databaseMemo, columns),
                    RowClassName = rowClassName,
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
                ArgumentName = GetCamelCase(i.Name),
                MaxLength = i.MaxLength,
                SqlTypeId = i.SqlTypeId,
            };

            if (i.IsTableType)
            {
                var (schemaName, typeName) = Program.ParseSchemaItem(i.Type);
                schemaName ??= hostSchema;

                static TableTypeMemo? FindMatch(DatabaseMemo databaseMemo, String schemaName, Parameter i)
                {
                    var schemaMemo = databaseMemo.Schemas[i.SqlTypeId.SchemaId];
                    if (schemaMemo.TableTypes.Values.FirstOrDefault(j => j.SqlTypeId == i.SqlTypeId) is { } tableType)
                    {
                        return tableType;
                    }
                    return null;
                }

                if (FindMatch(databaseMemo, schemaName, i) is not { } tableType)
                {
                    throw new InvalidOperationException($"Unable to find table type: {i.Type} ({i.SqlTypeId})");
                }

                tableType.IsReferenced = true;
                memo.MaxLength = null;
                memo.ParameterType = SqlDbType.Structured;
                memo.ArgumentType = $"List<{tableType.RowClassRef}>";
                memo.ArgumentExpression = $"new {tableType.DataTableClassRef}({GetCamelCase(i.Name)})";
                memo.ParameterTableRef = $"{tableType.SchemaName}.{tableType.TypeName}";
            }
            else
            {
                var dotnetType = GetDotnetType(databaseMemo, memo.SqlTypeId);
                if (dotnetType != typeof(String)) { memo.MaxLength = null; }

                memo.ParameterType = databaseMemo.Types[i.SqlTypeId].SqlDbType;
                memo.ArgumentType = GetShortestNameForType(dotnetType);
                memo.ArgumentExpression = GetCamelCase(i.Name);
            }

            memos.Add(memo);
        }

        return memos;
    }

    private static List<ColumnMemo> GetCommandColumns(DatabaseMemo database, List<Column> columns) =>
        columns.Select(i => (column: i, dotnetType: GetDotnetType(database, i.SqlTypeId))).Select(i => new ColumnMemo()
        {
            MaxLength = i.dotnetType == typeof(String) ? i.column.MaxLength : null,
            OrdinalVarName = $"ord{GetPascalCase(i.column.Name)}",
            ColumnName = i.column.Name,
            ColumnIsNullable = i.column.IsNullable,
            PropertyType = i.dotnetType,
            PropertyTypeName = GetStringForType(i.dotnetType, i.column.IsNullable),
            PropertyName = GetPascalCase(i.column.Name),
            FieldTypeName = GetShortestNameForType(i.dotnetType),
        }).ToList();

    private static List<Column> GetColumnsForResultSet<T>(List<T> resultSet) where T : IDmDescribeFirstResultSetRow =>
        resultSet.Select((i, index) => new Column()
        {
            Name = i.Name ?? $"Column{index}",
            Type = GetSqlDbType(i.SqlTypeName),
            // Type = i.IsTableType ? SqlDbType.Structured : GetSqlDbType(i.TypeName),
            IsNullable = i.IsNullable.GetValueOrDefault(true), // nullable by default
            SqlTypeId = i.GetSqlTypeId(),
        }).ToList();

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

    private static Type? GetDotnetType(DatabaseMemo database, SqlTypeId type)
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

        _ => throw new InvalidOperationException("Unexpected SQL type name: " + sqlTypeName),
    };
}
