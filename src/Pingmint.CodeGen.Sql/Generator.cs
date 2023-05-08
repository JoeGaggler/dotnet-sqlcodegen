using Pingmint.CodeGen.Sql.Model;
using System.Data;

using static Pingmint.CodeGen.Sql.Globals;

namespace Pingmint.CodeGen.Sql;

public static class Generator
{
    private static void CodeRecords(CodeWriter code, SortedDictionary<String, DatabaseMemo> databaseScopeMemos)
    {
        foreach (var dbItem in databaseScopeMemos)
        {
            var db = dbItem.Value;

            foreach (var schemaItem in db.Schemas)
            {
                var schema = schemaItem.Value;

                foreach (var record in schema.Records.Values)
                {
                    if (record.ParentTableType is { } tableType && !tableType.IsReferenced) { continue; }
                    CodeRecord(code, record);

                    code.Line();
                }

                foreach (var tableType in schema.TableTypes.Values)
                {
                    if (!tableType.IsReferenced) { continue; }
                    CodeRecord(code, tableType);

                    code.Line();
                }
            }

            foreach (var recordItem in db.Records)
            {
                var record = recordItem.Value;
                if (record.ParentTableType is { } tableType && !tableType.IsReferenced) { continue; }
                CodeRecord(code, record);

                code.Line();
            }
        }
    }

    private static void CodeRecord(CodeWriter code, RecordMemo recordMemo)
    {
        using (code.PartialClass("public", recordMemo.Name))
        {
            foreach (var prop in recordMemo.Properties)
            {
                var typeString = GetStringForType(prop.Type, prop.IsNullable);
                code.Line($"public {typeString} {prop.Name} {{ get; set; }}");
            }
        }
    }

    private static void CodeRecord(CodeWriter code, TableTypeMemo memo)
    {
        var dataTableClassName = memo.DataTableClassName ?? throw new NullReferenceException();
        var rowClassName = memo.RowClassName ?? throw new NullReferenceException();

        using (code.PartialClass("public sealed", dataTableClassName, "DataTable"))
        {
            code.Line("public {0}() : this(new List<{1}>()) {{ }}", dataTableClassName, rowClassName);
            code.Line("public {0}(List<{1}> rows) : base()", dataTableClassName, rowClassName);
            using (code.CreateBraceScope())
            {
                code.Line("ArgumentNullException.ThrowIfNull(rows);");
                code.Line();
                foreach (var col in memo.Columns)
                {
                    var allowDbNull = col.ColumnIsNullable ? "true" : "false";
                    var maxLength = (col.MaxLength is short s) ? $", MaxLength = {s}" : String.Empty; // the default is already "-1", so we do not have to emit this in code.
                    code.Line("base.Columns.Add(new DataColumn() {{ ColumnName = \"{0}\", DataType = typeof({1}), AllowDBNull = {2}{3} }});", col.ColumnName, col.PropertyTypeName.TrimEnd('?'), allowDbNull, maxLength);
                }
                using (code.ForEach("var row in rows"))
                {
                    var parameterBuilder = String.Empty;
                    foreach (var col in memo.Columns)
                    {
                        var localName = GetCamelCase(col.PropertyName);
                        var propName = col.PropertyName;

                        if (col.PropertyType == typeof(String) && col.MaxLength is { } maxLength && maxLength > 1)
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

    // TODO: remove YAML Config parameter
    public static void Generate(ConfigMemo configMemo, CodeWriter code)
    {
        var className = configMemo.ClassName ?? throw new NullReferenceException();
        var fileNs = configMemo.Namespace ?? throw new NullReferenceException();

        var databaseMemos = configMemo.Databases;

        code.UsingNamespace("System");
        code.UsingNamespace("System.Collections.Generic");
        code.UsingNamespace("System.Data");
        code.UsingNamespace("System.Threading");
        code.UsingNamespace("System.Threading.Tasks");
        code.UsingNamespace("Microsoft.Data.SqlClient");
        code.Line();
        code.FileNamespace(fileNs);
        code.Line();

        // TODO: reconsider interface
        // code.Line("public partial interface I{0}", className);
        // using (code.CreateBraceScope())
        // {
        //     foreach (var databaseMemo in databaseMemos.Values)
        //     {
        //         foreach (var schemaMemo in databaseMemo.Schemas.Values)
        //         {
        //             foreach (var procMemo in schemaMemo.Procedures.Values)
        //             {
        //                 CodeSqlStatementInterface(code, procMemo);
        //             }
        //         }

        //         foreach (var memo in databaseMemo.Statements)
        //         {
        //             CodeSqlStatementInterface(code, memo.Value);
        //         }
        //     }
        // }
        // code.Line();

        CodeRecords(code, databaseMemos);

        // CodeRecords already emits a blank line
        //code.Line();

        // using (code.PartialClass("public", className, "I" + className)) // TODO: reconsider implements
        using (code.PartialClass("public", className))
        {
            WriteConstructor(code, className);
            code.Line();

            // TODO: reconsider instance methods
            // foreach (var databaseMemo in databaseMemos.Values)
            // {
            //     foreach (var schemaMemo in databaseMemo.Schemas.Values)
            //     {
            //         foreach (var procMemo in schemaMemo.Procedures.Values)
            //         {
            //             CodeSqlStatementInstance(code, procMemo);
            //         }
            //     }

            //     foreach (var memo in databaseMemo.Statements)
            //     {
            //         CodeSqlStatementInstance(code, memo.Value);
            //     }
            // }
            // code.Line();

            WriteHelperMethods(code);
            code.Line();

            foreach (var databaseMemo in databaseMemos.Values)
            {
                foreach (var schemaMemo in databaseMemo.Schemas.Values)
                {
                    foreach (var procMemo in schemaMemo.Procedures.Values)
                    {
                        CodeSqlStatement(code, procMemo);
                        code.Line();
                    }
                }

                foreach (var memo in databaseMemo.Statements)
                {
                    CodeSqlStatement(code, memo.Value);
                    code.Line();
                }
            }
        }
    }

    private static void WriteConstructor(CodeWriter code, String className)
    {
        // TODO: reconsider instance methods
        // code.Line("private readonly Func<Task<SqlConnection>> connectionFunc;");
        // code.Line();
        // code.Line("public {0}({1})", className, "Func<Task<SqlConnection>> connectionFunc");
        // using (code.CreateBraceScope())
        // {
        //     code.Line("this.connectionFunc = connectionFunc;");
        // }
    }

    private static void WriteHelperMethods(CodeWriter code)
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
    }

    private struct StatementSignature
    {
        public String MethodName;
        public String ResultType;
        public String ParametersRaw;
        public String Parameters;
        public String ArgumentsRaw;
        public String Arguments;
        public String? RowClassRef;
    }

    private static StatementSignature GetStatementSignature(CommandMemo commandMemo)
    {
        String resultType;
        if (!commandMemo.IsNonQuery && commandMemo.RowClassName is { } rowClassName)
        {
            resultType = String.Format("List<{0}>", rowClassName);
        }
        else
        {
            resultType = "Int32";
            rowClassName = null;
        }
        var methodName = (commandMemo.MethodName ?? throw new NullReferenceException());

        var prms1 = "";
        var args1 = "";
        var prms = "connection";
        var args = "SqlConnection connection"; // TODO: parameters, then transaction
        if (commandMemo.Parameters is { } parameters && parameters.Count > 0)
        {
            prms1 = String.Join(", ", parameters?.Select(i => $"{i.ArgumentName}"));
            args1 = String.Join(", ", parameters?.Select(i => $"{i.ArgumentType} {i.ArgumentName}"));
            prms += ", " + prms1;
            args += ", " + args1;
        }

        return new()
        {
            MethodName = methodName,
            ResultType = resultType,
            Parameters = prms,
            ParametersRaw = prms1,
            Arguments = args,
            ArgumentsRaw = args1,
            RowClassRef = rowClassName,
        };
    }

    private static void CodeSqlStatement(CodeWriter code, CommandMemo commandMemo)
    {
        var commandText = commandMemo.CommandText ?? throw new NullReferenceException();

        var sig = GetStatementSignature(commandMemo);
        var resultType = sig.ResultType;
        var args = sig.Arguments;
        var prms = sig.Parameters;
        var rowClassRef = sig.RowClassRef;

        foreach (var isAsync in new[] { true, false })
        {
            var returnType = isAsync ? $"Task<{resultType}>" : resultType;
            var methodName = isAsync ? sig.MethodName + "Async" : sig.MethodName;
            var asyncKeyword = isAsync ? " async" : "";

            // Async only: generate convenience method without cancellation token
            var prmsWithCancellationToken = isAsync ? prms + ", CancellationToken.None" : prms;
            if (isAsync)
            {
                code.Line("public static {0} {1}({2}) => {1}({3});", returnType, methodName, args, prmsWithCancellationToken);
            }

            var argsWithCancellationToken = isAsync ? args + ", CancellationToken cancellationToken" : args;
            using (code.Method($"public static{asyncKeyword}", returnType, methodName, argsWithCancellationToken))
            {
                var commandType = commandMemo.CommandType switch
                {
                    CommandType.Text => CommandType.Text.ToString(),
                    CommandType.StoredProcedure => CommandType.StoredProcedure.ToString(),
                    var x => throw new InvalidOperationException($"unsupported command type: {x}")
                };

                var commandMethod = commandMemo.CommandType switch
                {
                    CommandType.Text => "CreateStatement",
                    CommandType.StoredProcedure => "CreateStoredProcedure",
                    var x => throw new InvalidOperationException($"unsupported command type: {x}")
                };

                code.Line($"using SqlCommand cmd = {commandMethod}(connection, \"{commandText}\");");
                code.Line();

                if (commandMemo.Parameters is { } parameters2 && parameters2.Count > 0) // TODO: reuse parameters
                {
                    foreach (var parameter in parameters2)
                    {
                        var withTableTypeName = (parameter.ParameterTableRef is String tableTypeName) ? $", \"{tableTypeName}\"" : "";
                        var withSize = (parameter.MaxLength is { } maxLength and not -1) ? $", {maxLength}" : "";
                        code.Line($"cmd.Parameters.Add(CreateParameter(\"@{parameter.ParameterName.TrimStart('@')}\", {parameter.ArgumentExpression}, SqlDbType.{parameter.ParameterType}{withTableTypeName}{withSize}));");
                    }
                    code.Line();
                }

                if (commandMemo.IsNonQuery)
                {
                    if (isAsync)
                    {
                        code.Return("await cmd.ExecuteNonQueryAsync(cancellationToken)");
                    }
                    else
                    {
                        code.Return("cmd.ExecuteNonQuery()");
                    }
                }
                else if (commandMemo.Columns is { } columns && columns.Count > 0 && rowClassRef is { } rowClassRef2)
                {
                    code.Line("var result = new {0}();", resultType);

                    if (isAsync)
                    {
                        code.Line("using var reader = await cmd.ExecuteReaderAsync(cancellationToken);");
                    }
                    else
                    {
                        code.Line("using var reader = cmd.ExecuteReader();");
                    }

                    IDisposable ifReader =
                        isAsync ?
                        code.If("await reader.ReadAsync(cancellationToken)") :
                        code.If("reader.Read()");
                    using (ifReader)
                    {
                        foreach (var column in commandMemo.Columns)
                        {
                            code.Line("int {0} = reader.GetOrdinal(\"{1}\");", column.OrdinalVarName, column.ColumnName);
                        }

                        code.Line();

                        IDisposable doWhileReader =
                            isAsync ?
                            code.DoWhile("await reader.ReadAsync(cancellationToken)") :
                            code.DoWhile("reader.Read()");
                        using (doWhileReader)
                        {
                            code.Line("result.Add(new {0}", rowClassRef2);
                            using (code.CreateBraceScope(null, ");"))
                            {
                                foreach (var column in commandMemo.Columns)
                                {
                                    var line = (column.PropertyType.IsValueType, column.ColumnIsNullable) switch
                                    {
                                        (false, true) => String.Format("{0} = GetField<{2}>(reader, {1}),", column.PropertyName, column.OrdinalVarName, column.FieldTypeName),
                                        (true, true) => String.Format("{0} = GetFieldValue<{2}>(reader, {1}),", column.PropertyName, column.OrdinalVarName, column.FieldTypeName),
                                        (false, false) => String.Format("{0} = GetNonNullField<{2}>(reader, {1}),", column.PropertyName, column.OrdinalVarName, column.FieldTypeName),
                                        (true, false) => String.Format("{0} = GetNonNullFieldValue<{2}>(reader, {1}),", column.PropertyName, column.OrdinalVarName, column.FieldTypeName),
                                    };
                                    code.Line(line);
                                }
                            }
                        }
                    }
                    code.Return("result");
                }
                else
                {
                    throw new InvalidOperationException("unable to determine expected results");
                }
            }

            // HACK: put line break between async and sync methods
            if (isAsync)
            {
                code.Line();
            }
        }
    }
}
