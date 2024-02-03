//using Pingmint.CodeGen.Sql.Refactor;
using System.Data;
using static Pingmint.CodeGen.Sql.Globals;

namespace Pingmint.CodeGen.Sql;

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

    /// <summary>
    /// All the type names used in this file, so we can avoid collisions
    /// </summary>
    public HashSet<String> TypeNames { get; } = new();

    public List<Record> Records { get; } = new();
    public List<Method> Methods { get; } = new();

    public String TypeKeyword { get; set; } = "record class";

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

        code.Line("#nullable enable");
        code.Line();

        code.FileNamespace(Namespace);
        code.Line();

        var isFirstRecord = true;
        foreach (var record in Records.OrderBy(i => i.CSharpName).ThenBy(i => i.TableTypeCSharpName))
        {
            if (isFirstRecord) { isFirstRecord = false; }
            else { code.Line(); }

            string? rowClassName = record.CSharpName;

            IDisposable recordClass = this.TypeKeyword switch
            {
                "class" => code.PartialClass("public", rowClassName),
                "record class" => code.PartialRecordClass("public", rowClassName),
                "record struct" => code.PartialRecordStruct("public", rowClassName),
                _ => throw new NotImplementedException("Unknown type keyword: " + this.TypeKeyword ?? "null" + ". Expected: 'class' or 'record class' or 'record struct'."),
            };
            using (recordClass)
            {
                foreach (var property in record.Properties)
                {
                    code.Line("  public required {0} {1} {{ get; init; }}", property.FieldType, property.FieldName);
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
                        var propertyTypeName = col.FieldTypeForGeneric;
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

        code.Line();
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
            code.Line();
            var isFirstMethod = true;
            foreach (var method in Methods.OrderBy(i => i.Name))
            {
                if (isFirstMethod) { isFirstMethod = false; }
                else { code.Line(); }

                var commandText = method.CommandText.ReplaceLineEndings(" ").Trim();
                var csharpParameters = method.CSharpParameters;
                var commandParameters = method.SqlParameters;

                foreach (var isAsync in new[] { false, true })
                {
                    if (isAsync) { code.Line(); } // Assumes async always comes after sync

                    var methodParameters = csharpParameters.ToList(); // NOTE THE COPY!
                    methodParameters.Insert(0, new MethodParameter() { CSharpType = "SqlConnection", CSharpName = "connection" });
                    var parametersString = String.Join(", ", methodParameters.Select(i => $"{i.CSharpType} {i.CSharpName}"));
                    var argumentsString = String.Join(", ", methodParameters.Select(i => $"{i.CSharpName}"));

                    var asyncKeyword = isAsync ? " async" : "";
                    var returnType = isAsync ? $"Task<{method.DataType}>" : method.DataType;
                    var actualMethodName = isAsync ? $"{method.Name}Async" : method.Name;

                    if (!isAsync) // command comes before sync method
                    {
                        using (var _2 = code.Method($"public static", "SqlCommand", method.Name + "Command", parametersString))
                        {
                            var cmdMethod = method.IsStoredProc ? "CreateStoredProcedure" : "CreateStatement";

                            if (commandParameters.Count == 0)
                            {
                                code.Return($"{cmdMethod}(connection, \"{commandText}\")");
                            }
                            else
                            {
                                code.Line($"SqlCommand cmd = {cmdMethod}(connection, \"{commandText}\");");
                                code.Line($"cmd.Parameters.AddRange([");
                                code.Indent();
                                foreach (var parameter in commandParameters)
                                {
                                    var withTableTypeName = (parameter.SqlTypeName is String tableTypeName) ? $", \"{tableTypeName}\"" : "";
                                    var needsSize = parameter.SqlDbType switch
                                    {
                                        SqlDbType.Xml or
                                        SqlDbType.Text or
                                        SqlDbType.Char or
                                        SqlDbType.NChar or
                                        SqlDbType.VarChar or
                                        SqlDbType.NVarChar or
                                        SqlDbType.Binary or
                                        SqlDbType.VarBinary => true,

                                        _ => false,
                                    };
                                    var withSize = (needsSize && parameter.MaxLength is { } maxLength and not -1) ? $", {maxLength}" : "";
                                    code.Line($"CreateParameter(\"@{parameter.SqlName}\", {parameter.CSharpExpression}, SqlDbType.{parameter.SqlDbType}{withTableTypeName}{withSize}),");
                                }
                                code.Dedent();
                                code.Line("]);");
                                code.Return("cmd");
                            }
                        }
                        code.Line();
                    }

                    using var _ = code.Method($"public static{asyncKeyword}", returnType, actualMethodName, parametersString);
                    code.Line($"using var cmd = {method.Name}Command({argumentsString});");

                    if (method.HasResultSet)
                    {
                        if (method.ResultSetRecord is not { } record) { throw new InvalidOperationException("Method has result set but no record type."); }

                        code.Line($"var result = new List<{record.CSharpName}>();");

                        IDisposable ifScope;
                        if (isAsync)
                        {
                            code.Line($"using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);");
                            ifScope = code.If("await reader.ReadAsync().ConfigureAwait(false)");
                        }
                        else
                        {
                            code.Line($"using var reader = cmd.ExecuteReader();");
                            ifScope = code.If("reader.Read()");
                        }

                        using (ifScope)
                        {
                            if (record.Properties.Count != 0)
                            {
                                foreach (var recordProperty in record.Properties)
                                {
                                    String columnName = recordProperty.ColumnName;
                                    code.Line($"int ord{GetPascalCase(columnName)} = reader.GetOrdinal(\"{columnName}\");");
                                }
                                code.Line();
                            }

                            IDisposable doWhileScope;
                            if (isAsync)
                            {
                                doWhileScope = code.DoWhile("await reader.ReadAsync().ConfigureAwait(false)");
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

                        code.Return("result");
                    }
                    else if (method.HasResultSet == false)
                    {
                        if (method.DataType != "int") { throw new InvalidOperationException("Method has no result set but return type is not 'int'."); }

                        if (isAsync)
                        {
                            code.Line($"return await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);");
                        }
                        else
                        {
                            code.Line($"return cmd.ExecuteNonQuery();");
                        }
                    }
                    else
                    {
                        throw new NotImplementedException("Unknown method return type: " + method.DataType ?? "null");
                    }
                }
            }
        }

        return code.ToString();
    }
}
