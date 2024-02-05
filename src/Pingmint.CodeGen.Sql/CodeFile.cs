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
                    code.Line("public required {0} {1} {{ get; init; }}", property.FieldType, property.FieldName);
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

	private static List<T> ExecuteCommand<T, O>(SqlCommand cmd, Func<SqlDataReader, O> ordinals, Func<SqlDataReader, O, T> readRow)
	{
		var result = new List<T>();
		using var reader = cmd.ExecuteReader();
		if (!reader.Read()) { return result; }
		var ords = ordinals(reader);
		do { result.Add(readRow(reader, ords)); } while (reader.Read());
		return result;
	}

	private static async Task<List<T>> ExecuteCommandAsync<T, O>(SqlCommand cmd, Func<SqlDataReader, O> ordinals, Func<SqlDataReader, O, T> readRow)
	{
		var result = new List<T>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (!await reader.ReadAsync().ConfigureAwait(false)) { return result; }
		var ords = ordinals(reader);
		do { result.Add(readRow(reader, ords)); } while (await reader.ReadAsync().ConfigureAwait(false));
		return result;
	}

""");
            code.Line();
            var isFirstMethod = true;
            foreach (var method in Methods.OrderBy(i => i.Name))
            {
                if (isFirstMethod) { isFirstMethod = false; }

                var commandText = method.CommandText.ReplaceLineEndings(" ").Trim();
                var csharpParameters = method.CSharpParameters;
                var commandParameters = method.SqlParameters;

                Boolean firstFlag = true;
                foreach (var isAsync in new[] { false, true })
                {
                    // HACK
                    var isFirst = firstFlag;
                    firstFlag = false;

                    var methodParametersWithConnection = csharpParameters.ToList(); // NOTE THE COPY!
                    var methodParametersWithCommand = csharpParameters.ToList(); // NOTE THE COPY!
                    methodParametersWithConnection.Insert(0, new MethodParameter() { CSharpType = "SqlConnection", CSharpName = "connection" });
                    methodParametersWithCommand.Insert(0, new MethodParameter() { CSharpType = "SqlCommand", CSharpName = "cmd" });
                    var parametersString = String.Join(", ", methodParametersWithConnection.Select(i => $"{i.CSharpType} {i.CSharpName}"));
                    var argumentsString = String.Join(", ", methodParametersWithConnection.Select(i => $"{i.CSharpName}"));

                    var asyncKeyword = isAsync ? " async" : "";
                    var returnType = isAsync ? $"Task<{method.DataType}>" : method.DataType;

                    // Ordinals/ReadRow/Command, which are shared between sync and async methods
                    if (isFirst)
                    {
                        if (method.ResultSetRecord is { } record)
                        {
                            var tuples = "(" + String.Join(", ", record.Properties.Select(i => "int")) + ")";
                            code.StartMethod($"private static", tuples, method.Name + "Ordinals", "SqlDataReader reader");
                            code.Text($" => (");
                            code.Line();
                            code.Indent();
                            var comma = "";
                            foreach (var recordProperty in record.Properties)
                            {
                                code.Line($"{comma}reader.GetOrdinal(\"{recordProperty.ColumnName}\")");
                                comma = ", ";
                            }
                            code.Dedent();
                            code.Line(");");
                            code.Line();

                            code.StartMethod($"private static{asyncKeyword}", record.CSharpName, method.Name + "ReadRow", $"SqlDataReader reader, {tuples} ords");

                            code.Text($" => new {record.CSharpName}");
                            code.Line();
                            using (code.CreateBraceScope(preamble: null, withClosingBrace: ";"))
                            {
                                int i = 1;
                                foreach (var property in record.Properties)
                                {
                                    var fieldName = property.FieldName;
                                    var IsValueType = property.FieldTypeIsValueType;
                                    var ColumnIsNullable = property.ColumnIsNullable;
                                    var fieldTypeForGeneric = property.FieldTypeForGeneric;
                                    var columnName = property.ColumnName;
                                    var ordinalVarName = $"ords.Item{i++}";
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
                            code.Line();
                        }

                        // Command
                        var cmdMethod = method.IsStoredProc ? "CreateStoredProcedure" : "CreateStatement";
                        if (commandParameters.Count == 0)
                        {
                            code.MethodExpression($"private static", "SqlCommand", method.Name + "Command", parametersString, $"{cmdMethod}(connection, \"{commandText}\")");
                        }
                        else
                        {
                            using var _2 = code.Method($"private static", "SqlCommand", method.Name + "Command", parametersString);
                            code.Line($"var cmd = {cmdMethod}(connection, \"{commandText}\");");
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
                        code.Line();
                    }

                    var actualMethodName = isAsync ? $"{method.Name}Async" : method.Name;

                    if (method.HasResultSet)
                    {
                        if (method.ResultSetRecord is not { } record) { throw new InvalidOperationException("Method has result set but no record type."); }
                        using (var _2 = code.Method($"public static{asyncKeyword}", returnType, actualMethodName, parametersString))
                        {
                            code.Line($"using var cmd = {method.Name}Command({argumentsString});");
                            code.Line("return " + (isAsync ? "await " : "") + "ExecuteCommand" + (isAsync ? "Async" : "") + "(cmd, " + method.Name + "Ordinals, " + method.Name + "ReadRow)" + (isAsync ? ".ConfigureAwait(false)" : "") + ";");
                        }
                        code.Line();
                    }
                    else if (method.HasResultSet == false)
                    {
                        if (method.DataType != "int") { throw new InvalidOperationException("Method has no result set but return type is not 'int'."); }

                        String expressionBody;
                        if (isAsync)
                        {
                            expressionBody = $"await cmd.ExecuteNonQueryAsync().ConfigureAwait(false)";
                        }
                        else
                        {
                            expressionBody = "cmd.ExecuteNonQuery()";
                        }
                        code.MethodExpression($"private static{asyncKeyword}", returnType, actualMethodName, "SqlCommand cmd", expressionBody);
                        code.Line();
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
