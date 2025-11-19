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
    public List<Constant> Constants { get; } = new();

    public SortedSet<int> OrdinalAliases { get; } = new();

    public String TypeKeyword { get; set; } = "record class";
    public Boolean AllowAsync { get; set; } = true;
    public Boolean TrimChar { get; set; } = false;

    public String GenerateCode()
    {
        var code = new CodeWriter();

        code.UsingNamespace("System");
        code.UsingNamespace("System.Collections.Generic");
        code.UsingNamespace("System.Data");
        code.UsingNamespace("System.Threading");
        code.UsingNamespace("System.Threading.Tasks");
        code.UsingNamespace("Microsoft.Data.SqlClient");
        code.Line($"using static {Namespace}.FileMethods;");
        code.Line();

        code.Line("#nullable enable");
        code.Line();

        code.FileNamespace(Namespace);
        code.Line();

        if (OrdinalAliases.Count > 0)
        {
            if (OrdinalAliases.Remove(1))
            {
                code.Line("using Ordinals1 = int;");
            }
            foreach (var ordinal in OrdinalAliases)
            {
                code.StartLine();
                code.Text($"record struct Ordinals{ordinal}(int Item1");
                for (int i = 2; i <= ordinal; i++)
                {
                    code.Text($", int Item{i}");
                }
                code.Text(");");
                code.Line();
            }
            code.Line();
        }

        var isFirstRecord = true;
        foreach (var record in Records.OrderBy(i => i.CSharpName).ThenBy(i => i.TableTypeCSharpName))
        {
            if (isFirstRecord) { isFirstRecord = false; }
            else { code.Line(); }

            string? rowClassName = record.CSharpName;
            var (ordinalsArg, tupleType) = record.Properties.Count switch
            {
                1 => ("ordinal", "Ordinals1"), // redundant, but wanted to note that this is just an "int"
                _ => ("ordinals", $"Ordinals{record.Properties.Count}"),
            };

            IDisposable recordClass = this.TypeKeyword switch
            {
                "class" => code.PartialClass("public", rowClassName, $"IReading<{rowClassName}, {tupleType}>"),
                "record class" => code.PartialRecordClass("public", rowClassName, $"IReading<{rowClassName}, {tupleType}>"),
                "record struct" => code.PartialRecordStruct("public", rowClassName, $"IReading<{rowClassName}, {tupleType}>"),
                _ => throw new NotImplementedException("Unknown type keyword: " + this.TypeKeyword ?? "null" + ". Expected: 'class' or 'record class' or 'record struct'."),
            };
            using (recordClass)
            {
                foreach (var property in record.Properties)
                {
                    code.Line("public required {0} {1} {{ get; init; }}", property.FieldType, property.FieldName);
                }
                code.Line();

                code.StartMethod($"static", tupleType, $"IReading<{rowClassName}, {tupleType}>.Ordinals", "SqlDataReader reader");
                if (record.Properties.Count == 1)
                {
                    code.Text($" => (");
                }
                else
                {
                    code.Text($" => new(");
                }
                code.Line();
                code.Indent();
                using (var it = record.Properties.GetEnumerator())
                {
                    if (it.MoveNext())
                    {
                        while (true)
                        {
                            code.StartLine();
                            code.Text($"reader.GetOrdinal(\"{it.Current.ColumnName}\")");
                            if (it.MoveNext())
                            {
                                code.Text(",");
                                code.Line();
                                continue;
                            }
                            code.Line();
                            break;
                        }
                    }
                }
                code.Dedent();
                code.Line(");");
                code.Line();

                code.StartLine();
                code.Text($"static {rowClassName} IReading<{rowClassName}, {tupleType}>.Read(SqlDataReader reader, {tupleType} {ordinalsArg})");
                code.Text($" => new {record.CSharpName}");
                code.Line();
                using (code.CreateBraceScope(preamble: null, withClosingBrace: ";"))
                {
                    int i = 1;
                    foreach (var property in record.Properties)
                    {
                        if (property.SqlDbType == SqlDbType.Char)
                        {
                            code.Line($"// {TrimChar} {property}");
                        }
                        var fieldName = property.FieldName;
                        var IsValueType = property.FieldTypeIsValueType;
                        var ColumnIsNullable = property.ColumnIsNullable;
                        var fieldTypeWithoutNullable = property.FieldTypeWithoutNullable;
                        var columnName = property.ColumnName;
                        var ordinalVarName = record.Properties.Count == 1 ? ordinalsArg : $"{ordinalsArg}.Item{i++}";
                        var line = (IsValueType, ColumnIsNullable) switch
                        {
                            (false, true) => String.Format("{0} = OptionalClass<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeWithoutNullable),
                            (true, true) => String.Format("{0} = OptionalValue<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeWithoutNullable),
                            (false, false) => String.Format("{0} = RequiredClass<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeWithoutNullable),
                            (true, false) => String.Format("{0} = RequiredValue<{2}>(reader, {1}),", fieldName, ordinalVarName, fieldTypeWithoutNullable),
                        };
                        code.Line(line);
                    }
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
                        var propertyTypeName = col.FieldTypeWithoutNullable;
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

        code.Text(
"""
file interface IReading<TRow, TOrds>
{
	static abstract TRow Read(SqlDataReader reader, TOrds ordinals);
	static abstract TOrds Ordinals(SqlDataReader reader);
}

file static class FileMethods
{
	public static T? OptionalClass<T>(SqlDataReader reader, int ordinal) where T : class => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
	public static T? OptionalValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
	public static T RequiredClass<T>(SqlDataReader reader, int ordinal) where T : class => reader.GetFieldValue<T>(ordinal);
	public static T RequiredValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.GetFieldValue<T>(ordinal);

	public static List<TRow> ExecuteCommand<TRow, TOrds>(SqlCommand cmd) where TRow : IReading<TRow, TOrds>
	{
		var result = new List<TRow>();
		using var reader = cmd.ExecuteReader();
		if (!reader.Read()) { return result; }
		var ords = TRow.Ordinals(reader);
		do { result.Add(TRow.Read(reader, ords)); } while (reader.Read());
		return result;
	}

	public static async Task<List<TRow>> ExecuteCommandAsync<TRow, TOrds>(SqlCommand cmd, CancellationToken cancellationToken) where TRow : IReading<TRow, TOrds>
	{
		var result = new List<TRow>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
		if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) { return result; }
		var ords = TRow.Ordinals(reader);
		do { result.Add(TRow.Read(reader, ords)); } while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false));
		return result;
	}

	public static SqlCommand CreateStatement(SqlConnection connection, String text) => new() { Connection = connection, CommandType = CommandType.Text, CommandText = text };
	public static SqlCommand CreateStoredProcedure(SqlConnection connection, String text) => new() { Connection = connection, CommandType = CommandType.StoredProcedure, CommandText = text };
	public static SqlCommand CreateStatement(SqlConnection connection, String text, SqlParameter[] parameters)
	{
		var cmd = new SqlCommand() { Connection = connection, CommandType = CommandType.Text, CommandText = text };
		cmd.Parameters.AddRange(parameters);
		return cmd;
	}
	public static SqlCommand CreateStoredProcedure(SqlConnection connection, String text, SqlParameter[] parameters)
	{
		var cmd = new SqlCommand() { Connection = connection, CommandType = CommandType.StoredProcedure, CommandText = text };
		cmd.Parameters.AddRange(parameters);
		return cmd;
	}

	public static SqlParameter CreateParameter(String parameterName, Object? value, SqlDbType sqlDbType, Int32 size = -1, ParameterDirection direction = ParameterDirection.Input) => new()
	{
		Size = size,
		Direction = direction,
		SqlDbType = sqlDbType,
		ParameterName = parameterName,
		Value = value ?? DBNull.Value,
	};

	public static SqlParameter CreateParameter(String parameterName, Object? value, SqlDbType sqlDbType, String typeName, Int32 size = -1, ParameterDirection direction = ParameterDirection.Input) => new()
	{
		Size = size,
		Direction = direction,
		TypeName = typeName,
		SqlDbType = sqlDbType,
		ParameterName = parameterName,
		Value = value ?? DBNull.Value,
	};
}

"""
);

        code.Line();
        using (code.PartialClass("public static", ClassName))
        {
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
                    var methodParametersWithConnectionWithCancellation = csharpParameters.ToList(); // NOTE THE COPY!
                    var methodParametersWithCommand = csharpParameters.ToList(); // NOTE THE COPY!
                    methodParametersWithConnection.Insert(0, new MethodParameter() { CSharpType = "SqlConnection", CSharpName = "connection" });
                    methodParametersWithConnectionWithCancellation.Insert(0, new MethodParameter() { CSharpType = "SqlConnection", CSharpName = "connection" });
                    methodParametersWithConnectionWithCancellation.Add(new MethodParameter() { CSharpType = "CancellationToken", CSharpName = "cancellationToken = default" }); // HACK: sneaking default value into the name
                    methodParametersWithCommand.Insert(0, new MethodParameter() { CSharpType = "SqlCommand", CSharpName = "cmd" });

                    var commandParametersString = String.Join(", ", methodParametersWithCommand.Select(i => $"{i.CSharpType} {i.CSharpName}"));
                    var connectionParametersString = String.Join(", ", methodParametersWithConnection.Select(i => $"{i.CSharpType} {i.CSharpName}"));
                    var connectionParametersWithCancellationString = String.Join(", ", methodParametersWithConnectionWithCancellation.Select(i => $"{i.CSharpType} {i.CSharpName}"));

                    var connectionArgumentsString = String.Join(", ", methodParametersWithConnection.Select(i => $"{i.CSharpName}"));
                    var connectionArgumentsWithCancellationString = String.Join(", ", methodParametersWithConnectionWithCancellation.Select(i => $"{i.CSharpName}"));

                    var asyncKeyword = isAsync ? " async" : "";
                    var returnType = isAsync ? $"Task<{method.DataType}>" : method.DataType;

                    // Ordinals/ReadRow/Command, which are shared between sync and async methods
                    if (isFirst)
                    {
                        // Command
                        var cmdMethod = method.IsStoredProc ? "CreateStoredProcedure" : "CreateStatement";
                        if (commandParameters.Count == 0)
                        {
                            code.MethodExpression($"private static", "SqlCommand", method.Name + "Command", connectionParametersString, $"{cmdMethod}(connection, \"{commandText}\")");
                        }
                        else
                        {
                            code.Line($"private static SqlCommand {method.Name}Command({connectionParametersString}) => {cmdMethod}(connection, \"{commandText}\", [");
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
                        }
                        code.Line();
                    }

                    if (method.MSDescription is { Length: > 0 } msDescription)
                    {
                        code.Line($"/// <summary>");
                        code.Line($"/// {msDescription}");
                        code.Line($"/// </summary>");
                    }
                    if (method.ObsoleteMessage is { Length: > 0 } obsoleteMessage)
                    {
                        code.Line($"[Obsolete(\"{obsoleteMessage}\")]");
                    }

                    var actualMethodName = isAsync ? $"{method.Name}Async" : method.Name;

                    var actualParamsString = isAsync ? connectionParametersWithCancellationString : connectionParametersString;
                    using (var _2 = code.Method($"public static{asyncKeyword}", returnType, actualMethodName, actualParamsString))
                    {
                        if (isAsync && !AllowAsync)
                        {
                            // no-op await suppresses warning
                            code.Line("await Task.CompletedTask; // sqlclient.async: false");
                        }
                        if (method.HasResultSet)
                        {
                            if (method.ResultSetRecord is not { } record) { throw new InvalidOperationException("Method has result set but no record type."); }
                            var rowType = method.ResultSetRecord.CSharpName;
                            var tupleType = $"Ordinals{record.Properties.Count}";
                            code.Line($"using var cmd = {method.Name}Command({connectionArgumentsString});");
                            if (isAsync && AllowAsync)
                            {
                                code.Line($"return await ExecuteCommandAsync<{rowType}, {tupleType}>(cmd, cancellationToken).ConfigureAwait(false);");
                            }
                            else
                            {
                                code.Line($"return ExecuteCommand<{rowType}, {tupleType}>(cmd);");
                            }
                        }
                        else if (method.HasResultSet == false)
                        {
                            if (method.DataType != "int") { throw new InvalidOperationException("Method has no result set but return type is not 'int'."); }

                            code.Line($"using var cmd = {method.Name}Command({connectionArgumentsString});");
                            code.Line($"return {(isAsync && AllowAsync ? "await " : "")}cmd.ExecuteNonQuery{(isAsync && AllowAsync ? "Async" : "")}({(isAsync && AllowAsync ? "cancellationToken" : "")}){(isAsync && AllowAsync ? ".ConfigureAwait(false)" : "")};");
                        }
                        else
                        {
                            throw new NotImplementedException("Unknown method return type: " + method.DataType ?? "null");
                        }
                    }
                    code.Line();
                }
            }
        }

        code.Line();
        foreach (var constant in Constants.OrderBy(c => c.Name))
        {
            using (code.PartialClass("public static", constant.Name))
            {
                foreach (var item in constant.Items)
                {
                    code.Line(item);
                }
            }
        }

        return code.ToString();
    }
}
