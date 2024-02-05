using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

#nullable enable

namespace Pingmint.CodeGen.Sql;

public partial record class DmDescribeFirstResultSetForObjectRow
{
	public required String? Name { get; init; }
	public required Int32 SchemaId { get; init; }
	public required Byte SystemTypeId { get; init; }
	public required Int32 UserTypeId { get; init; }
	public required Boolean? IsNullable { get; init; }
	public required Int32? ColumnOrdinal { get; init; }
	public required String SqlTypeName { get; init; }
}

public partial record class DmDescribeFirstResultSetRow
{
	public required String? Name { get; init; }
	public required Int32 SchemaId { get; init; }
	public required Byte SystemTypeId { get; init; }
	public required Int32 UserTypeId { get; init; }
	public required Boolean? IsNullable { get; init; }
	public required Int32? ColumnOrdinal { get; init; }
	public required String SqlTypeName { get; init; }
}

public partial record class GetParametersForObjectRow
{
	public required Int32 ParameterId { get; init; }
	public required Int32 SchemaId { get; init; }
	public required Byte SystemTypeId { get; init; }
	public required Int32 UserTypeId { get; init; }
	public required String? Name { get; init; }
	public required Boolean IsOutput { get; init; }
	public required Int16 MaxLength { get; init; }
	public required Boolean IsTableType { get; init; }
	public required String TypeName { get; init; }
}

public partial record class GetProcedureForSchemaRow
{
	public required String Name { get; init; }
	public required Int32 ObjectId { get; init; }
	public required String SchemaName { get; init; }
	public required String? ObsoleteMessage { get; init; }
}

public partial record class GetProceduresForSchemaRow
{
	public required String Name { get; init; }
	public required Int32 ObjectId { get; init; }
	public required String SchemaName { get; init; }
	public required String? ObsoleteMessage { get; init; }
}

public partial record class GetSchemasRow
{
	public required String Name { get; init; }
	public required Int32 SchemaId { get; init; }
}

public partial record class GetSysTypeRow
{
	public required Byte SystemTypeId { get; init; }
	public required Boolean IsTableType { get; init; }
	public required String Name { get; init; }
}

public partial record class GetSysTypesRow
{
	public required String Name { get; init; }
	public required Byte SystemTypeId { get; init; }
	public required Int32 UserTypeId { get; init; }
	public required Int32 SchemaId { get; init; }
	public required Int32? PrincipalId { get; init; }
	public required Int16 MaxLength { get; init; }
	public required Byte Precision { get; init; }
	public required Byte Scale { get; init; }
	public required String? CollationName { get; init; }
	public required Boolean? IsNullable { get; init; }
	public required Boolean IsUserDefined { get; init; }
	public required Boolean IsAssemblyType { get; init; }
	public required Int32 DefaultObjectId { get; init; }
	public required Int32 RuleObjectId { get; init; }
	public required Boolean IsTableType { get; init; }
	public required String SchemaName { get; init; }
	public required Boolean IsFromSysSchema { get; init; }
}

public partial record class GetTableTypeColumnsRow
{
	public required Boolean? IsNullable { get; init; }
	public required Int16 MaxLength { get; init; }
	public required String? Name { get; init; }
	public required String TypeName { get; init; }
	public required Int32 SchemaId { get; init; }
	public required Byte SystemTypeId { get; init; }
	public required Int32 UserTypeId { get; init; }
}

public partial record class GetTableTypesRow
{
	public required String Name { get; init; }
	public required Int32 TypeTableObjectId { get; init; }
	public required String SchemaName { get; init; }
	public required Int32 SchemaId { get; init; }
	public required Byte SystemTypeId { get; init; }
	public required Int32 UserTypeId { get; init; }
}

public partial class Proxy
{
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

	private static (int, int, int, int, int, int, int) DmDescribeFirstResultSetOrdinals(SqlDataReader reader) => (
		reader.GetOrdinal("name")
		, reader.GetOrdinal("schema_id")
		, reader.GetOrdinal("system_type_id")
		, reader.GetOrdinal("user_type_id")
		, reader.GetOrdinal("is_nullable")
		, reader.GetOrdinal("column_ordinal")
		, reader.GetOrdinal("sql_type_name")
	);

	private static DmDescribeFirstResultSetRow DmDescribeFirstResultSetReadRow(SqlDataReader reader, (int, int, int, int, int, int, int) ords) => new DmDescribeFirstResultSetRow
	{
		Name = OptionalClass<String>(reader, ords.Item1),
		SchemaId = RequiredValue<Int32>(reader, ords.Item2),
		SystemTypeId = RequiredValue<Byte>(reader, ords.Item3),
		UserTypeId = RequiredValue<Int32>(reader, ords.Item4),
		IsNullable = OptionalValue<Boolean>(reader, ords.Item5),
		ColumnOrdinal = OptionalValue<Int32>(reader, ords.Item6),
		SqlTypeName = RequiredClass<String>(reader, ords.Item7),
	};

	private static SqlCommand DmDescribeFirstResultSetCommand(SqlConnection connection, String? text, String? parameters)
	{
		var cmd = CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set(@text, @parameters, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");
		cmd.Parameters.AddRange([
			CreateParameter("@text", text, SqlDbType.NVarChar, 8000),
			CreateParameter("@parameters", parameters, SqlDbType.NVarChar, 8000),
		]);
		return cmd;
	}

	public static List<DmDescribeFirstResultSetRow> DmDescribeFirstResultSet(SqlConnection connection, String? text, String? parameters)
	{
		using var cmd = DmDescribeFirstResultSetCommand(connection, text, parameters);
		return ExecuteCommand(cmd, DmDescribeFirstResultSetOrdinals, DmDescribeFirstResultSetReadRow);
	}

	public static async Task<List<DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlConnection connection, String? text, String? parameters)
	{
		using var cmd = DmDescribeFirstResultSetCommand(connection, text, parameters);
		return await ExecuteCommandAsync(cmd, DmDescribeFirstResultSetOrdinals, DmDescribeFirstResultSetReadRow).ConfigureAwait(false);
	}

	private static (int, int, int, int, int, int, int) DmDescribeFirstResultSetForObjectOrdinals(SqlDataReader reader) => (
		reader.GetOrdinal("name")
		, reader.GetOrdinal("schema_id")
		, reader.GetOrdinal("system_type_id")
		, reader.GetOrdinal("user_type_id")
		, reader.GetOrdinal("is_nullable")
		, reader.GetOrdinal("column_ordinal")
		, reader.GetOrdinal("sql_type_name")
	);

	private static DmDescribeFirstResultSetForObjectRow DmDescribeFirstResultSetForObjectReadRow(SqlDataReader reader, (int, int, int, int, int, int, int) ords) => new DmDescribeFirstResultSetForObjectRow
	{
		Name = OptionalClass<String>(reader, ords.Item1),
		SchemaId = RequiredValue<Int32>(reader, ords.Item2),
		SystemTypeId = RequiredValue<Byte>(reader, ords.Item3),
		UserTypeId = RequiredValue<Int32>(reader, ords.Item4),
		IsNullable = OptionalValue<Boolean>(reader, ords.Item5),
		ColumnOrdinal = OptionalValue<Int32>(reader, ords.Item6),
		SqlTypeName = RequiredClass<String>(reader, ords.Item7),
	};

	private static SqlCommand DmDescribeFirstResultSetForObjectCommand(SqlConnection connection, Int32? objectid)
	{
		var cmd = CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set_for_object(@objectid, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");
		cmd.Parameters.AddRange([
			CreateParameter("@objectid", objectid, SqlDbType.Int),
		]);
		return cmd;
	}

	public static List<DmDescribeFirstResultSetForObjectRow> DmDescribeFirstResultSetForObject(SqlConnection connection, Int32? objectid)
	{
		using var cmd = DmDescribeFirstResultSetForObjectCommand(connection, objectid);
		return ExecuteCommand(cmd, DmDescribeFirstResultSetForObjectOrdinals, DmDescribeFirstResultSetForObjectReadRow);
	}

	public static async Task<List<DmDescribeFirstResultSetForObjectRow>> DmDescribeFirstResultSetForObjectAsync(SqlConnection connection, Int32? objectid)
	{
		using var cmd = DmDescribeFirstResultSetForObjectCommand(connection, objectid);
		return await ExecuteCommandAsync(cmd, DmDescribeFirstResultSetForObjectOrdinals, DmDescribeFirstResultSetForObjectReadRow).ConfigureAwait(false);
	}

	private static (int, int, int, int, int, int, int, int, int) GetParametersForObjectOrdinals(SqlDataReader reader) => (
		reader.GetOrdinal("parameter_id")
		, reader.GetOrdinal("schema_id")
		, reader.GetOrdinal("system_type_id")
		, reader.GetOrdinal("user_type_id")
		, reader.GetOrdinal("name")
		, reader.GetOrdinal("is_output")
		, reader.GetOrdinal("max_length")
		, reader.GetOrdinal("is_table_type")
		, reader.GetOrdinal("Type_Name")
	);

	private static GetParametersForObjectRow GetParametersForObjectReadRow(SqlDataReader reader, (int, int, int, int, int, int, int, int, int) ords) => new GetParametersForObjectRow
	{
		ParameterId = RequiredValue<Int32>(reader, ords.Item1),
		SchemaId = RequiredValue<Int32>(reader, ords.Item2),
		SystemTypeId = RequiredValue<Byte>(reader, ords.Item3),
		UserTypeId = RequiredValue<Int32>(reader, ords.Item4),
		Name = OptionalClass<String>(reader, ords.Item5),
		IsOutput = RequiredValue<Boolean>(reader, ords.Item6),
		MaxLength = RequiredValue<Int16>(reader, ords.Item7),
		IsTableType = RequiredValue<Boolean>(reader, ords.Item8),
		TypeName = RequiredClass<String>(reader, ords.Item9),
	};

	private static SqlCommand GetParametersForObjectCommand(SqlConnection connection, Int32? id)
	{
		var cmd = CreateStatement(connection, "SELECT P.parameter_id, T.schema_id, P.system_type_id, P.user_type_id, P.name, P.is_output, P.max_length, T.is_table_type, T.name as [Type_Name] FROM sys.parameters AS P JOIN sys.types AS T ON (P.system_type_id = T.system_type_id AND P.user_type_id = T.user_type_id) WHERE P.object_id = @id");
		cmd.Parameters.AddRange([
			CreateParameter("@id", id, SqlDbType.Int),
		]);
		return cmd;
	}

	public static List<GetParametersForObjectRow> GetParametersForObject(SqlConnection connection, Int32? id)
	{
		using var cmd = GetParametersForObjectCommand(connection, id);
		return ExecuteCommand(cmd, GetParametersForObjectOrdinals, GetParametersForObjectReadRow);
	}

	public static async Task<List<GetParametersForObjectRow>> GetParametersForObjectAsync(SqlConnection connection, Int32? id)
	{
		using var cmd = GetParametersForObjectCommand(connection, id);
		return await ExecuteCommandAsync(cmd, GetParametersForObjectOrdinals, GetParametersForObjectReadRow).ConfigureAwait(false);
	}

	private static (int, int, int, int) GetProcedureForSchemaOrdinals(SqlDataReader reader) => (
		reader.GetOrdinal("name")
		, reader.GetOrdinal("object_id")
		, reader.GetOrdinal("Schema_Name")
		, reader.GetOrdinal("Obsolete_Message")
	);

	private static GetProcedureForSchemaRow GetProcedureForSchemaReadRow(SqlDataReader reader, (int, int, int, int) ords) => new GetProcedureForSchemaRow
	{
		Name = RequiredClass<String>(reader, ords.Item1),
		ObjectId = RequiredValue<Int32>(reader, ords.Item2),
		SchemaName = RequiredClass<String>(reader, ords.Item3),
		ObsoleteMessage = OptionalClass<String>(reader, ords.Item4),
	};

	private static SqlCommand GetProcedureForSchemaCommand(SqlConnection connection, String? schema, String? proc)
	{
		var cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema AND P.name = @proc ORDER BY P.name");
		cmd.Parameters.AddRange([
			CreateParameter("@schema", schema, SqlDbType.VarChar, 8000),
			CreateParameter("@proc", proc, SqlDbType.VarChar, 8000),
		]);
		return cmd;
	}

	public static List<GetProcedureForSchemaRow> GetProcedureForSchema(SqlConnection connection, String? schema, String? proc)
	{
		using var cmd = GetProcedureForSchemaCommand(connection, schema, proc);
		return ExecuteCommand(cmd, GetProcedureForSchemaOrdinals, GetProcedureForSchemaReadRow);
	}

	public static async Task<List<GetProcedureForSchemaRow>> GetProcedureForSchemaAsync(SqlConnection connection, String? schema, String? proc)
	{
		using var cmd = GetProcedureForSchemaCommand(connection, schema, proc);
		return await ExecuteCommandAsync(cmd, GetProcedureForSchemaOrdinals, GetProcedureForSchemaReadRow).ConfigureAwait(false);
	}

	private static (int, int, int, int) GetProceduresForSchemaOrdinals(SqlDataReader reader) => (
		reader.GetOrdinal("name")
		, reader.GetOrdinal("object_id")
		, reader.GetOrdinal("Schema_Name")
		, reader.GetOrdinal("Obsolete_Message")
	);

	private static GetProceduresForSchemaRow GetProceduresForSchemaReadRow(SqlDataReader reader, (int, int, int, int) ords) => new GetProceduresForSchemaRow
	{
		Name = RequiredClass<String>(reader, ords.Item1),
		ObjectId = RequiredValue<Int32>(reader, ords.Item2),
		SchemaName = RequiredClass<String>(reader, ords.Item3),
		ObsoleteMessage = OptionalClass<String>(reader, ords.Item4),
	};

	private static SqlCommand GetProceduresForSchemaCommand(SqlConnection connection, String? schema)
	{
		var cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema ORDER BY P.name");
		cmd.Parameters.AddRange([
			CreateParameter("@schema", schema, SqlDbType.VarChar, 8000),
		]);
		return cmd;
	}

	public static List<GetProceduresForSchemaRow> GetProceduresForSchema(SqlConnection connection, String? schema)
	{
		using var cmd = GetProceduresForSchemaCommand(connection, schema);
		return ExecuteCommand(cmd, GetProceduresForSchemaOrdinals, GetProceduresForSchemaReadRow);
	}

	public static async Task<List<GetProceduresForSchemaRow>> GetProceduresForSchemaAsync(SqlConnection connection, String? schema)
	{
		using var cmd = GetProceduresForSchemaCommand(connection, schema);
		return await ExecuteCommandAsync(cmd, GetProceduresForSchemaOrdinals, GetProceduresForSchemaReadRow).ConfigureAwait(false);
	}

	private static (int, int) GetSchemasOrdinals(SqlDataReader reader) => (
		reader.GetOrdinal("name")
		, reader.GetOrdinal("schema_id")
	);

	private static GetSchemasRow GetSchemasReadRow(SqlDataReader reader, (int, int) ords) => new GetSchemasRow
	{
		Name = RequiredClass<String>(reader, ords.Item1),
		SchemaId = RequiredValue<Int32>(reader, ords.Item2),
	};

	private static SqlCommand GetSchemasCommand(SqlConnection connection) => CreateStatement(connection, "SELECT name, schema_id FROM sys.schemas");

	public static List<GetSchemasRow> GetSchemas(SqlConnection connection)
	{
		using var cmd = GetSchemasCommand(connection);
		return ExecuteCommand(cmd, GetSchemasOrdinals, GetSchemasReadRow);
	}

	public static async Task<List<GetSchemasRow>> GetSchemasAsync(SqlConnection connection)
	{
		using var cmd = GetSchemasCommand(connection);
		return await ExecuteCommandAsync(cmd, GetSchemasOrdinals, GetSchemasReadRow).ConfigureAwait(false);
	}

	private static (int, int, int) GetSysTypeOrdinals(SqlDataReader reader) => (
		reader.GetOrdinal("system_type_id")
		, reader.GetOrdinal("is_table_type")
		, reader.GetOrdinal("name")
	);

	private static GetSysTypeRow GetSysTypeReadRow(SqlDataReader reader, (int, int, int) ords) => new GetSysTypeRow
	{
		SystemTypeId = RequiredValue<Byte>(reader, ords.Item1),
		IsTableType = RequiredValue<Boolean>(reader, ords.Item2),
		Name = RequiredClass<String>(reader, ords.Item3),
	};

	private static SqlCommand GetSysTypeCommand(SqlConnection connection, Int32? id)
	{
		var cmd = CreateStatement(connection, "SELECT system_type_id, is_table_type, name FROM sys.types where system_type_id = @id");
		cmd.Parameters.AddRange([
			CreateParameter("@id", id, SqlDbType.Int),
		]);
		return cmd;
	}

	public static List<GetSysTypeRow> GetSysType(SqlConnection connection, Int32? id)
	{
		using var cmd = GetSysTypeCommand(connection, id);
		return ExecuteCommand(cmd, GetSysTypeOrdinals, GetSysTypeReadRow);
	}

	public static async Task<List<GetSysTypeRow>> GetSysTypeAsync(SqlConnection connection, Int32? id)
	{
		using var cmd = GetSysTypeCommand(connection, id);
		return await ExecuteCommandAsync(cmd, GetSysTypeOrdinals, GetSysTypeReadRow).ConfigureAwait(false);
	}

	private static (int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int) GetSysTypesOrdinals(SqlDataReader reader) => (
		reader.GetOrdinal("name")
		, reader.GetOrdinal("system_type_id")
		, reader.GetOrdinal("user_type_id")
		, reader.GetOrdinal("schema_id")
		, reader.GetOrdinal("principal_id")
		, reader.GetOrdinal("max_length")
		, reader.GetOrdinal("precision")
		, reader.GetOrdinal("scale")
		, reader.GetOrdinal("collation_name")
		, reader.GetOrdinal("is_nullable")
		, reader.GetOrdinal("is_user_defined")
		, reader.GetOrdinal("is_assembly_type")
		, reader.GetOrdinal("default_object_id")
		, reader.GetOrdinal("rule_object_id")
		, reader.GetOrdinal("is_table_type")
		, reader.GetOrdinal("schema_name")
		, reader.GetOrdinal("is_from_sys_schema")
	);

	private static GetSysTypesRow GetSysTypesReadRow(SqlDataReader reader, (int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int) ords) => new GetSysTypesRow
	{
		Name = RequiredClass<String>(reader, ords.Item1),
		SystemTypeId = RequiredValue<Byte>(reader, ords.Item2),
		UserTypeId = RequiredValue<Int32>(reader, ords.Item3),
		SchemaId = RequiredValue<Int32>(reader, ords.Item4),
		PrincipalId = OptionalValue<Int32>(reader, ords.Item5),
		MaxLength = RequiredValue<Int16>(reader, ords.Item6),
		Precision = RequiredValue<Byte>(reader, ords.Item7),
		Scale = RequiredValue<Byte>(reader, ords.Item8),
		CollationName = OptionalClass<String>(reader, ords.Item9),
		IsNullable = OptionalValue<Boolean>(reader, ords.Item10),
		IsUserDefined = RequiredValue<Boolean>(reader, ords.Item11),
		IsAssemblyType = RequiredValue<Boolean>(reader, ords.Item12),
		DefaultObjectId = RequiredValue<Int32>(reader, ords.Item13),
		RuleObjectId = RequiredValue<Int32>(reader, ords.Item14),
		IsTableType = RequiredValue<Boolean>(reader, ords.Item15),
		SchemaName = RequiredClass<String>(reader, ords.Item16),
		IsFromSysSchema = RequiredValue<Boolean>(reader, ords.Item17),
	};

	private static SqlCommand GetSysTypesCommand(SqlConnection connection) => CreateStatement(connection, "SELECT   T.*,   S.name as [schema_name],   ISNULL(CAST(CASE WHEN S.name = 'sys' THEN 1 ELSE 0 END as bit), 0) AS [is_from_sys_schema] FROM sys.types T JOIN sys.schemas S ON (T.schema_id = S.schema_id)");

	public static List<GetSysTypesRow> GetSysTypes(SqlConnection connection)
	{
		using var cmd = GetSysTypesCommand(connection);
		return ExecuteCommand(cmd, GetSysTypesOrdinals, GetSysTypesReadRow);
	}

	public static async Task<List<GetSysTypesRow>> GetSysTypesAsync(SqlConnection connection)
	{
		using var cmd = GetSysTypesCommand(connection);
		return await ExecuteCommandAsync(cmd, GetSysTypesOrdinals, GetSysTypesReadRow).ConfigureAwait(false);
	}

	private static (int, int, int, int, int, int, int) GetTableTypeColumnsOrdinals(SqlDataReader reader) => (
		reader.GetOrdinal("is_nullable")
		, reader.GetOrdinal("max_length")
		, reader.GetOrdinal("name")
		, reader.GetOrdinal("Type_Name")
		, reader.GetOrdinal("schema_id")
		, reader.GetOrdinal("system_type_id")
		, reader.GetOrdinal("user_type_id")
	);

	private static GetTableTypeColumnsRow GetTableTypeColumnsReadRow(SqlDataReader reader, (int, int, int, int, int, int, int) ords) => new GetTableTypeColumnsRow
	{
		IsNullable = OptionalValue<Boolean>(reader, ords.Item1),
		MaxLength = RequiredValue<Int16>(reader, ords.Item2),
		Name = OptionalClass<String>(reader, ords.Item3),
		TypeName = RequiredClass<String>(reader, ords.Item4),
		SchemaId = RequiredValue<Int32>(reader, ords.Item5),
		SystemTypeId = RequiredValue<Byte>(reader, ords.Item6),
		UserTypeId = RequiredValue<Int32>(reader, ords.Item7),
	};

	private static SqlCommand GetTableTypeColumnsCommand(SqlConnection connection, Int32? id)
	{
		var cmd = CreateStatement(connection, "SELECT C.is_nullable, C.max_length, C.name, t.name as [Type_Name], T.schema_id, T.system_type_id, T.user_type_id from sys.columns as C join sys.types T ON (C.system_type_id = T.system_type_id) where C.object_id = @id and t.name <> 'sysname' order by c.column_id");
		cmd.Parameters.AddRange([
			CreateParameter("@id", id, SqlDbType.Int),
		]);
		return cmd;
	}

	public static List<GetTableTypeColumnsRow> GetTableTypeColumns(SqlConnection connection, Int32? id)
	{
		using var cmd = GetTableTypeColumnsCommand(connection, id);
		return ExecuteCommand(cmd, GetTableTypeColumnsOrdinals, GetTableTypeColumnsReadRow);
	}

	public static async Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(SqlConnection connection, Int32? id)
	{
		using var cmd = GetTableTypeColumnsCommand(connection, id);
		return await ExecuteCommandAsync(cmd, GetTableTypeColumnsOrdinals, GetTableTypeColumnsReadRow).ConfigureAwait(false);
	}

	private static (int, int, int, int, int, int) GetTableTypesOrdinals(SqlDataReader reader) => (
		reader.GetOrdinal("name")
		, reader.GetOrdinal("type_table_object_id")
		, reader.GetOrdinal("Schema_Name")
		, reader.GetOrdinal("schema_id")
		, reader.GetOrdinal("system_type_id")
		, reader.GetOrdinal("user_type_id")
	);

	private static GetTableTypesRow GetTableTypesReadRow(SqlDataReader reader, (int, int, int, int, int, int) ords) => new GetTableTypesRow
	{
		Name = RequiredClass<String>(reader, ords.Item1),
		TypeTableObjectId = RequiredValue<Int32>(reader, ords.Item2),
		SchemaName = RequiredClass<String>(reader, ords.Item3),
		SchemaId = RequiredValue<Int32>(reader, ords.Item4),
		SystemTypeId = RequiredValue<Byte>(reader, ords.Item5),
		UserTypeId = RequiredValue<Int32>(reader, ords.Item6),
	};

	private static SqlCommand GetTableTypesCommand(SqlConnection connection) => CreateStatement(connection, "SELECT T.name, T.type_table_object_id, S.name as [Schema_Name], T.schema_id, T.system_type_id, T.user_type_id FROM sys.table_types AS T INNER JOIN sys.schemas as S ON (T.schema_id = S.schema_id) ORDER BY S.name, T.name");

	public static List<GetTableTypesRow> GetTableTypes(SqlConnection connection)
	{
		using var cmd = GetTableTypesCommand(connection);
		return ExecuteCommand(cmd, GetTableTypesOrdinals, GetTableTypesReadRow);
	}

	public static async Task<List<GetTableTypesRow>> GetTableTypesAsync(SqlConnection connection)
	{
		using var cmd = GetTableTypesCommand(connection);
		return await ExecuteCommandAsync(cmd, GetTableTypesOrdinals, GetTableTypesReadRow).ConfigureAwait(false);
	}

}
