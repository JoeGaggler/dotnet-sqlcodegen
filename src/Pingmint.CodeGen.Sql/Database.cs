using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using static Pingmint.CodeGen.Sql.FileMethods;

#nullable enable

namespace Pingmint.CodeGen.Sql;

using Ordinals1 = int;
record struct Ordinals2(int Item1, int Item2);
record struct Ordinals3(int Item1, int Item2, int Item3);
record struct Ordinals4(int Item1, int Item2, int Item3, int Item4);
record struct Ordinals6(int Item1, int Item2, int Item3, int Item4, int Item5, int Item6);
record struct Ordinals7(int Item1, int Item2, int Item3, int Item4, int Item5, int Item6, int Item7);
record struct Ordinals9(int Item1, int Item2, int Item3, int Item4, int Item5, int Item6, int Item7, int Item8, int Item9);
record struct Ordinals17(int Item1, int Item2, int Item3, int Item4, int Item5, int Item6, int Item7, int Item8, int Item9, int Item10, int Item11, int Item12, int Item13, int Item14, int Item15, int Item16, int Item17);

public partial record class DmDescribeFirstResultSetForObjectRow : IReading<DmDescribeFirstResultSetForObjectRow, Ordinals7>
{
	public required String? Name { get; init; }
	public required Int32 SchemaId { get; init; }
	public required Byte SystemTypeId { get; init; }
	public required Int32 UserTypeId { get; init; }
	public required Boolean? IsNullable { get; init; }
	public required Int32? ColumnOrdinal { get; init; }
	public required String SqlTypeName { get; init; }

	static Ordinals7 IReading<DmDescribeFirstResultSetForObjectRow, Ordinals7>.Ordinals(SqlDataReader reader) => new(
		reader.GetOrdinal("name"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id"),
		reader.GetOrdinal("is_nullable"),
		reader.GetOrdinal("column_ordinal"),
		reader.GetOrdinal("sql_type_name")
	);

	static DmDescribeFirstResultSetForObjectRow IReading<DmDescribeFirstResultSetForObjectRow, Ordinals7>.Read(SqlDataReader reader, Ordinals7 ordinals) => new DmDescribeFirstResultSetForObjectRow
	{
		Name = OptionalClass<String>(reader, ordinals.Item1),
		SchemaId = RequiredValue<Int32>(reader, ordinals.Item2),
		SystemTypeId = RequiredValue<Byte>(reader, ordinals.Item3),
		UserTypeId = RequiredValue<Int32>(reader, ordinals.Item4),
		IsNullable = OptionalValue<Boolean>(reader, ordinals.Item5),
		ColumnOrdinal = OptionalValue<Int32>(reader, ordinals.Item6),
		SqlTypeName = RequiredClass<String>(reader, ordinals.Item7),
	};
}

public partial record class DmDescribeFirstResultSetRow : IReading<DmDescribeFirstResultSetRow, Ordinals7>
{
	public required String? Name { get; init; }
	public required Int32 SchemaId { get; init; }
	public required Byte SystemTypeId { get; init; }
	public required Int32 UserTypeId { get; init; }
	public required Boolean? IsNullable { get; init; }
	public required Int32? ColumnOrdinal { get; init; }
	public required String SqlTypeName { get; init; }

	static Ordinals7 IReading<DmDescribeFirstResultSetRow, Ordinals7>.Ordinals(SqlDataReader reader) => new(
		reader.GetOrdinal("name"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id"),
		reader.GetOrdinal("is_nullable"),
		reader.GetOrdinal("column_ordinal"),
		reader.GetOrdinal("sql_type_name")
	);

	static DmDescribeFirstResultSetRow IReading<DmDescribeFirstResultSetRow, Ordinals7>.Read(SqlDataReader reader, Ordinals7 ordinals) => new DmDescribeFirstResultSetRow
	{
		Name = OptionalClass<String>(reader, ordinals.Item1),
		SchemaId = RequiredValue<Int32>(reader, ordinals.Item2),
		SystemTypeId = RequiredValue<Byte>(reader, ordinals.Item3),
		UserTypeId = RequiredValue<Int32>(reader, ordinals.Item4),
		IsNullable = OptionalValue<Boolean>(reader, ordinals.Item5),
		ColumnOrdinal = OptionalValue<Int32>(reader, ordinals.Item6),
		SqlTypeName = RequiredClass<String>(reader, ordinals.Item7),
	};
}

public partial record class GetParametersForObjectRow : IReading<GetParametersForObjectRow, Ordinals9>
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

	static Ordinals9 IReading<GetParametersForObjectRow, Ordinals9>.Ordinals(SqlDataReader reader) => new(
		reader.GetOrdinal("parameter_id"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id"),
		reader.GetOrdinal("name"),
		reader.GetOrdinal("is_output"),
		reader.GetOrdinal("max_length"),
		reader.GetOrdinal("is_table_type"),
		reader.GetOrdinal("Type_Name")
	);

	static GetParametersForObjectRow IReading<GetParametersForObjectRow, Ordinals9>.Read(SqlDataReader reader, Ordinals9 ordinals) => new GetParametersForObjectRow
	{
		ParameterId = RequiredValue<Int32>(reader, ordinals.Item1),
		SchemaId = RequiredValue<Int32>(reader, ordinals.Item2),
		SystemTypeId = RequiredValue<Byte>(reader, ordinals.Item3),
		UserTypeId = RequiredValue<Int32>(reader, ordinals.Item4),
		Name = OptionalClass<String>(reader, ordinals.Item5),
		IsOutput = RequiredValue<Boolean>(reader, ordinals.Item6),
		MaxLength = RequiredValue<Int16>(reader, ordinals.Item7),
		IsTableType = RequiredValue<Boolean>(reader, ordinals.Item8),
		TypeName = RequiredClass<String>(reader, ordinals.Item9),
	};
}

public partial record class GetProcedureForSchemaRow : IReading<GetProcedureForSchemaRow, Ordinals4>
{
	public required String Name { get; init; }
	public required Int32 ObjectId { get; init; }
	public required String SchemaName { get; init; }
	public required String? ObsoleteMessage { get; init; }

	static Ordinals4 IReading<GetProcedureForSchemaRow, Ordinals4>.Ordinals(SqlDataReader reader) => new(
		reader.GetOrdinal("name"),
		reader.GetOrdinal("object_id"),
		reader.GetOrdinal("Schema_Name"),
		reader.GetOrdinal("Obsolete_Message")
	);

	static GetProcedureForSchemaRow IReading<GetProcedureForSchemaRow, Ordinals4>.Read(SqlDataReader reader, Ordinals4 ordinals) => new GetProcedureForSchemaRow
	{
		Name = RequiredClass<String>(reader, ordinals.Item1),
		ObjectId = RequiredValue<Int32>(reader, ordinals.Item2),
		SchemaName = RequiredClass<String>(reader, ordinals.Item3),
		ObsoleteMessage = OptionalClass<String>(reader, ordinals.Item4),
	};
}

public partial record class GetProceduresForSchemaRow : IReading<GetProceduresForSchemaRow, Ordinals4>
{
	public required String Name { get; init; }
	public required Int32 ObjectId { get; init; }
	public required String SchemaName { get; init; }
	public required String? ObsoleteMessage { get; init; }

	static Ordinals4 IReading<GetProceduresForSchemaRow, Ordinals4>.Ordinals(SqlDataReader reader) => new(
		reader.GetOrdinal("name"),
		reader.GetOrdinal("object_id"),
		reader.GetOrdinal("Schema_Name"),
		reader.GetOrdinal("Obsolete_Message")
	);

	static GetProceduresForSchemaRow IReading<GetProceduresForSchemaRow, Ordinals4>.Read(SqlDataReader reader, Ordinals4 ordinals) => new GetProceduresForSchemaRow
	{
		Name = RequiredClass<String>(reader, ordinals.Item1),
		ObjectId = RequiredValue<Int32>(reader, ordinals.Item2),
		SchemaName = RequiredClass<String>(reader, ordinals.Item3),
		ObsoleteMessage = OptionalClass<String>(reader, ordinals.Item4),
	};
}

public partial record class GetSchemasRow : IReading<GetSchemasRow, Ordinals2>
{
	public required String Name { get; init; }
	public required Int32 SchemaId { get; init; }

	static Ordinals2 IReading<GetSchemasRow, Ordinals2>.Ordinals(SqlDataReader reader) => new(
		reader.GetOrdinal("name"),
		reader.GetOrdinal("schema_id")
	);

	static GetSchemasRow IReading<GetSchemasRow, Ordinals2>.Read(SqlDataReader reader, Ordinals2 ordinals) => new GetSchemasRow
	{
		Name = RequiredClass<String>(reader, ordinals.Item1),
		SchemaId = RequiredValue<Int32>(reader, ordinals.Item2),
	};
}

public partial record class GetSysTypeRow : IReading<GetSysTypeRow, Ordinals3>
{
	public required Byte SystemTypeId { get; init; }
	public required Boolean IsTableType { get; init; }
	public required String Name { get; init; }

	static Ordinals3 IReading<GetSysTypeRow, Ordinals3>.Ordinals(SqlDataReader reader) => new(
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("is_table_type"),
		reader.GetOrdinal("name")
	);

	static GetSysTypeRow IReading<GetSysTypeRow, Ordinals3>.Read(SqlDataReader reader, Ordinals3 ordinals) => new GetSysTypeRow
	{
		SystemTypeId = RequiredValue<Byte>(reader, ordinals.Item1),
		IsTableType = RequiredValue<Boolean>(reader, ordinals.Item2),
		Name = RequiredClass<String>(reader, ordinals.Item3),
	};
}

public partial record class GetSysTypesRow : IReading<GetSysTypesRow, Ordinals17>
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

	static Ordinals17 IReading<GetSysTypesRow, Ordinals17>.Ordinals(SqlDataReader reader) => new(
		reader.GetOrdinal("name"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("principal_id"),
		reader.GetOrdinal("max_length"),
		reader.GetOrdinal("precision"),
		reader.GetOrdinal("scale"),
		reader.GetOrdinal("collation_name"),
		reader.GetOrdinal("is_nullable"),
		reader.GetOrdinal("is_user_defined"),
		reader.GetOrdinal("is_assembly_type"),
		reader.GetOrdinal("default_object_id"),
		reader.GetOrdinal("rule_object_id"),
		reader.GetOrdinal("is_table_type"),
		reader.GetOrdinal("schema_name"),
		reader.GetOrdinal("is_from_sys_schema")
	);

	static GetSysTypesRow IReading<GetSysTypesRow, Ordinals17>.Read(SqlDataReader reader, Ordinals17 ordinals) => new GetSysTypesRow
	{
		Name = RequiredClass<String>(reader, ordinals.Item1),
		SystemTypeId = RequiredValue<Byte>(reader, ordinals.Item2),
		UserTypeId = RequiredValue<Int32>(reader, ordinals.Item3),
		SchemaId = RequiredValue<Int32>(reader, ordinals.Item4),
		PrincipalId = OptionalValue<Int32>(reader, ordinals.Item5),
		MaxLength = RequiredValue<Int16>(reader, ordinals.Item6),
		Precision = RequiredValue<Byte>(reader, ordinals.Item7),
		Scale = RequiredValue<Byte>(reader, ordinals.Item8),
		CollationName = OptionalClass<String>(reader, ordinals.Item9),
		IsNullable = OptionalValue<Boolean>(reader, ordinals.Item10),
		IsUserDefined = RequiredValue<Boolean>(reader, ordinals.Item11),
		IsAssemblyType = RequiredValue<Boolean>(reader, ordinals.Item12),
		DefaultObjectId = RequiredValue<Int32>(reader, ordinals.Item13),
		RuleObjectId = RequiredValue<Int32>(reader, ordinals.Item14),
		IsTableType = RequiredValue<Boolean>(reader, ordinals.Item15),
		SchemaName = RequiredClass<String>(reader, ordinals.Item16),
		IsFromSysSchema = RequiredValue<Boolean>(reader, ordinals.Item17),
	};
}

public partial record class GetTableTypeColumnsRow : IReading<GetTableTypeColumnsRow, Ordinals7>
{
	public required Boolean? IsNullable { get; init; }
	public required Int16 MaxLength { get; init; }
	public required String? Name { get; init; }
	public required String TypeName { get; init; }
	public required Int32 SchemaId { get; init; }
	public required Byte SystemTypeId { get; init; }
	public required Int32 UserTypeId { get; init; }

	static Ordinals7 IReading<GetTableTypeColumnsRow, Ordinals7>.Ordinals(SqlDataReader reader) => new(
		reader.GetOrdinal("is_nullable"),
		reader.GetOrdinal("max_length"),
		reader.GetOrdinal("name"),
		reader.GetOrdinal("Type_Name"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id")
	);

	static GetTableTypeColumnsRow IReading<GetTableTypeColumnsRow, Ordinals7>.Read(SqlDataReader reader, Ordinals7 ordinals) => new GetTableTypeColumnsRow
	{
		IsNullable = OptionalValue<Boolean>(reader, ordinals.Item1),
		MaxLength = RequiredValue<Int16>(reader, ordinals.Item2),
		Name = OptionalClass<String>(reader, ordinals.Item3),
		TypeName = RequiredClass<String>(reader, ordinals.Item4),
		SchemaId = RequiredValue<Int32>(reader, ordinals.Item5),
		SystemTypeId = RequiredValue<Byte>(reader, ordinals.Item6),
		UserTypeId = RequiredValue<Int32>(reader, ordinals.Item7),
	};
}

public partial record class GetTableTypesRow : IReading<GetTableTypesRow, Ordinals6>
{
	public required String Name { get; init; }
	public required Int32 TypeTableObjectId { get; init; }
	public required String SchemaName { get; init; }
	public required Int32 SchemaId { get; init; }
	public required Byte SystemTypeId { get; init; }
	public required Int32 UserTypeId { get; init; }

	static Ordinals6 IReading<GetTableTypesRow, Ordinals6>.Ordinals(SqlDataReader reader) => new(
		reader.GetOrdinal("name"),
		reader.GetOrdinal("type_table_object_id"),
		reader.GetOrdinal("Schema_Name"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id")
	);

	static GetTableTypesRow IReading<GetTableTypesRow, Ordinals6>.Read(SqlDataReader reader, Ordinals6 ordinals) => new GetTableTypesRow
	{
		Name = RequiredClass<String>(reader, ordinals.Item1),
		TypeTableObjectId = RequiredValue<Int32>(reader, ordinals.Item2),
		SchemaName = RequiredClass<String>(reader, ordinals.Item3),
		SchemaId = RequiredValue<Int32>(reader, ordinals.Item4),
		SystemTypeId = RequiredValue<Byte>(reader, ordinals.Item5),
		UserTypeId = RequiredValue<Int32>(reader, ordinals.Item6),
	};
}

public partial record class ZTestSelect1Row : IReading<ZTestSelect1Row, Ordinals1>
{
	public required Int32 Blah { get; init; }

	static Ordinals1 IReading<ZTestSelect1Row, Ordinals1>.Ordinals(SqlDataReader reader) => (
		reader.GetOrdinal("Blah")
	);

	static ZTestSelect1Row IReading<ZTestSelect1Row, Ordinals1>.Read(SqlDataReader reader, Ordinals1 ordinal) => new ZTestSelect1Row
	{
		Blah = RequiredValue<Int32>(reader, ordinal),
	};
}

file interface IReading<TRow, OrdinalsTuple>
{
	static abstract TRow Read(SqlDataReader reader, OrdinalsTuple ordinals);
	static abstract OrdinalsTuple Ordinals(SqlDataReader reader);
}

file static class FileMethods
{
	public static T? OptionalClass<T>(SqlDataReader reader, int ordinal) where T : class => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
	public static T? OptionalValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
	public static T RequiredClass<T>(SqlDataReader reader, int ordinal) where T : class => reader.GetFieldValue<T>(ordinal);
	public static T RequiredValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.GetFieldValue<T>(ordinal);

	public static List<TRow> ExecuteCommand<TRow, OrdinalsTuple>(SqlCommand cmd) where TRow : IReading<TRow, OrdinalsTuple>
	{
		var result = new List<TRow>();
		using var reader = cmd.ExecuteReader();
		if (!reader.Read()) { return result; }
		var ords = TRow.Ordinals(reader);
		do { result.Add(TRow.Read(reader, ords)); } while (reader.Read());
		return result;
	}

	public static async Task<List<TRow>> ExecuteCommandAsync<TRow, OrdinalsTuple>(SqlCommand cmd, CancellationToken cancellationToken) where TRow : IReading<TRow, OrdinalsTuple>
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

public partial class Database
{
	private static SqlCommand DmDescribeFirstResultSetCommand(SqlConnection connection, String? text, String? parameters) => CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set(@text, @parameters, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal", [
		CreateParameter("@text", text, SqlDbType.NVarChar, 8000),
		CreateParameter("@parameters", parameters, SqlDbType.NVarChar, 8000),
	]);

	public static List<DmDescribeFirstResultSetRow> DmDescribeFirstResultSet(SqlConnection connection, String? text, String? parameters)
	{
		using var cmd = DmDescribeFirstResultSetCommand(connection, text, parameters);
		return ExecuteCommand<DmDescribeFirstResultSetRow, Ordinals7>(cmd);
	}

	public static async Task<List<DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlConnection connection, String? text, String? parameters, CancellationToken cancellationToken = default)
	{
		using var cmd = DmDescribeFirstResultSetCommand(connection, text, parameters);
		return await ExecuteCommandAsync<DmDescribeFirstResultSetRow, Ordinals7>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand DmDescribeFirstResultSetForObjectCommand(SqlConnection connection, Int32? objectid) => CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set_for_object(@objectid, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal", [
		CreateParameter("@objectid", objectid, SqlDbType.Int),
	]);

	public static List<DmDescribeFirstResultSetForObjectRow> DmDescribeFirstResultSetForObject(SqlConnection connection, Int32? objectid)
	{
		using var cmd = DmDescribeFirstResultSetForObjectCommand(connection, objectid);
		return ExecuteCommand<DmDescribeFirstResultSetForObjectRow, Ordinals7>(cmd);
	}

	public static async Task<List<DmDescribeFirstResultSetForObjectRow>> DmDescribeFirstResultSetForObjectAsync(SqlConnection connection, Int32? objectid, CancellationToken cancellationToken = default)
	{
		using var cmd = DmDescribeFirstResultSetForObjectCommand(connection, objectid);
		return await ExecuteCommandAsync<DmDescribeFirstResultSetForObjectRow, Ordinals7>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand GetParametersForObjectCommand(SqlConnection connection, Int32? id) => CreateStatement(connection, "SELECT P.parameter_id, T.schema_id, P.system_type_id, P.user_type_id, P.name, P.is_output, P.max_length, T.is_table_type, T.name as [Type_Name] FROM sys.parameters AS P JOIN sys.types AS T ON (P.system_type_id = T.system_type_id AND P.user_type_id = T.user_type_id) WHERE P.object_id = @id", [
		CreateParameter("@id", id, SqlDbType.Int),
	]);

	public static List<GetParametersForObjectRow> GetParametersForObject(SqlConnection connection, Int32? id)
	{
		using var cmd = GetParametersForObjectCommand(connection, id);
		return ExecuteCommand<GetParametersForObjectRow, Ordinals9>(cmd);
	}

	public static async Task<List<GetParametersForObjectRow>> GetParametersForObjectAsync(SqlConnection connection, Int32? id, CancellationToken cancellationToken = default)
	{
		using var cmd = GetParametersForObjectCommand(connection, id);
		return await ExecuteCommandAsync<GetParametersForObjectRow, Ordinals9>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand GetProcedureForSchemaCommand(SqlConnection connection, String? schema, String? proc) => CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema AND P.name = @proc ORDER BY P.name", [
		CreateParameter("@schema", schema, SqlDbType.VarChar, 8000),
		CreateParameter("@proc", proc, SqlDbType.VarChar, 8000),
	]);

	public static List<GetProcedureForSchemaRow> GetProcedureForSchema(SqlConnection connection, String? schema, String? proc)
	{
		using var cmd = GetProcedureForSchemaCommand(connection, schema, proc);
		return ExecuteCommand<GetProcedureForSchemaRow, Ordinals4>(cmd);
	}

	public static async Task<List<GetProcedureForSchemaRow>> GetProcedureForSchemaAsync(SqlConnection connection, String? schema, String? proc, CancellationToken cancellationToken = default)
	{
		using var cmd = GetProcedureForSchemaCommand(connection, schema, proc);
		return await ExecuteCommandAsync<GetProcedureForSchemaRow, Ordinals4>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand GetProceduresForSchemaCommand(SqlConnection connection, String? schema) => CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema ORDER BY P.name", [
		CreateParameter("@schema", schema, SqlDbType.VarChar, 8000),
	]);

	public static List<GetProceduresForSchemaRow> GetProceduresForSchema(SqlConnection connection, String? schema)
	{
		using var cmd = GetProceduresForSchemaCommand(connection, schema);
		return ExecuteCommand<GetProceduresForSchemaRow, Ordinals4>(cmd);
	}

	public static async Task<List<GetProceduresForSchemaRow>> GetProceduresForSchemaAsync(SqlConnection connection, String? schema, CancellationToken cancellationToken = default)
	{
		using var cmd = GetProceduresForSchemaCommand(connection, schema);
		return await ExecuteCommandAsync<GetProceduresForSchemaRow, Ordinals4>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand GetSchemasCommand(SqlConnection connection) => CreateStatement(connection, "SELECT name, schema_id FROM sys.schemas");

	public static List<GetSchemasRow> GetSchemas(SqlConnection connection)
	{
		using var cmd = GetSchemasCommand(connection);
		return ExecuteCommand<GetSchemasRow, Ordinals2>(cmd);
	}

	public static async Task<List<GetSchemasRow>> GetSchemasAsync(SqlConnection connection, CancellationToken cancellationToken = default)
	{
		using var cmd = GetSchemasCommand(connection);
		return await ExecuteCommandAsync<GetSchemasRow, Ordinals2>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand GetSysTypeCommand(SqlConnection connection, Int32? id) => CreateStatement(connection, "SELECT system_type_id, is_table_type, name FROM sys.types where system_type_id = @id", [
		CreateParameter("@id", id, SqlDbType.Int),
	]);

	public static List<GetSysTypeRow> GetSysType(SqlConnection connection, Int32? id)
	{
		using var cmd = GetSysTypeCommand(connection, id);
		return ExecuteCommand<GetSysTypeRow, Ordinals3>(cmd);
	}

	public static async Task<List<GetSysTypeRow>> GetSysTypeAsync(SqlConnection connection, Int32? id, CancellationToken cancellationToken = default)
	{
		using var cmd = GetSysTypeCommand(connection, id);
		return await ExecuteCommandAsync<GetSysTypeRow, Ordinals3>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand GetSysTypesCommand(SqlConnection connection) => CreateStatement(connection, "SELECT   T.*,   S.name as [schema_name],   ISNULL(CAST(CASE WHEN S.name = 'sys' THEN 1 ELSE 0 END as bit), 0) AS [is_from_sys_schema] FROM sys.types T JOIN sys.schemas S ON (T.schema_id = S.schema_id)");

	public static List<GetSysTypesRow> GetSysTypes(SqlConnection connection)
	{
		using var cmd = GetSysTypesCommand(connection);
		return ExecuteCommand<GetSysTypesRow, Ordinals17>(cmd);
	}

	public static async Task<List<GetSysTypesRow>> GetSysTypesAsync(SqlConnection connection, CancellationToken cancellationToken = default)
	{
		using var cmd = GetSysTypesCommand(connection);
		return await ExecuteCommandAsync<GetSysTypesRow, Ordinals17>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand GetTableTypeColumnsCommand(SqlConnection connection, Int32? id) => CreateStatement(connection, "SELECT C.is_nullable, C.max_length, C.name, t.name as [Type_Name], T.schema_id, T.system_type_id, T.user_type_id from sys.columns as C join sys.types T ON (C.system_type_id = T.system_type_id) where C.object_id = @id and t.name <> 'sysname' order by c.column_id", [
		CreateParameter("@id", id, SqlDbType.Int),
	]);

	public static List<GetTableTypeColumnsRow> GetTableTypeColumns(SqlConnection connection, Int32? id)
	{
		using var cmd = GetTableTypeColumnsCommand(connection, id);
		return ExecuteCommand<GetTableTypeColumnsRow, Ordinals7>(cmd);
	}

	public static async Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(SqlConnection connection, Int32? id, CancellationToken cancellationToken = default)
	{
		using var cmd = GetTableTypeColumnsCommand(connection, id);
		return await ExecuteCommandAsync<GetTableTypeColumnsRow, Ordinals7>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand GetTableTypesCommand(SqlConnection connection) => CreateStatement(connection, "SELECT T.name, T.type_table_object_id, S.name as [Schema_Name], T.schema_id, T.system_type_id, T.user_type_id FROM sys.table_types AS T INNER JOIN sys.schemas as S ON (T.schema_id = S.schema_id) ORDER BY S.name, T.name");

	public static List<GetTableTypesRow> GetTableTypes(SqlConnection connection)
	{
		using var cmd = GetTableTypesCommand(connection);
		return ExecuteCommand<GetTableTypesRow, Ordinals6>(cmd);
	}

	public static async Task<List<GetTableTypesRow>> GetTableTypesAsync(SqlConnection connection, CancellationToken cancellationToken = default)
	{
		using var cmd = GetTableTypesCommand(connection);
		return await ExecuteCommandAsync<GetTableTypesRow, Ordinals6>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand ZTestSelect1Command(SqlConnection connection) => CreateStatement(connection, "SELECT 1 AS [Blah]");

	public static List<ZTestSelect1Row> ZTestSelect1(SqlConnection connection)
	{
		using var cmd = ZTestSelect1Command(connection);
		return ExecuteCommand<ZTestSelect1Row, Ordinals1>(cmd);
	}

	public static async Task<List<ZTestSelect1Row>> ZTestSelect1Async(SqlConnection connection, CancellationToken cancellationToken = default)
	{
		using var cmd = ZTestSelect1Command(connection);
		return await ExecuteCommandAsync<ZTestSelect1Row, Ordinals1>(cmd, cancellationToken).ConfigureAwait(false);
	}

	private static SqlCommand ZTestUpdate1Command(SqlConnection connection) => CreateStatement(connection, "UPDATE sys.types SET name = 'blah'");

	public static int ZTestUpdate1(SqlConnection connection)
	{
		using var cmd = ZTestUpdate1Command(connection);
		return cmd.ExecuteNonQuery();
	}

	public static async Task<int> ZTestUpdate1Async(SqlConnection connection, CancellationToken cancellationToken = default)
	{
		using var cmd = ZTestUpdate1Command(connection);
		return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
	}

}

