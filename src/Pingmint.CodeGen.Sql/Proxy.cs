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

    private static List<T> ExecuteCommand<T>(SqlCommand cmd, Func<SqlDataReader, int[]> ordinals, Func<SqlDataReader, int[], T> readRow)
	{
		var result = new List<T>();
		using var reader = cmd.ExecuteReader();
		if (!reader.Read()) { return result; }
		var ords = ordinals(reader);
		do { result.Add(readRow(reader, ords)); } while (reader.Read());
		return result;
	}

    private static async Task<List<T>> ExecuteCommandAsync<T>(SqlCommand cmd, Func<SqlDataReader, int[]> ordinals, Func<SqlDataReader, int[], T> readRow)
	{
		var result = new List<T>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (! await reader.ReadAsync().ConfigureAwait(false)) { return result; }
		var ords = ordinals(reader);
		do { result.Add(readRow(reader, ords)); } while (await reader.ReadAsync().ConfigureAwait(false));
		return result;
	}

	private static int[] DmDescribeFirstResultSetOrdinals(SqlDataReader reader) => [
		reader.GetOrdinal("name"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id"),
		reader.GetOrdinal("is_nullable"),
		reader.GetOrdinal("column_ordinal"),
		reader.GetOrdinal("sql_type_name"),
	];

	private static DmDescribeFirstResultSetRow DmDescribeFirstResultSetReadRow(SqlDataReader reader, int[] ords) => new DmDescribeFirstResultSetRow
	{
		Name = OptionalClass<String>(reader, ords[0]),
		SchemaId = RequiredValue<Int32>(reader, ords[1]),
		SystemTypeId = RequiredValue<Byte>(reader, ords[2]),
		UserTypeId = RequiredValue<Int32>(reader, ords[3]),
		IsNullable = OptionalValue<Boolean>(reader, ords[4]),
		ColumnOrdinal = OptionalValue<Int32>(reader, ords[5]),
		SqlTypeName = RequiredClass<String>(reader, ords[6]),
	};

	public static SqlCommand DmDescribeFirstResultSetCommand(SqlConnection connection, String? text, String? parameters)
	{
		SqlCommand cmd = CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set(@text, @parameters, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");
		cmd.Parameters.AddRange([
			CreateParameter("@text", text, SqlDbType.NVarChar, 8000),
			CreateParameter("@parameters", parameters, SqlDbType.NVarChar, 8000),
		]);
		return cmd;
	}

	public static List<DmDescribeFirstResultSetRow> DmDescribeFirstResultSet(SqlCommand cmd) => ExecuteCommand(cmd, DmDescribeFirstResultSetOrdinals, DmDescribeFirstResultSetReadRow);

	public static List<DmDescribeFirstResultSetRow> DmDescribeFirstResultSet(SqlConnection connection, String? text, String? parameters)
	{
		using var cmd = DmDescribeFirstResultSetCommand(connection, text, parameters);
		return DmDescribeFirstResultSet(cmd);
	}

	public static async Task<List<DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlCommand cmd) => await ExecuteCommandAsync(cmd, DmDescribeFirstResultSetOrdinals, DmDescribeFirstResultSetReadRow);

	public static async Task<List<DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlConnection connection, String? text, String? parameters)
	{
		using var cmd = DmDescribeFirstResultSetCommand(connection, text, parameters);
		return await DmDescribeFirstResultSetAsync(cmd);
	}

	private static int[] DmDescribeFirstResultSetForObjectOrdinals(SqlDataReader reader) => [
		reader.GetOrdinal("name"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id"),
		reader.GetOrdinal("is_nullable"),
		reader.GetOrdinal("column_ordinal"),
		reader.GetOrdinal("sql_type_name"),
	];

	private static DmDescribeFirstResultSetForObjectRow DmDescribeFirstResultSetForObjectReadRow(SqlDataReader reader, int[] ords) => new DmDescribeFirstResultSetForObjectRow
	{
		Name = OptionalClass<String>(reader, ords[0]),
		SchemaId = RequiredValue<Int32>(reader, ords[1]),
		SystemTypeId = RequiredValue<Byte>(reader, ords[2]),
		UserTypeId = RequiredValue<Int32>(reader, ords[3]),
		IsNullable = OptionalValue<Boolean>(reader, ords[4]),
		ColumnOrdinal = OptionalValue<Int32>(reader, ords[5]),
		SqlTypeName = RequiredClass<String>(reader, ords[6]),
	};

	public static SqlCommand DmDescribeFirstResultSetForObjectCommand(SqlConnection connection, Int32? objectid)
	{
		SqlCommand cmd = CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set_for_object(@objectid, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");
		cmd.Parameters.AddRange([
			CreateParameter("@objectid", objectid, SqlDbType.Int),
		]);
		return cmd;
	}

	public static List<DmDescribeFirstResultSetForObjectRow> DmDescribeFirstResultSetForObject(SqlCommand cmd) => ExecuteCommand(cmd, DmDescribeFirstResultSetForObjectOrdinals, DmDescribeFirstResultSetForObjectReadRow);

	public static List<DmDescribeFirstResultSetForObjectRow> DmDescribeFirstResultSetForObject(SqlConnection connection, Int32? objectid)
	{
		using var cmd = DmDescribeFirstResultSetForObjectCommand(connection, objectid);
		return DmDescribeFirstResultSetForObject(cmd);
	}

	public static async Task<List<DmDescribeFirstResultSetForObjectRow>> DmDescribeFirstResultSetForObjectAsync(SqlCommand cmd) => await ExecuteCommandAsync(cmd, DmDescribeFirstResultSetForObjectOrdinals, DmDescribeFirstResultSetForObjectReadRow);

	public static async Task<List<DmDescribeFirstResultSetForObjectRow>> DmDescribeFirstResultSetForObjectAsync(SqlConnection connection, Int32? objectid)
	{
		using var cmd = DmDescribeFirstResultSetForObjectCommand(connection, objectid);
		return await DmDescribeFirstResultSetForObjectAsync(cmd);
	}

	private static int[] GetParametersForObjectOrdinals(SqlDataReader reader) => [
		reader.GetOrdinal("parameter_id"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id"),
		reader.GetOrdinal("name"),
		reader.GetOrdinal("is_output"),
		reader.GetOrdinal("max_length"),
		reader.GetOrdinal("is_table_type"),
		reader.GetOrdinal("Type_Name"),
	];

	private static GetParametersForObjectRow GetParametersForObjectReadRow(SqlDataReader reader, int[] ords) => new GetParametersForObjectRow
	{
		ParameterId = RequiredValue<Int32>(reader, ords[0]),
		SchemaId = RequiredValue<Int32>(reader, ords[1]),
		SystemTypeId = RequiredValue<Byte>(reader, ords[2]),
		UserTypeId = RequiredValue<Int32>(reader, ords[3]),
		Name = OptionalClass<String>(reader, ords[4]),
		IsOutput = RequiredValue<Boolean>(reader, ords[5]),
		MaxLength = RequiredValue<Int16>(reader, ords[6]),
		IsTableType = RequiredValue<Boolean>(reader, ords[7]),
		TypeName = RequiredClass<String>(reader, ords[8]),
	};

	public static SqlCommand GetParametersForObjectCommand(SqlConnection connection, Int32? id)
	{
		SqlCommand cmd = CreateStatement(connection, "SELECT P.parameter_id, T.schema_id, P.system_type_id, P.user_type_id, P.name, P.is_output, P.max_length, T.is_table_type, T.name as [Type_Name] FROM sys.parameters AS P JOIN sys.types AS T ON (P.system_type_id = T.system_type_id AND P.user_type_id = T.user_type_id) WHERE P.object_id = @id");
		cmd.Parameters.AddRange([
			CreateParameter("@id", id, SqlDbType.Int),
		]);
		return cmd;
	}

	public static List<GetParametersForObjectRow> GetParametersForObject(SqlCommand cmd) => ExecuteCommand(cmd, GetParametersForObjectOrdinals, GetParametersForObjectReadRow);

	public static List<GetParametersForObjectRow> GetParametersForObject(SqlConnection connection, Int32? id)
	{
		using var cmd = GetParametersForObjectCommand(connection, id);
		return GetParametersForObject(cmd);
	}

	public static async Task<List<GetParametersForObjectRow>> GetParametersForObjectAsync(SqlCommand cmd) => await ExecuteCommandAsync(cmd, GetParametersForObjectOrdinals, GetParametersForObjectReadRow);

	public static async Task<List<GetParametersForObjectRow>> GetParametersForObjectAsync(SqlConnection connection, Int32? id)
	{
		using var cmd = GetParametersForObjectCommand(connection, id);
		return await GetParametersForObjectAsync(cmd);
	}

	private static int[] GetProcedureForSchemaOrdinals(SqlDataReader reader) => [
		reader.GetOrdinal("name"),
		reader.GetOrdinal("object_id"),
		reader.GetOrdinal("Schema_Name"),
		reader.GetOrdinal("Obsolete_Message"),
	];

	private static GetProcedureForSchemaRow GetProcedureForSchemaReadRow(SqlDataReader reader, int[] ords) => new GetProcedureForSchemaRow
	{
		Name = RequiredClass<String>(reader, ords[0]),
		ObjectId = RequiredValue<Int32>(reader, ords[1]),
		SchemaName = RequiredClass<String>(reader, ords[2]),
		ObsoleteMessage = OptionalClass<String>(reader, ords[3]),
	};

	public static SqlCommand GetProcedureForSchemaCommand(SqlConnection connection, String? schema, String? proc)
	{
		SqlCommand cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema AND P.name = @proc ORDER BY P.name");
		cmd.Parameters.AddRange([
			CreateParameter("@schema", schema, SqlDbType.VarChar, 8000),
			CreateParameter("@proc", proc, SqlDbType.VarChar, 8000),
		]);
		return cmd;
	}

	public static List<GetProcedureForSchemaRow> GetProcedureForSchema(SqlCommand cmd) => ExecuteCommand(cmd, GetProcedureForSchemaOrdinals, GetProcedureForSchemaReadRow);

	public static List<GetProcedureForSchemaRow> GetProcedureForSchema(SqlConnection connection, String? schema, String? proc)
	{
		using var cmd = GetProcedureForSchemaCommand(connection, schema, proc);
		return GetProcedureForSchema(cmd);
	}

	public static async Task<List<GetProcedureForSchemaRow>> GetProcedureForSchemaAsync(SqlCommand cmd) => await ExecuteCommandAsync(cmd, GetProcedureForSchemaOrdinals, GetProcedureForSchemaReadRow);

	public static async Task<List<GetProcedureForSchemaRow>> GetProcedureForSchemaAsync(SqlConnection connection, String? schema, String? proc)
	{
		using var cmd = GetProcedureForSchemaCommand(connection, schema, proc);
		return await GetProcedureForSchemaAsync(cmd);
	}

	private static int[] GetProceduresForSchemaOrdinals(SqlDataReader reader) => [
		reader.GetOrdinal("name"),
		reader.GetOrdinal("object_id"),
		reader.GetOrdinal("Schema_Name"),
		reader.GetOrdinal("Obsolete_Message"),
	];

	private static GetProceduresForSchemaRow GetProceduresForSchemaReadRow(SqlDataReader reader, int[] ords) => new GetProceduresForSchemaRow
	{
		Name = RequiredClass<String>(reader, ords[0]),
		ObjectId = RequiredValue<Int32>(reader, ords[1]),
		SchemaName = RequiredClass<String>(reader, ords[2]),
		ObsoleteMessage = OptionalClass<String>(reader, ords[3]),
	};

	public static SqlCommand GetProceduresForSchemaCommand(SqlConnection connection, String? schema)
	{
		SqlCommand cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema ORDER BY P.name");
		cmd.Parameters.AddRange([
			CreateParameter("@schema", schema, SqlDbType.VarChar, 8000),
		]);
		return cmd;
	}

	public static List<GetProceduresForSchemaRow> GetProceduresForSchema(SqlCommand cmd) => ExecuteCommand(cmd, GetProceduresForSchemaOrdinals, GetProceduresForSchemaReadRow);

	public static List<GetProceduresForSchemaRow> GetProceduresForSchema(SqlConnection connection, String? schema)
	{
		using var cmd = GetProceduresForSchemaCommand(connection, schema);
		return GetProceduresForSchema(cmd);
	}

	public static async Task<List<GetProceduresForSchemaRow>> GetProceduresForSchemaAsync(SqlCommand cmd) => await ExecuteCommandAsync(cmd, GetProceduresForSchemaOrdinals, GetProceduresForSchemaReadRow);

	public static async Task<List<GetProceduresForSchemaRow>> GetProceduresForSchemaAsync(SqlConnection connection, String? schema)
	{
		using var cmd = GetProceduresForSchemaCommand(connection, schema);
		return await GetProceduresForSchemaAsync(cmd);
	}

	private static int[] GetSchemasOrdinals(SqlDataReader reader) => [
		reader.GetOrdinal("name"),
		reader.GetOrdinal("schema_id"),
	];

	private static GetSchemasRow GetSchemasReadRow(SqlDataReader reader, int[] ords) => new GetSchemasRow
	{
		Name = RequiredClass<String>(reader, ords[0]),
		SchemaId = RequiredValue<Int32>(reader, ords[1]),
	};

	public static SqlCommand GetSchemasCommand(SqlConnection connection) => CreateStatement(connection, "SELECT name, schema_id FROM sys.schemas");

	public static List<GetSchemasRow> GetSchemas(SqlCommand cmd) => ExecuteCommand(cmd, GetSchemasOrdinals, GetSchemasReadRow);

	public static List<GetSchemasRow> GetSchemas(SqlConnection connection)
	{
		using var cmd = GetSchemasCommand(connection);
		return GetSchemas(cmd);
	}

	public static async Task<List<GetSchemasRow>> GetSchemasAsync(SqlCommand cmd) => await ExecuteCommandAsync(cmd, GetSchemasOrdinals, GetSchemasReadRow);

	public static async Task<List<GetSchemasRow>> GetSchemasAsync(SqlConnection connection)
	{
		using var cmd = GetSchemasCommand(connection);
		return await GetSchemasAsync(cmd);
	}

	private static int[] GetSysTypeOrdinals(SqlDataReader reader) => [
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("is_table_type"),
		reader.GetOrdinal("name"),
	];

	private static GetSysTypeRow GetSysTypeReadRow(SqlDataReader reader, int[] ords) => new GetSysTypeRow
	{
		SystemTypeId = RequiredValue<Byte>(reader, ords[0]),
		IsTableType = RequiredValue<Boolean>(reader, ords[1]),
		Name = RequiredClass<String>(reader, ords[2]),
	};

	public static SqlCommand GetSysTypeCommand(SqlConnection connection, Int32? id)
	{
		SqlCommand cmd = CreateStatement(connection, "SELECT system_type_id, is_table_type, name FROM sys.types where system_type_id = @id");
		cmd.Parameters.AddRange([
			CreateParameter("@id", id, SqlDbType.Int),
		]);
		return cmd;
	}

	public static List<GetSysTypeRow> GetSysType(SqlCommand cmd) => ExecuteCommand(cmd, GetSysTypeOrdinals, GetSysTypeReadRow);

	public static List<GetSysTypeRow> GetSysType(SqlConnection connection, Int32? id)
	{
		using var cmd = GetSysTypeCommand(connection, id);
		return GetSysType(cmd);
	}

	public static async Task<List<GetSysTypeRow>> GetSysTypeAsync(SqlCommand cmd) => await ExecuteCommandAsync(cmd, GetSysTypeOrdinals, GetSysTypeReadRow);

	public static async Task<List<GetSysTypeRow>> GetSysTypeAsync(SqlConnection connection, Int32? id)
	{
		using var cmd = GetSysTypeCommand(connection, id);
		return await GetSysTypeAsync(cmd);
	}

	private static int[] GetSysTypesOrdinals(SqlDataReader reader) => [
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
		reader.GetOrdinal("is_from_sys_schema"),
	];

	private static GetSysTypesRow GetSysTypesReadRow(SqlDataReader reader, int[] ords) => new GetSysTypesRow
	{
		Name = RequiredClass<String>(reader, ords[0]),
		SystemTypeId = RequiredValue<Byte>(reader, ords[1]),
		UserTypeId = RequiredValue<Int32>(reader, ords[2]),
		SchemaId = RequiredValue<Int32>(reader, ords[3]),
		PrincipalId = OptionalValue<Int32>(reader, ords[4]),
		MaxLength = RequiredValue<Int16>(reader, ords[5]),
		Precision = RequiredValue<Byte>(reader, ords[6]),
		Scale = RequiredValue<Byte>(reader, ords[7]),
		CollationName = OptionalClass<String>(reader, ords[8]),
		IsNullable = OptionalValue<Boolean>(reader, ords[9]),
		IsUserDefined = RequiredValue<Boolean>(reader, ords[10]),
		IsAssemblyType = RequiredValue<Boolean>(reader, ords[11]),
		DefaultObjectId = RequiredValue<Int32>(reader, ords[12]),
		RuleObjectId = RequiredValue<Int32>(reader, ords[13]),
		IsTableType = RequiredValue<Boolean>(reader, ords[14]),
		SchemaName = RequiredClass<String>(reader, ords[15]),
		IsFromSysSchema = RequiredValue<Boolean>(reader, ords[16]),
	};

	public static SqlCommand GetSysTypesCommand(SqlConnection connection) => CreateStatement(connection, "SELECT   T.*,   S.name as [schema_name],   ISNULL(CAST(CASE WHEN S.name = 'sys' THEN 1 ELSE 0 END as bit), 0) AS [is_from_sys_schema] FROM sys.types T JOIN sys.schemas S ON (T.schema_id = S.schema_id)");

	public static List<GetSysTypesRow> GetSysTypes(SqlCommand cmd) => ExecuteCommand(cmd, GetSysTypesOrdinals, GetSysTypesReadRow);

	public static List<GetSysTypesRow> GetSysTypes(SqlConnection connection)
	{
		using var cmd = GetSysTypesCommand(connection);
		return GetSysTypes(cmd);
	}

	public static async Task<List<GetSysTypesRow>> GetSysTypesAsync(SqlCommand cmd) => await ExecuteCommandAsync(cmd, GetSysTypesOrdinals, GetSysTypesReadRow);

	public static async Task<List<GetSysTypesRow>> GetSysTypesAsync(SqlConnection connection)
	{
		using var cmd = GetSysTypesCommand(connection);
		return await GetSysTypesAsync(cmd);
	}

	private static int[] GetTableTypeColumnsOrdinals(SqlDataReader reader) => [
		reader.GetOrdinal("is_nullable"),
		reader.GetOrdinal("max_length"),
		reader.GetOrdinal("name"),
		reader.GetOrdinal("Type_Name"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id"),
	];

	private static GetTableTypeColumnsRow GetTableTypeColumnsReadRow(SqlDataReader reader, int[] ords) => new GetTableTypeColumnsRow
	{
		IsNullable = OptionalValue<Boolean>(reader, ords[0]),
		MaxLength = RequiredValue<Int16>(reader, ords[1]),
		Name = OptionalClass<String>(reader, ords[2]),
		TypeName = RequiredClass<String>(reader, ords[3]),
		SchemaId = RequiredValue<Int32>(reader, ords[4]),
		SystemTypeId = RequiredValue<Byte>(reader, ords[5]),
		UserTypeId = RequiredValue<Int32>(reader, ords[6]),
	};

	public static SqlCommand GetTableTypeColumnsCommand(SqlConnection connection, Int32? id)
	{
		SqlCommand cmd = CreateStatement(connection, "SELECT C.is_nullable, C.max_length, C.name, t.name as [Type_Name], T.schema_id, T.system_type_id, T.user_type_id from sys.columns as C join sys.types T ON (C.system_type_id = T.system_type_id) where C.object_id = @id and t.name <> 'sysname' order by c.column_id");
		cmd.Parameters.AddRange([
			CreateParameter("@id", id, SqlDbType.Int),
		]);
		return cmd;
	}

	public static List<GetTableTypeColumnsRow> GetTableTypeColumns(SqlCommand cmd) => ExecuteCommand(cmd, GetTableTypeColumnsOrdinals, GetTableTypeColumnsReadRow);

	public static List<GetTableTypeColumnsRow> GetTableTypeColumns(SqlConnection connection, Int32? id)
	{
		using var cmd = GetTableTypeColumnsCommand(connection, id);
		return GetTableTypeColumns(cmd);
	}

	public static async Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(SqlCommand cmd) => await ExecuteCommandAsync(cmd, GetTableTypeColumnsOrdinals, GetTableTypeColumnsReadRow);

	public static async Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(SqlConnection connection, Int32? id)
	{
		using var cmd = GetTableTypeColumnsCommand(connection, id);
		return await GetTableTypeColumnsAsync(cmd);
	}

	private static int[] GetTableTypesOrdinals(SqlDataReader reader) => [
		reader.GetOrdinal("name"),
		reader.GetOrdinal("type_table_object_id"),
		reader.GetOrdinal("Schema_Name"),
		reader.GetOrdinal("schema_id"),
		reader.GetOrdinal("system_type_id"),
		reader.GetOrdinal("user_type_id"),
	];

	private static GetTableTypesRow GetTableTypesReadRow(SqlDataReader reader, int[] ords) => new GetTableTypesRow
	{
		Name = RequiredClass<String>(reader, ords[0]),
		TypeTableObjectId = RequiredValue<Int32>(reader, ords[1]),
		SchemaName = RequiredClass<String>(reader, ords[2]),
		SchemaId = RequiredValue<Int32>(reader, ords[3]),
		SystemTypeId = RequiredValue<Byte>(reader, ords[4]),
		UserTypeId = RequiredValue<Int32>(reader, ords[5]),
	};

	public static SqlCommand GetTableTypesCommand(SqlConnection connection) => CreateStatement(connection, "SELECT T.name, T.type_table_object_id, S.name as [Schema_Name], T.schema_id, T.system_type_id, T.user_type_id FROM sys.table_types AS T INNER JOIN sys.schemas as S ON (T.schema_id = S.schema_id) ORDER BY S.name, T.name");

	public static List<GetTableTypesRow> GetTableTypes(SqlCommand cmd) => ExecuteCommand(cmd, GetTableTypesOrdinals, GetTableTypesReadRow);

	public static List<GetTableTypesRow> GetTableTypes(SqlConnection connection)
	{
		using var cmd = GetTableTypesCommand(connection);
		return GetTableTypes(cmd);
	}

	public static async Task<List<GetTableTypesRow>> GetTableTypesAsync(SqlCommand cmd) => await ExecuteCommandAsync(cmd, GetTableTypesOrdinals, GetTableTypesReadRow);

	public static async Task<List<GetTableTypesRow>> GetTableTypesAsync(SqlConnection connection)
	{
		using var cmd = GetTableTypesCommand(connection);
		return await GetTableTypesAsync(cmd);
	}

}
