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

	public static List<DmDescribeFirstResultSetRow> DmDescribeFirstResultSet(SqlConnection connection, String text, String parameters)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set(@text, @parameters, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");

		cmd.Parameters.Add(CreateParameter("@text", text, SqlDbType.NVarChar, 8000));
		cmd.Parameters.Add(CreateParameter("@parameters", parameters, SqlDbType.NVarChar, 8000));

		var result = new List<DmDescribeFirstResultSetRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			int ordName = reader.GetOrdinal("name");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordColumnOrdinal = reader.GetOrdinal("column_ordinal");
			int ordSqlTypeName = reader.GetOrdinal("sql_type_name");

			do
			{
				result.Add(new DmDescribeFirstResultSetRow
				{
					Name = OptionalClass<String>(reader, ordName),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
					IsNullable = OptionalValue<Boolean>(reader, ordIsNullable),
					ColumnOrdinal = OptionalValue<Int32>(reader, ordColumnOrdinal),
					SqlTypeName = RequiredClass<String>(reader, ordSqlTypeName),
				});
			} while (reader.Read());
		}
		return result;
	}

	public static async Task<List<DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlConnection connection, String text, String parameters)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set(@text, @parameters, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");

		cmd.Parameters.Add(CreateParameter("@text", text, SqlDbType.NVarChar, 8000));
		cmd.Parameters.Add(CreateParameter("@parameters", parameters, SqlDbType.NVarChar, 8000));

		var result = new List<DmDescribeFirstResultSetRow>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (await reader.ReadAsync().ConfigureAwait(false))
		{
			int ordName = reader.GetOrdinal("name");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordColumnOrdinal = reader.GetOrdinal("column_ordinal");
			int ordSqlTypeName = reader.GetOrdinal("sql_type_name");

			do
			{
				result.Add(new DmDescribeFirstResultSetRow
				{
					Name = OptionalClass<String>(reader, ordName),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
					IsNullable = OptionalValue<Boolean>(reader, ordIsNullable),
					ColumnOrdinal = OptionalValue<Int32>(reader, ordColumnOrdinal),
					SqlTypeName = RequiredClass<String>(reader, ordSqlTypeName),
				});
			} while (await reader.ReadAsync().ConfigureAwait(false));
		}
		return result;
	}

	public static List<DmDescribeFirstResultSetForObjectRow> DmDescribeFirstResultSetForObject(SqlConnection connection, Int32 objectid)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set_for_object(@objectid, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");

		cmd.Parameters.Add(CreateParameter("@objectid", objectid, SqlDbType.Int));

		var result = new List<DmDescribeFirstResultSetForObjectRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			int ordName = reader.GetOrdinal("name");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordColumnOrdinal = reader.GetOrdinal("column_ordinal");
			int ordSqlTypeName = reader.GetOrdinal("sql_type_name");

			do
			{
				result.Add(new DmDescribeFirstResultSetForObjectRow
				{
					Name = OptionalClass<String>(reader, ordName),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
					IsNullable = OptionalValue<Boolean>(reader, ordIsNullable),
					ColumnOrdinal = OptionalValue<Int32>(reader, ordColumnOrdinal),
					SqlTypeName = RequiredClass<String>(reader, ordSqlTypeName),
				});
			} while (reader.Read());
		}
		return result;
	}

	public static async Task<List<DmDescribeFirstResultSetForObjectRow>> DmDescribeFirstResultSetForObjectAsync(SqlConnection connection, Int32 objectid)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set_for_object(@objectid, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");

		cmd.Parameters.Add(CreateParameter("@objectid", objectid, SqlDbType.Int));

		var result = new List<DmDescribeFirstResultSetForObjectRow>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (await reader.ReadAsync().ConfigureAwait(false))
		{
			int ordName = reader.GetOrdinal("name");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordColumnOrdinal = reader.GetOrdinal("column_ordinal");
			int ordSqlTypeName = reader.GetOrdinal("sql_type_name");

			do
			{
				result.Add(new DmDescribeFirstResultSetForObjectRow
				{
					Name = OptionalClass<String>(reader, ordName),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
					IsNullable = OptionalValue<Boolean>(reader, ordIsNullable),
					ColumnOrdinal = OptionalValue<Int32>(reader, ordColumnOrdinal),
					SqlTypeName = RequiredClass<String>(reader, ordSqlTypeName),
				});
			} while (await reader.ReadAsync().ConfigureAwait(false));
		}
		return result;
	}

	public static List<GetParametersForObjectRow> GetParametersForObject(SqlConnection connection, Int32 id)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.parameter_id, T.schema_id, P.system_type_id, P.user_type_id, P.name, P.is_output, P.max_length, T.is_table_type, T.name as [Type_Name] FROM sys.parameters AS P JOIN sys.types AS T ON (P.system_type_id = T.system_type_id AND P.user_type_id = T.user_type_id) WHERE P.object_id = @id");

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<GetParametersForObjectRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			int ordParameterId = reader.GetOrdinal("parameter_id");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");
			int ordName = reader.GetOrdinal("name");
			int ordIsOutput = reader.GetOrdinal("is_output");
			int ordMaxLength = reader.GetOrdinal("max_length");
			int ordIsTableType = reader.GetOrdinal("is_table_type");
			int ordTypeName = reader.GetOrdinal("Type_Name");

			do
			{
				result.Add(new GetParametersForObjectRow
				{
					ParameterId = RequiredValue<Int32>(reader, ordParameterId),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
					Name = OptionalClass<String>(reader, ordName),
					IsOutput = RequiredValue<Boolean>(reader, ordIsOutput),
					MaxLength = RequiredValue<Int16>(reader, ordMaxLength),
					IsTableType = RequiredValue<Boolean>(reader, ordIsTableType),
					TypeName = RequiredClass<String>(reader, ordTypeName),
				});
			} while (reader.Read());
		}
		return result;
	}

	public static async Task<List<GetParametersForObjectRow>> GetParametersForObjectAsync(SqlConnection connection, Int32 id)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.parameter_id, T.schema_id, P.system_type_id, P.user_type_id, P.name, P.is_output, P.max_length, T.is_table_type, T.name as [Type_Name] FROM sys.parameters AS P JOIN sys.types AS T ON (P.system_type_id = T.system_type_id AND P.user_type_id = T.user_type_id) WHERE P.object_id = @id");

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<GetParametersForObjectRow>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (await reader.ReadAsync().ConfigureAwait(false))
		{
			int ordParameterId = reader.GetOrdinal("parameter_id");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");
			int ordName = reader.GetOrdinal("name");
			int ordIsOutput = reader.GetOrdinal("is_output");
			int ordMaxLength = reader.GetOrdinal("max_length");
			int ordIsTableType = reader.GetOrdinal("is_table_type");
			int ordTypeName = reader.GetOrdinal("Type_Name");

			do
			{
				result.Add(new GetParametersForObjectRow
				{
					ParameterId = RequiredValue<Int32>(reader, ordParameterId),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
					Name = OptionalClass<String>(reader, ordName),
					IsOutput = RequiredValue<Boolean>(reader, ordIsOutput),
					MaxLength = RequiredValue<Int16>(reader, ordMaxLength),
					IsTableType = RequiredValue<Boolean>(reader, ordIsTableType),
					TypeName = RequiredClass<String>(reader, ordTypeName),
				});
			} while (await reader.ReadAsync().ConfigureAwait(false));
		}
		return result;
	}

	public static List<GetProcedureForSchemaRow> GetProcedureForSchema(SqlConnection connection, String schema, String proc)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema AND P.name = @proc ORDER BY P.name");

		cmd.Parameters.Add(CreateParameter("@schema", schema, SqlDbType.VarChar, 8000));
		cmd.Parameters.Add(CreateParameter("@proc", proc, SqlDbType.VarChar, 8000));

		var result = new List<GetProcedureForSchemaRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			int ordName = reader.GetOrdinal("name");
			int ordObjectId = reader.GetOrdinal("object_id");
			int ordSchemaName = reader.GetOrdinal("Schema_Name");
			int ordObsoleteMessage = reader.GetOrdinal("Obsolete_Message");

			do
			{
				result.Add(new GetProcedureForSchemaRow
				{
					Name = RequiredClass<String>(reader, ordName),
					ObjectId = RequiredValue<Int32>(reader, ordObjectId),
					SchemaName = RequiredClass<String>(reader, ordSchemaName),
					ObsoleteMessage = OptionalClass<String>(reader, ordObsoleteMessage),
				});
			} while (reader.Read());
		}
		return result;
	}

	public static async Task<List<GetProcedureForSchemaRow>> GetProcedureForSchemaAsync(SqlConnection connection, String schema, String proc)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema AND P.name = @proc ORDER BY P.name");

		cmd.Parameters.Add(CreateParameter("@schema", schema, SqlDbType.VarChar, 8000));
		cmd.Parameters.Add(CreateParameter("@proc", proc, SqlDbType.VarChar, 8000));

		var result = new List<GetProcedureForSchemaRow>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (await reader.ReadAsync().ConfigureAwait(false))
		{
			int ordName = reader.GetOrdinal("name");
			int ordObjectId = reader.GetOrdinal("object_id");
			int ordSchemaName = reader.GetOrdinal("Schema_Name");
			int ordObsoleteMessage = reader.GetOrdinal("Obsolete_Message");

			do
			{
				result.Add(new GetProcedureForSchemaRow
				{
					Name = RequiredClass<String>(reader, ordName),
					ObjectId = RequiredValue<Int32>(reader, ordObjectId),
					SchemaName = RequiredClass<String>(reader, ordSchemaName),
					ObsoleteMessage = OptionalClass<String>(reader, ordObsoleteMessage),
				});
			} while (await reader.ReadAsync().ConfigureAwait(false));
		}
		return result;
	}

	public static List<GetProceduresForSchemaRow> GetProceduresForSchema(SqlConnection connection, String schema)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema ORDER BY P.name");

		cmd.Parameters.Add(CreateParameter("@schema", schema, SqlDbType.VarChar, 8000));

		var result = new List<GetProceduresForSchemaRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			int ordName = reader.GetOrdinal("name");
			int ordObjectId = reader.GetOrdinal("object_id");
			int ordSchemaName = reader.GetOrdinal("Schema_Name");
			int ordObsoleteMessage = reader.GetOrdinal("Obsolete_Message");

			do
			{
				result.Add(new GetProceduresForSchemaRow
				{
					Name = RequiredClass<String>(reader, ordName),
					ObjectId = RequiredValue<Int32>(reader, ordObjectId),
					SchemaName = RequiredClass<String>(reader, ordSchemaName),
					ObsoleteMessage = OptionalClass<String>(reader, ordObsoleteMessage),
				});
			} while (reader.Read());
		}
		return result;
	}

	public static async Task<List<GetProceduresForSchemaRow>> GetProceduresForSchemaAsync(SqlConnection connection, String schema)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema ORDER BY P.name");

		cmd.Parameters.Add(CreateParameter("@schema", schema, SqlDbType.VarChar, 8000));

		var result = new List<GetProceduresForSchemaRow>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (await reader.ReadAsync().ConfigureAwait(false))
		{
			int ordName = reader.GetOrdinal("name");
			int ordObjectId = reader.GetOrdinal("object_id");
			int ordSchemaName = reader.GetOrdinal("Schema_Name");
			int ordObsoleteMessage = reader.GetOrdinal("Obsolete_Message");

			do
			{
				result.Add(new GetProceduresForSchemaRow
				{
					Name = RequiredClass<String>(reader, ordName),
					ObjectId = RequiredValue<Int32>(reader, ordObjectId),
					SchemaName = RequiredClass<String>(reader, ordSchemaName),
					ObsoleteMessage = OptionalClass<String>(reader, ordObsoleteMessage),
				});
			} while (await reader.ReadAsync().ConfigureAwait(false));
		}
		return result;
	}

	public static List<GetSchemasRow> GetSchemas(SqlConnection connection)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT name, schema_id FROM sys.schemas");

		var result = new List<GetSchemasRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			int ordName = reader.GetOrdinal("name");
			int ordSchemaId = reader.GetOrdinal("schema_id");

			do
			{
				result.Add(new GetSchemasRow
				{
					Name = RequiredClass<String>(reader, ordName),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
				});
			} while (reader.Read());
		}
		return result;
	}

	public static async Task<List<GetSchemasRow>> GetSchemasAsync(SqlConnection connection)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT name, schema_id FROM sys.schemas");

		var result = new List<GetSchemasRow>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (await reader.ReadAsync().ConfigureAwait(false))
		{
			int ordName = reader.GetOrdinal("name");
			int ordSchemaId = reader.GetOrdinal("schema_id");

			do
			{
				result.Add(new GetSchemasRow
				{
					Name = RequiredClass<String>(reader, ordName),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
				});
			} while (await reader.ReadAsync().ConfigureAwait(false));
		}
		return result;
	}

	public static List<GetSysTypeRow> GetSysType(SqlConnection connection, Int32 id)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT system_type_id, is_table_type, name FROM sys.types where system_type_id = @id");

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<GetSysTypeRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordIsTableType = reader.GetOrdinal("is_table_type");
			int ordName = reader.GetOrdinal("name");

			do
			{
				result.Add(new GetSysTypeRow
				{
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					IsTableType = RequiredValue<Boolean>(reader, ordIsTableType),
					Name = RequiredClass<String>(reader, ordName),
				});
			} while (reader.Read());
		}
		return result;
	}

	public static async Task<List<GetSysTypeRow>> GetSysTypeAsync(SqlConnection connection, Int32 id)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT system_type_id, is_table_type, name FROM sys.types where system_type_id = @id");

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<GetSysTypeRow>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (await reader.ReadAsync().ConfigureAwait(false))
		{
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordIsTableType = reader.GetOrdinal("is_table_type");
			int ordName = reader.GetOrdinal("name");

			do
			{
				result.Add(new GetSysTypeRow
				{
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					IsTableType = RequiredValue<Boolean>(reader, ordIsTableType),
					Name = RequiredClass<String>(reader, ordName),
				});
			} while (await reader.ReadAsync().ConfigureAwait(false));
		}
		return result;
	}

	public static List<GetSysTypesRow> GetSysTypes(SqlConnection connection)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT   T.*,   S.name as [schema_name],   ISNULL(CAST(CASE WHEN S.name = 'sys' THEN 1 ELSE 0 END as bit), 0) AS [is_from_sys_schema] FROM sys.types T JOIN sys.schemas S ON (T.schema_id = S.schema_id)");

		var result = new List<GetSysTypesRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			int ordName = reader.GetOrdinal("name");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordPrincipalId = reader.GetOrdinal("principal_id");
			int ordMaxLength = reader.GetOrdinal("max_length");
			int ordPrecision = reader.GetOrdinal("precision");
			int ordScale = reader.GetOrdinal("scale");
			int ordCollationName = reader.GetOrdinal("collation_name");
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordIsUserDefined = reader.GetOrdinal("is_user_defined");
			int ordIsAssemblyType = reader.GetOrdinal("is_assembly_type");
			int ordDefaultObjectId = reader.GetOrdinal("default_object_id");
			int ordRuleObjectId = reader.GetOrdinal("rule_object_id");
			int ordIsTableType = reader.GetOrdinal("is_table_type");
			int ordSchemaName = reader.GetOrdinal("schema_name");
			int ordIsFromSysSchema = reader.GetOrdinal("is_from_sys_schema");

			do
			{
				result.Add(new GetSysTypesRow
				{
					Name = RequiredClass<String>(reader, ordName),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					PrincipalId = OptionalValue<Int32>(reader, ordPrincipalId),
					MaxLength = RequiredValue<Int16>(reader, ordMaxLength),
					Precision = RequiredValue<Byte>(reader, ordPrecision),
					Scale = RequiredValue<Byte>(reader, ordScale),
					CollationName = OptionalClass<String>(reader, ordCollationName),
					IsNullable = OptionalValue<Boolean>(reader, ordIsNullable),
					IsUserDefined = RequiredValue<Boolean>(reader, ordIsUserDefined),
					IsAssemblyType = RequiredValue<Boolean>(reader, ordIsAssemblyType),
					DefaultObjectId = RequiredValue<Int32>(reader, ordDefaultObjectId),
					RuleObjectId = RequiredValue<Int32>(reader, ordRuleObjectId),
					IsTableType = RequiredValue<Boolean>(reader, ordIsTableType),
					SchemaName = RequiredClass<String>(reader, ordSchemaName),
					IsFromSysSchema = RequiredValue<Boolean>(reader, ordIsFromSysSchema),
				});
			} while (reader.Read());
		}
		return result;
	}

	public static async Task<List<GetSysTypesRow>> GetSysTypesAsync(SqlConnection connection)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT   T.*,   S.name as [schema_name],   ISNULL(CAST(CASE WHEN S.name = 'sys' THEN 1 ELSE 0 END as bit), 0) AS [is_from_sys_schema] FROM sys.types T JOIN sys.schemas S ON (T.schema_id = S.schema_id)");

		var result = new List<GetSysTypesRow>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (await reader.ReadAsync().ConfigureAwait(false))
		{
			int ordName = reader.GetOrdinal("name");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordPrincipalId = reader.GetOrdinal("principal_id");
			int ordMaxLength = reader.GetOrdinal("max_length");
			int ordPrecision = reader.GetOrdinal("precision");
			int ordScale = reader.GetOrdinal("scale");
			int ordCollationName = reader.GetOrdinal("collation_name");
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordIsUserDefined = reader.GetOrdinal("is_user_defined");
			int ordIsAssemblyType = reader.GetOrdinal("is_assembly_type");
			int ordDefaultObjectId = reader.GetOrdinal("default_object_id");
			int ordRuleObjectId = reader.GetOrdinal("rule_object_id");
			int ordIsTableType = reader.GetOrdinal("is_table_type");
			int ordSchemaName = reader.GetOrdinal("schema_name");
			int ordIsFromSysSchema = reader.GetOrdinal("is_from_sys_schema");

			do
			{
				result.Add(new GetSysTypesRow
				{
					Name = RequiredClass<String>(reader, ordName),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					PrincipalId = OptionalValue<Int32>(reader, ordPrincipalId),
					MaxLength = RequiredValue<Int16>(reader, ordMaxLength),
					Precision = RequiredValue<Byte>(reader, ordPrecision),
					Scale = RequiredValue<Byte>(reader, ordScale),
					CollationName = OptionalClass<String>(reader, ordCollationName),
					IsNullable = OptionalValue<Boolean>(reader, ordIsNullable),
					IsUserDefined = RequiredValue<Boolean>(reader, ordIsUserDefined),
					IsAssemblyType = RequiredValue<Boolean>(reader, ordIsAssemblyType),
					DefaultObjectId = RequiredValue<Int32>(reader, ordDefaultObjectId),
					RuleObjectId = RequiredValue<Int32>(reader, ordRuleObjectId),
					IsTableType = RequiredValue<Boolean>(reader, ordIsTableType),
					SchemaName = RequiredClass<String>(reader, ordSchemaName),
					IsFromSysSchema = RequiredValue<Boolean>(reader, ordIsFromSysSchema),
				});
			} while (await reader.ReadAsync().ConfigureAwait(false));
		}
		return result;
	}

	public static List<GetTableTypeColumnsRow> GetTableTypeColumns(SqlConnection connection, Int32 id)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT C.is_nullable, C.max_length, C.name, t.name as [Type_Name], T.schema_id, T.system_type_id, T.user_type_id from sys.columns as C join sys.types T ON (C.system_type_id = T.system_type_id) where C.object_id = @id and t.name <> 'sysname' order by c.column_id");

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<GetTableTypeColumnsRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordMaxLength = reader.GetOrdinal("max_length");
			int ordName = reader.GetOrdinal("name");
			int ordTypeName = reader.GetOrdinal("Type_Name");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");

			do
			{
				result.Add(new GetTableTypeColumnsRow
				{
					IsNullable = OptionalValue<Boolean>(reader, ordIsNullable),
					MaxLength = RequiredValue<Int16>(reader, ordMaxLength),
					Name = OptionalClass<String>(reader, ordName),
					TypeName = RequiredClass<String>(reader, ordTypeName),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
				});
			} while (reader.Read());
		}
		return result;
	}

	public static async Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(SqlConnection connection, Int32 id)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT C.is_nullable, C.max_length, C.name, t.name as [Type_Name], T.schema_id, T.system_type_id, T.user_type_id from sys.columns as C join sys.types T ON (C.system_type_id = T.system_type_id) where C.object_id = @id and t.name <> 'sysname' order by c.column_id");

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<GetTableTypeColumnsRow>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (await reader.ReadAsync().ConfigureAwait(false))
		{
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordMaxLength = reader.GetOrdinal("max_length");
			int ordName = reader.GetOrdinal("name");
			int ordTypeName = reader.GetOrdinal("Type_Name");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");

			do
			{
				result.Add(new GetTableTypeColumnsRow
				{
					IsNullable = OptionalValue<Boolean>(reader, ordIsNullable),
					MaxLength = RequiredValue<Int16>(reader, ordMaxLength),
					Name = OptionalClass<String>(reader, ordName),
					TypeName = RequiredClass<String>(reader, ordTypeName),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
				});
			} while (await reader.ReadAsync().ConfigureAwait(false));
		}
		return result;
	}

	public static List<GetTableTypesRow> GetTableTypes(SqlConnection connection)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT T.name, T.type_table_object_id, S.name as [Schema_Name], T.schema_id, T.system_type_id, T.user_type_id FROM sys.table_types AS T INNER JOIN sys.schemas as S ON (T.schema_id = S.schema_id) ORDER BY S.name, T.name");

		var result = new List<GetTableTypesRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			int ordName = reader.GetOrdinal("name");
			int ordTypeTableObjectId = reader.GetOrdinal("type_table_object_id");
			int ordSchemaName = reader.GetOrdinal("Schema_Name");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");

			do
			{
				result.Add(new GetTableTypesRow
				{
					Name = RequiredClass<String>(reader, ordName),
					TypeTableObjectId = RequiredValue<Int32>(reader, ordTypeTableObjectId),
					SchemaName = RequiredClass<String>(reader, ordSchemaName),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
				});
			} while (reader.Read());
		}
		return result;
	}

	public static async Task<List<GetTableTypesRow>> GetTableTypesAsync(SqlConnection connection)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT T.name, T.type_table_object_id, S.name as [Schema_Name], T.schema_id, T.system_type_id, T.user_type_id FROM sys.table_types AS T INNER JOIN sys.schemas as S ON (T.schema_id = S.schema_id) ORDER BY S.name, T.name");

		var result = new List<GetTableTypesRow>();
		using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
		if (await reader.ReadAsync().ConfigureAwait(false))
		{
			int ordName = reader.GetOrdinal("name");
			int ordTypeTableObjectId = reader.GetOrdinal("type_table_object_id");
			int ordSchemaName = reader.GetOrdinal("Schema_Name");
			int ordSchemaId = reader.GetOrdinal("schema_id");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");

			do
			{
				result.Add(new GetTableTypesRow
				{
					Name = RequiredClass<String>(reader, ordName),
					TypeTableObjectId = RequiredValue<Int32>(reader, ordTypeTableObjectId),
					SchemaName = RequiredClass<String>(reader, ordSchemaName),
					SchemaId = RequiredValue<Int32>(reader, ordSchemaId),
					SystemTypeId = RequiredValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = RequiredValue<Int32>(reader, ordUserTypeId),
				});
			} while (await reader.ReadAsync().ConfigureAwait(false));
		}
		return result;
	}
}
