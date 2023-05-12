using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Pingmint.CodeGen.Sql2;

public partial record class DmDescribeFirstResultSetForObjectRow
{
	  public String? Name { get; set; }
	  public Int32 SchemaId { get; set; }
	  public Byte SystemTypeId { get; set; }
	  public Int32 UserTypeId { get; set; }
	  public Boolean? IsNullable { get; set; }
	  public Int32? ColumnOrdinal { get; set; }
	  public String SqlTypeName { get; set; }
}
public partial record class DmDescribeFirstResultSetRow
{
	  public String? Name { get; set; }
	  public Int32 SchemaId { get; set; }
	  public Byte SystemTypeId { get; set; }
	  public Int32 UserTypeId { get; set; }
	  public Boolean? IsNullable { get; set; }
	  public Int32? ColumnOrdinal { get; set; }
	  public String SqlTypeName { get; set; }
}
public partial record class GetProcedureForSchemaRow
{
	  public String Name { get; set; }
	  public Int32 ObjectId { get; set; }
	  public String SchemaName { get; set; }
	  public String? ObsoleteMessage { get; set; }
}
public partial record class GetProceduresForSchemaRow
{
	  public String Name { get; set; }
	  public Int32 ObjectId { get; set; }
	  public String SchemaName { get; set; }
	  public String? ObsoleteMessage { get; set; }
}
public partial record class GetParametersForObjectRow
{
	  public Int32 ParameterId { get; set; }
	  public Int32 SchemaId { get; set; }
	  public Byte SystemTypeId { get; set; }
	  public Int32 UserTypeId { get; set; }
	  public String? Name { get; set; }
	  public Boolean IsOutput { get; set; }
	  public Int16 MaxLength { get; set; }
	  public Boolean IsTableType { get; set; }
	  public String TypeName { get; set; }
}
public partial record class GetTableTypesRow
{
	  public String Name { get; set; }
	  public Int32 TypeTableObjectId { get; set; }
	  public String SchemaName { get; set; }
	  public Int32 SchemaId { get; set; }
	  public Byte SystemTypeId { get; set; }
	  public Int32 UserTypeId { get; set; }
}
public partial record class GetSysTypesRow
{
	  public String Name { get; set; }
	  public Int32 SchemaId { get; set; }
	  public Byte SystemTypeId { get; set; }
	  public Int32 UserTypeId { get; set; }
	  public Boolean IsUserDefined { get; set; }
}
public partial record class GetSysTypeRow
{
	  public Byte SystemTypeId { get; set; }
	  public Boolean IsTableType { get; set; }
	  public String Name { get; set; }
}
public partial record class GetTableTypeColumnsRow
{
	  public Boolean? IsNullable { get; set; }
	  public Int16 MaxLength { get; set; }
	  public String? Name { get; set; }
	  public String TypeName { get; set; }
	  public Int32 SchemaId { get; set; }
	  public Byte SystemTypeId { get; set; }
	  public Int32 UserTypeId { get; set; }
}
public partial record class GetSchemasRow
{
	  public String Name { get; set; }
	  public Int32 SchemaId { get; set; }
}
public partial class Proxy2
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

    private static T? GetField<T>(SqlDataReader reader, int ordinal) where T : class => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
    private static T? GetFieldValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
    private static T GetNonNullField<T>(SqlDataReader reader, int ordinal) where T : class => reader.IsDBNull(ordinal) ? throw new NullReferenceException() : reader.GetFieldValue<T>(ordinal);
    private static T GetNonNullFieldValue<T>(SqlDataReader reader, int ordinal) where T : struct => reader.IsDBNull(ordinal) ? throw new NullReferenceException() : reader.GetFieldValue<T>(ordinal);

    private static SqlCommand CreateStatement(SqlConnection connection, String text) => new() { Connection = connection, CommandType = CommandType.Text, CommandText = text, };
    private static SqlCommand CreateStoredProcedure(SqlConnection connection, String text) => new() { Connection = connection, CommandType = CommandType.StoredProcedure, CommandText = text, };
	public static List<DmDescribeFirstResultSetForObjectRow> DmDescribeFirstResultSetForObject(SqlConnection connection, Int32 objectid)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set_for_object(@objectid, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");

		cmd.Parameters.Add(CreateParameter("@objectid", objectid, SqlDbType.Int));

		var result = new List<DmDescribeFirstResultSetForObjectRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			var ordName = reader.GetOrdinal("name");
			var ordSchemaId = reader.GetOrdinal("schema_id");
			var ordSystemTypeId = reader.GetOrdinal("system_type_id");
			var ordUserTypeId = reader.GetOrdinal("user_type_id");
			var ordIsNullable = reader.GetOrdinal("is_nullable");
			var ordColumnOrdinal = reader.GetOrdinal("column_ordinal");
			var ordSqlTypeName = reader.GetOrdinal("sql_type_name");
			do
			{
				result.Add(new DmDescribeFirstResultSetForObjectRow
				{
					Name = GetField<String>(reader, ordName),
					SchemaId = GetNonNullFieldValue<Int32>(reader, ordSchemaId),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
					IsNullable = GetFieldValue<Boolean>(reader, ordIsNullable),
					ColumnOrdinal = GetFieldValue<Int32>(reader, ordColumnOrdinal),
					SqlTypeName = GetNonNullField<String>(reader, ordSqlTypeName),
				});
			} while (reader.Read());
		}
		return result;
	}
	public static List<DmDescribeFirstResultSetRow> DmDescribeFirstResultSet(SqlConnection connection, String text, String parameters)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT D.name, T.schema_id, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set(@text, @parameters, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");

		cmd.Parameters.Add(CreateParameter("@text", text, SqlDbType.NVarChar));
		cmd.Parameters.Add(CreateParameter("@parameters", parameters, SqlDbType.NVarChar));

		var result = new List<DmDescribeFirstResultSetRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			var ordName = reader.GetOrdinal("name");
			var ordSchemaId = reader.GetOrdinal("schema_id");
			var ordSystemTypeId = reader.GetOrdinal("system_type_id");
			var ordUserTypeId = reader.GetOrdinal("user_type_id");
			var ordIsNullable = reader.GetOrdinal("is_nullable");
			var ordColumnOrdinal = reader.GetOrdinal("column_ordinal");
			var ordSqlTypeName = reader.GetOrdinal("sql_type_name");
			do
			{
				result.Add(new DmDescribeFirstResultSetRow
				{
					Name = GetField<String>(reader, ordName),
					SchemaId = GetNonNullFieldValue<Int32>(reader, ordSchemaId),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
					IsNullable = GetFieldValue<Boolean>(reader, ordIsNullable),
					ColumnOrdinal = GetFieldValue<Int32>(reader, ordColumnOrdinal),
					SqlTypeName = GetNonNullField<String>(reader, ordSqlTypeName),
				});
			} while (reader.Read());
		}
		return result;
	}
	public static List<GetProcedureForSchemaRow> GetProcedureForSchema(SqlConnection connection, String schema, String proc)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema AND P.name = @proc ORDER BY P.name");

		cmd.Parameters.Add(CreateParameter("@schema", schema, SqlDbType.VarChar));
		cmd.Parameters.Add(CreateParameter("@proc", proc, SqlDbType.VarChar));

		var result = new List<GetProcedureForSchemaRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			var ordName = reader.GetOrdinal("name");
			var ordObjectId = reader.GetOrdinal("object_id");
			var ordSchemaName = reader.GetOrdinal("Schema_Name");
			var ordObsoleteMessage = reader.GetOrdinal("Obsolete_Message");
			do
			{
				result.Add(new GetProcedureForSchemaRow
				{
					Name = GetNonNullField<String>(reader, ordName),
					ObjectId = GetNonNullFieldValue<Int32>(reader, ordObjectId),
					SchemaName = GetNonNullField<String>(reader, ordSchemaName),
					ObsoleteMessage = GetField<String>(reader, ordObsoleteMessage),
				});
			} while (reader.Read());
		}
		return result;
	}
	public static List<GetProceduresForSchemaRow> GetProceduresForSchema(SqlConnection connection, String schema)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema ORDER BY P.name");

		cmd.Parameters.Add(CreateParameter("@schema", schema, SqlDbType.VarChar));

		var result = new List<GetProceduresForSchemaRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			var ordName = reader.GetOrdinal("name");
			var ordObjectId = reader.GetOrdinal("object_id");
			var ordSchemaName = reader.GetOrdinal("Schema_Name");
			var ordObsoleteMessage = reader.GetOrdinal("Obsolete_Message");
			do
			{
				result.Add(new GetProceduresForSchemaRow
				{
					Name = GetNonNullField<String>(reader, ordName),
					ObjectId = GetNonNullFieldValue<Int32>(reader, ordObjectId),
					SchemaName = GetNonNullField<String>(reader, ordSchemaName),
					ObsoleteMessage = GetField<String>(reader, ordObsoleteMessage),
				});
			} while (reader.Read());
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
			var ordParameterId = reader.GetOrdinal("parameter_id");
			var ordSchemaId = reader.GetOrdinal("schema_id");
			var ordSystemTypeId = reader.GetOrdinal("system_type_id");
			var ordUserTypeId = reader.GetOrdinal("user_type_id");
			var ordName = reader.GetOrdinal("name");
			var ordIsOutput = reader.GetOrdinal("is_output");
			var ordMaxLength = reader.GetOrdinal("max_length");
			var ordIsTableType = reader.GetOrdinal("is_table_type");
			var ordTypeName = reader.GetOrdinal("Type_Name");
			do
			{
				result.Add(new GetParametersForObjectRow
				{
					ParameterId = GetNonNullFieldValue<Int32>(reader, ordParameterId),
					SchemaId = GetNonNullFieldValue<Int32>(reader, ordSchemaId),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
					Name = GetField<String>(reader, ordName),
					IsOutput = GetNonNullFieldValue<Boolean>(reader, ordIsOutput),
					MaxLength = GetNonNullFieldValue<Int16>(reader, ordMaxLength),
					IsTableType = GetNonNullFieldValue<Boolean>(reader, ordIsTableType),
					TypeName = GetNonNullField<String>(reader, ordTypeName),
				});
			} while (reader.Read());
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
			var ordName = reader.GetOrdinal("name");
			var ordTypeTableObjectId = reader.GetOrdinal("type_table_object_id");
			var ordSchemaName = reader.GetOrdinal("Schema_Name");
			var ordSchemaId = reader.GetOrdinal("schema_id");
			var ordSystemTypeId = reader.GetOrdinal("system_type_id");
			var ordUserTypeId = reader.GetOrdinal("user_type_id");
			do
			{
				result.Add(new GetTableTypesRow
				{
					Name = GetNonNullField<String>(reader, ordName),
					TypeTableObjectId = GetNonNullFieldValue<Int32>(reader, ordTypeTableObjectId),
					SchemaName = GetNonNullField<String>(reader, ordSchemaName),
					SchemaId = GetNonNullFieldValue<Int32>(reader, ordSchemaId),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
				});
			} while (reader.Read());
		}
		return result;
	}
	public static List<GetSysTypesRow> GetSysTypes(SqlConnection connection)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT name, schema_id, system_type_id, user_type_id, is_user_defined FROM sys.types");


		var result = new List<GetSysTypesRow>();
		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			var ordName = reader.GetOrdinal("name");
			var ordSchemaId = reader.GetOrdinal("schema_id");
			var ordSystemTypeId = reader.GetOrdinal("system_type_id");
			var ordUserTypeId = reader.GetOrdinal("user_type_id");
			var ordIsUserDefined = reader.GetOrdinal("is_user_defined");
			do
			{
				result.Add(new GetSysTypesRow
				{
					Name = GetNonNullField<String>(reader, ordName),
					SchemaId = GetNonNullFieldValue<Int32>(reader, ordSchemaId),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
					IsUserDefined = GetNonNullFieldValue<Boolean>(reader, ordIsUserDefined),
				});
			} while (reader.Read());
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
			var ordSystemTypeId = reader.GetOrdinal("system_type_id");
			var ordIsTableType = reader.GetOrdinal("is_table_type");
			var ordName = reader.GetOrdinal("name");
			do
			{
				result.Add(new GetSysTypeRow
				{
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					IsTableType = GetNonNullFieldValue<Boolean>(reader, ordIsTableType),
					Name = GetNonNullField<String>(reader, ordName),
				});
			} while (reader.Read());
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
			var ordIsNullable = reader.GetOrdinal("is_nullable");
			var ordMaxLength = reader.GetOrdinal("max_length");
			var ordName = reader.GetOrdinal("name");
			var ordTypeName = reader.GetOrdinal("Type_Name");
			var ordSchemaId = reader.GetOrdinal("schema_id");
			var ordSystemTypeId = reader.GetOrdinal("system_type_id");
			var ordUserTypeId = reader.GetOrdinal("user_type_id");
			do
			{
				result.Add(new GetTableTypeColumnsRow
				{
					IsNullable = GetFieldValue<Boolean>(reader, ordIsNullable),
					MaxLength = GetNonNullFieldValue<Int16>(reader, ordMaxLength),
					Name = GetField<String>(reader, ordName),
					TypeName = GetNonNullField<String>(reader, ordTypeName),
					SchemaId = GetNonNullFieldValue<Int32>(reader, ordSchemaId),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
				});
			} while (reader.Read());
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
			var ordName = reader.GetOrdinal("name");
			var ordSchemaId = reader.GetOrdinal("schema_id");
			do
			{
				result.Add(new GetSchemasRow
				{
					Name = GetNonNullField<String>(reader, ordName),
					SchemaId = GetNonNullFieldValue<Int32>(reader, ordSchemaId),
				});
			} while (reader.Read());
		}
		return result;
	}
}
