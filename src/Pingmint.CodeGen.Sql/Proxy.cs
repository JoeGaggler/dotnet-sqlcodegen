using System.Data;
using Microsoft.Data.SqlClient;

namespace Pingmint.CodeGen.Sql;

public static partial class Proxy
{
    private static SqlParameter CreateParameter(String parameterName, Object? value, SqlDbType sqlDbType, Int32 size = -1, ParameterDirection direction = ParameterDirection.Input)
    {
        var parameter = new SqlParameter(parameterName, value ?? DBNull.Value);
        parameter.Size = size;
        parameter.Direction = direction;
        parameter.SqlDbType = sqlDbType;
        return parameter;
    }

    private static SqlParameter CreateParameter(String parameterName, Object? value, SqlDbType sqlDbType, String typeName, Int32 size = -1, ParameterDirection direction = ParameterDirection.Input)
    {
        var parameter = new SqlParameter(parameterName, value ?? DBNull.Value);
        parameter.Size = size;
        parameter.Direction = direction;
        parameter.TypeName = typeName;
        parameter.SqlDbType = sqlDbType;
        return parameter;
    }

	public static async Task<List<TempDb.GetSysTypesRow>> GetSysTypesAsync(SqlConnection connection)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT name FROM sys.types";

		var result = new List<TempDb.GetSysTypesRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordName = reader.GetOrdinal("name");

				do
				{
					var row = new TempDb.GetSysTypesRow
					{
						Name = reader.IsDBNull(ordName) ? null! : reader.GetFieldValue<String>(ordName),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<TempDb.GetTableTypesRow>> GetTableTypesAsync(SqlConnection connection)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT T.name, T.type_table_object_id, S.name as [Schema_Name] FROM sys.table_types AS T INNER JOIN sys.schemas as S ON (T.schema_id = S.schema_id) ORDER BY S.name, T.name";

		var result = new List<TempDb.GetTableTypesRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordName = reader.GetOrdinal("name");
				int ordTypeTableObjectId = reader.GetOrdinal("type_table_object_id");
				int ordSchemaName = reader.GetOrdinal("Schema_Name");

				do
				{
					var row = new TempDb.GetTableTypesRow
					{
						Name = reader.IsDBNull(ordName) ? null! : reader.GetFieldValue<String>(ordName),
						TypeTableObjectId = reader.IsDBNull(ordTypeTableObjectId) ? default : reader.GetFieldValue<Int32>(ordTypeTableObjectId),
						SchemaName = reader.IsDBNull(ordSchemaName) ? null! : reader.GetFieldValue<String>(ordSchemaName),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<TempDb.GetObsoleteProceduresRow>> GetObsoleteProceduresAsync(SqlConnection connection)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT E.name, CAST(E.value as VARCHAR(MAX)) AS [value], E.major_id FROM sys.procedures AS P INNER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id) WHERE E.name = 'Obsolete'";

		var result = new List<TempDb.GetObsoleteProceduresRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordName = reader.GetOrdinal("name");
				int ordValue = reader.GetOrdinal("value");
				int ordMajorId = reader.GetOrdinal("major_id");

				do
				{
					var row = new TempDb.GetObsoleteProceduresRow
					{
						Name = reader.IsDBNull(ordName) ? null! : reader.GetFieldValue<String>(ordName),
						Value = reader.IsDBNull(ordValue) ? null : reader.GetFieldValue<String>(ordValue),
						MajorId = reader.IsDBNull(ordMajorId) ? default : reader.GetFieldValue<Int32>(ordMajorId),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<TempDb.GetProcGenHintProceduresRow>> GetProcGenHintProceduresAsync(SqlConnection connection, String hintName)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT CAST(E.value as VARCHAR(MAX)) AS [value], E.major_id FROM sys.procedures AS P INNER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id) WHERE E.name = @hint_name";

		cmd.Parameters.Add(CreateParameter("@hint_name", hintName, SqlDbType.VarChar));

		var result = new List<TempDb.GetProcGenHintProceduresRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordValue = reader.GetOrdinal("value");
				int ordMajorId = reader.GetOrdinal("major_id");

				do
				{
					var row = new TempDb.GetProcGenHintProceduresRow
					{
						Value = reader.IsDBNull(ordValue) ? null : reader.GetFieldValue<String>(ordValue),
						MajorId = reader.IsDBNull(ordMajorId) ? default : reader.GetFieldValue<Int32>(ordMajorId),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<TempDb.GetExtendedPropertiesForProcedureRow>> GetExtendedPropertiesForProcedureAsync(SqlConnection connection, String schema, String proc)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT CAST(e.value as varchar(max)) as [value] FROM sys.fn_listextendedproperty('ProcGen_ReinterpretColumns', 'SCHEMA', @schema, 'PROCEDURE', @proc, NULL, NULL) AS E";

		cmd.Parameters.Add(CreateParameter("@schema", schema, SqlDbType.VarChar));
		cmd.Parameters.Add(CreateParameter("@proc", proc, SqlDbType.VarChar));

		var result = new List<TempDb.GetExtendedPropertiesForProcedureRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordValue = reader.GetOrdinal("value");

				do
				{
					var row = new TempDb.GetExtendedPropertiesForProcedureRow
					{
						Value = reader.IsDBNull(ordValue) ? null : reader.GetFieldValue<String>(ordValue),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<TempDb.GetProceduresForSchemaRow>> GetProceduresForSchemaAsync(SqlConnection connection, String schema, String proc)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT P.name, P.object_id, S.name as [Schema_Name] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) WHERE S.name = @schema ORDER BY P.name";

		cmd.Parameters.Add(CreateParameter("@schema", schema, SqlDbType.VarChar));
		cmd.Parameters.Add(CreateParameter("@proc", proc, SqlDbType.VarChar));

		var result = new List<TempDb.GetProceduresForSchemaRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordName = reader.GetOrdinal("name");
				int ordObjectId = reader.GetOrdinal("object_id");
				int ordSchemaName = reader.GetOrdinal("Schema_Name");

				do
				{
					var row = new TempDb.GetProceduresForSchemaRow
					{
						Name = reader.IsDBNull(ordName) ? null! : reader.GetFieldValue<String>(ordName),
						ObjectId = reader.IsDBNull(ordObjectId) ? default : reader.GetFieldValue<Int32>(ordObjectId),
						SchemaName = reader.IsDBNull(ordSchemaName) ? null! : reader.GetFieldValue<String>(ordSchemaName),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<TempDb.DmDescribeFirstResultSetForObjectRow>> DmDescribeFirstResultSetForObjectAsync(SqlConnection connection, Int32 objectid)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT D.name, D.system_type_id, D.is_nullable, D.column_ordinal, T.name as [Type_Name] FROM sys.dm_exec_describe_first_result_set_for_object(@objectid, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id) WHERE T.name <> 'sysname' ORDER BY D.column_ordinal";

		cmd.Parameters.Add(CreateParameter("@objectid", objectid, SqlDbType.Int));

		var result = new List<TempDb.DmDescribeFirstResultSetForObjectRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordName = reader.GetOrdinal("name");
				int ordSystemTypeId = reader.GetOrdinal("system_type_id");
				int ordIsNullable = reader.GetOrdinal("is_nullable");
				int ordColumnOrdinal = reader.GetOrdinal("column_ordinal");
				int ordTypeName = reader.GetOrdinal("Type_Name");

				do
				{
					var row = new TempDb.DmDescribeFirstResultSetForObjectRow
					{
						Name = reader.IsDBNull(ordName) ? null : reader.GetFieldValue<String>(ordName),
						SystemTypeId = reader.IsDBNull(ordSystemTypeId) ? null : reader.GetFieldValue<Int32>(ordSystemTypeId),
						IsNullable = reader.IsDBNull(ordIsNullable) ? null : reader.GetFieldValue<Boolean>(ordIsNullable),
						ColumnOrdinal = reader.IsDBNull(ordColumnOrdinal) ? null : reader.GetFieldValue<Int32>(ordColumnOrdinal),
						TypeName = reader.IsDBNull(ordTypeName) ? null! : reader.GetFieldValue<String>(ordTypeName),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<TempDb.DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlConnection connection, String text)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT D.name, D.system_type_id, D.is_nullable, D.column_ordinal, T.name as [type_name] FROM sys.dm_exec_describe_first_result_set(@text, NULL, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = D.system_type_id) ORDER BY D.column_ordinal";

		cmd.Parameters.Add(CreateParameter("@text", text, SqlDbType.VarChar));

		var result = new List<TempDb.DmDescribeFirstResultSetRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordName = reader.GetOrdinal("name");
				int ordSystemTypeId = reader.GetOrdinal("system_type_id");
				int ordIsNullable = reader.GetOrdinal("is_nullable");
				int ordColumnOrdinal = reader.GetOrdinal("column_ordinal");
				int ordTypeName = reader.GetOrdinal("type_name");

				do
				{
					var row = new TempDb.DmDescribeFirstResultSetRow
					{
						Name = reader.IsDBNull(ordName) ? null : reader.GetFieldValue<String>(ordName),
						SystemTypeId = reader.IsDBNull(ordSystemTypeId) ? null : reader.GetFieldValue<Int32>(ordSystemTypeId),
						IsNullable = reader.IsDBNull(ordIsNullable) ? null : reader.GetFieldValue<Boolean>(ordIsNullable),
						ColumnOrdinal = reader.IsDBNull(ordColumnOrdinal) ? null : reader.GetFieldValue<Int32>(ordColumnOrdinal),
						TypeName = reader.IsDBNull(ordTypeName) ? null! : reader.GetFieldValue<String>(ordTypeName),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<TempDb.GetSysTypeRow>> GetSysTypeAsync(SqlConnection connection, Int32 id)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT system_type_id, is_table_type, name FROM sys.types where system_type_id = @id";

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<TempDb.GetSysTypeRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordSystemTypeId = reader.GetOrdinal("system_type_id");
				int ordIsTableType = reader.GetOrdinal("is_table_type");
				int ordName = reader.GetOrdinal("name");

				do
				{
					var row = new TempDb.GetSysTypeRow
					{
						SystemTypeId = reader.IsDBNull(ordSystemTypeId) ? default : reader.GetFieldValue<Byte>(ordSystemTypeId),
						IsTableType = reader.IsDBNull(ordIsTableType) ? default : reader.GetFieldValue<Boolean>(ordIsTableType),
						Name = reader.IsDBNull(ordName) ? null! : reader.GetFieldValue<String>(ordName),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<TempDb.GetParametersForObjectRow>> GetParametersForObjectAsync(SqlConnection connection, Int32 id)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT P.parameter_id, P.system_type_id, P.name, P.is_output, P.max_length, T.is_table_type, T.name as [Type_Name] FROM sys.parameters AS P JOIN sys.types AS T ON (P.system_type_id = T.system_type_id AND P.user_type_id = T.user_type_id) WHERE P.object_id = @id";

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<TempDb.GetParametersForObjectRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordParameterId = reader.GetOrdinal("parameter_id");
				int ordSystemTypeId = reader.GetOrdinal("system_type_id");
				int ordName = reader.GetOrdinal("name");
				int ordIsOutput = reader.GetOrdinal("is_output");
				int ordMaxLength = reader.GetOrdinal("max_length");
				int ordIsTableType = reader.GetOrdinal("is_table_type");
				int ordTypeName = reader.GetOrdinal("Type_Name");

				do
				{
					var row = new TempDb.GetParametersForObjectRow
					{
						ParameterId = reader.IsDBNull(ordParameterId) ? default : reader.GetFieldValue<Int32>(ordParameterId),
						SystemTypeId = reader.IsDBNull(ordSystemTypeId) ? default : reader.GetFieldValue<Byte>(ordSystemTypeId),
						Name = reader.IsDBNull(ordName) ? null : reader.GetFieldValue<String>(ordName),
						IsOutput = reader.IsDBNull(ordIsOutput) ? default : reader.GetFieldValue<Boolean>(ordIsOutput),
						MaxLength = reader.IsDBNull(ordMaxLength) ? default : reader.GetFieldValue<Int16>(ordMaxLength),
						IsTableType = reader.IsDBNull(ordIsTableType) ? default : reader.GetFieldValue<Boolean>(ordIsTableType),
						TypeName = reader.IsDBNull(ordTypeName) ? null! : reader.GetFieldValue<String>(ordTypeName),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<TempDb.GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(SqlConnection connection, Int32 id)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT C.is_nullable, C.max_length, C.name, t.name as [Type_Name] from sys.columns as C join sys.types T ON (C.system_type_id = T.system_type_id) where C.object_id = @id and t.name <> 'sysname' order by c.column_id";

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<TempDb.GetTableTypeColumnsRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordIsNullable = reader.GetOrdinal("is_nullable");
				int ordMaxLength = reader.GetOrdinal("max_length");
				int ordName = reader.GetOrdinal("name");
				int ordTypeName = reader.GetOrdinal("Type_Name");

				do
				{
					var row = new TempDb.GetTableTypeColumnsRow
					{
						IsNullable = reader.IsDBNull(ordIsNullable) ? null : reader.GetFieldValue<Boolean>(ordIsNullable),
						MaxLength = reader.IsDBNull(ordMaxLength) ? default : reader.GetFieldValue<Int16>(ordMaxLength),
						Name = reader.IsDBNull(ordName) ? null : reader.GetFieldValue<String>(ordName),
						TypeName = reader.IsDBNull(ordTypeName) ? null! : reader.GetFieldValue<String>(ordTypeName),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public partial class TempDb
	{
		public partial class GetSysTypesRow
		{
			public String Name { get; set; }
		}
	}
	public partial class TempDb
	{
		public partial class GetTableTypesRow
		{
			public String Name { get; set; }
			public Int32 TypeTableObjectId { get; set; }
			public String SchemaName { get; set; }
		}
	}
	public partial class TempDb
	{
		public partial class GetObsoleteProceduresRow
		{
			public String Name { get; set; }
			public String? Value { get; set; }
			public Int32 MajorId { get; set; }
		}
	}
	public partial class TempDb
	{
		public partial class GetProcGenHintProceduresRow
		{
			public String? Value { get; set; }
			public Int32 MajorId { get; set; }
		}
	}
	public partial class TempDb
	{
		public partial class GetExtendedPropertiesForProcedureRow
		{
			public String? Value { get; set; }
		}
	}
	public partial class TempDb
	{
		public partial class GetProceduresForSchemaRow
		{
			public String Name { get; set; }
			public Int32 ObjectId { get; set; }
			public String SchemaName { get; set; }
		}
	}
	public partial class TempDb
	{
		public partial class DmDescribeFirstResultSetForObjectRow
		{
			public String? Name { get; set; }
			public Int32? SystemTypeId { get; set; }
			public Boolean? IsNullable { get; set; }
			public Int32? ColumnOrdinal { get; set; }
			public String TypeName { get; set; }
		}
	}
	public partial class TempDb
	{
		public partial class DmDescribeFirstResultSetRow
		{
			public String? Name { get; set; }
			public Int32? SystemTypeId { get; set; }
			public Boolean? IsNullable { get; set; }
			public Int32? ColumnOrdinal { get; set; }
			public String TypeName { get; set; }
		}
	}
	public partial class TempDb
	{
		public partial class GetSysTypeRow
		{
			public Byte SystemTypeId { get; set; }
			public Boolean IsTableType { get; set; }
			public String Name { get; set; }
		}
	}
	public partial class TempDb
	{
		public partial class GetParametersForObjectRow
		{
			public Int32 ParameterId { get; set; }
			public Byte SystemTypeId { get; set; }
			public String? Name { get; set; }
			public Boolean IsOutput { get; set; }
			public Int16 MaxLength { get; set; }
			public Boolean IsTableType { get; set; }
			public String TypeName { get; set; }
		}
	}
	public partial class TempDb
	{
		public partial class GetTableTypeColumnsRow
		{
			public Boolean? IsNullable { get; set; }
			public Int16 MaxLength { get; set; }
			public String? Name { get; set; }
			public String TypeName { get; set; }
		}
	}
}
