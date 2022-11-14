using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Pingmint.CodeGen.Sql;

public partial interface IProxy
{
	Task<List<EchoScopes2Row>> EchoScopes2Async(List<ScopesRow> scopes1, List<ScopesRow> scopes2);
	Task<Int32> InsertFooAsync(Int32 val);
	Task<List<DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(String text);
	Task<List<DmDescribeFirstResultSetForObjectRow>> DmDescribeFirstResultSetForObjectAsync(Int32 objectid);
	Task<List<GetParametersForObjectRow>> GetParametersForObjectAsync(Int32 id);
	Task<List<GetProcedureForSchemaRow>> GetProcedureForSchemaAsync(String schema, String proc);
	Task<List<GetProceduresForSchemaRow>> GetProceduresForSchemaAsync(String schema);
	Task<List<GetSysTypeRow>> GetSysTypeAsync(Int32 id);
	Task<List<GetSysTypesRow>> GetSysTypesAsync();
	Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(Int32 id);
	Task<List<GetTableTypesRow>> GetTableTypesAsync();
}

public partial class EchoScopes2Row
{
	public String Scope { get; set; }
}
public partial class ScopesRow // pingmint.Scopes
{
	public String Scope { get; set; }
}
public sealed partial class ScopesRowDataTable : DataTable
{
	public ScopesRowDataTable() : this(new List<ScopesRow>()) { }
	public ScopesRowDataTable(List<ScopesRow> rows) : base()
	{
		ArgumentNullException.ThrowIfNull(rows);

		base.Columns.Add(new DataColumn() { ColumnName = "Scope", DataType = typeof(String), AllowDBNull = false, MaxLength = 50 });
		foreach (var row in rows)
		{
			var scope = String.IsNullOrEmpty(row.Scope) || row.Scope.Length <= 50 ? row.Scope : row.Scope.Remove(50);
			base.Rows.Add(scope);
		}
	}
}
public partial class DmDescribeFirstResultSetForObjectRow
{
	public String? Name { get; set; }
	public Byte SystemTypeId { get; set; }
	public Int32 UserTypeId { get; set; }
	public Boolean? IsNullable { get; set; }
	public Int32? ColumnOrdinal { get; set; }
	public String SqlTypeName { get; set; }
}
public partial class DmDescribeFirstResultSetRow
{
	public String? Name { get; set; }
	public Byte SystemTypeId { get; set; }
	public Int32 UserTypeId { get; set; }
	public Boolean? IsNullable { get; set; }
	public Int32? ColumnOrdinal { get; set; }
	public String SqlTypeName { get; set; }
}
public partial class GetParametersForObjectRow
{
	public Int32 ParameterId { get; set; }
	public Byte SystemTypeId { get; set; }
	public Int32 UserTypeId { get; set; }
	public String? Name { get; set; }
	public Boolean IsOutput { get; set; }
	public Int16 MaxLength { get; set; }
	public Boolean IsTableType { get; set; }
	public String TypeName { get; set; }
}
public partial class GetProcedureForSchemaRow
{
	public String Name { get; set; }
	public Int32 ObjectId { get; set; }
	public String SchemaName { get; set; }
	public String? ObsoleteMessage { get; set; }
}
public partial class GetProceduresForSchemaRow
{
	public String Name { get; set; }
	public Int32 ObjectId { get; set; }
	public String SchemaName { get; set; }
	public String? ObsoleteMessage { get; set; }
}
public partial class GetSysTypeRow
{
	public Byte SystemTypeId { get; set; }
	public Boolean IsTableType { get; set; }
	public String Name { get; set; }
}
public partial class GetSysTypesRow
{
	public String Name { get; set; }
	public Byte SystemTypeId { get; set; }
	public Int32 UserTypeId { get; set; }
}
public partial class GetTableTypeColumnsRow
{
	public Boolean? IsNullable { get; set; }
	public Int16 MaxLength { get; set; }
	public String? Name { get; set; }
	public String TypeName { get; set; }
	public Byte SystemTypeId { get; set; }
	public Int32 UserTypeId { get; set; }
}
public partial class GetTableTypesRow
{
	public String Name { get; set; }
	public Int32 TypeTableObjectId { get; set; }
	public String SchemaName { get; set; }
	public Byte SystemTypeId { get; set; }
	public Int32 UserTypeId { get; set; }
}

public partial class Proxy : IProxy
{
	private readonly Func<Task<SqlConnection>> connectionFunc;

	public Proxy(Func<Task<SqlConnection>> connectionFunc)
	{
		this.connectionFunc = connectionFunc;
	}

	public async Task<List<EchoScopes2Row>> EchoScopes2Async(List<ScopesRow> scopes1, List<ScopesRow> scopes2) => await EchoScopes2Async(await connectionFunc(), scopes1, scopes2);
	public async Task<Int32> InsertFooAsync(Int32 val) => await InsertFooAsync(await connectionFunc(), val);
	public async Task<List<DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(String text) => await DmDescribeFirstResultSetAsync(await connectionFunc(), text);
	public async Task<List<DmDescribeFirstResultSetForObjectRow>> DmDescribeFirstResultSetForObjectAsync(Int32 objectid) => await DmDescribeFirstResultSetForObjectAsync(await connectionFunc(), objectid);
	public async Task<List<GetParametersForObjectRow>> GetParametersForObjectAsync(Int32 id) => await GetParametersForObjectAsync(await connectionFunc(), id);
	public async Task<List<GetProcedureForSchemaRow>> GetProcedureForSchemaAsync(String schema, String proc) => await GetProcedureForSchemaAsync(await connectionFunc(), schema, proc);
	public async Task<List<GetProceduresForSchemaRow>> GetProceduresForSchemaAsync(String schema) => await GetProceduresForSchemaAsync(await connectionFunc(), schema);
	public async Task<List<GetSysTypeRow>> GetSysTypeAsync(Int32 id) => await GetSysTypeAsync(await connectionFunc(), id);
	public async Task<List<GetSysTypesRow>> GetSysTypesAsync() => await GetSysTypesAsync(await connectionFunc());
	public async Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(Int32 id) => await GetTableTypeColumnsAsync(await connectionFunc(), id);
	public async Task<List<GetTableTypesRow>> GetTableTypesAsync() => await GetTableTypesAsync(await connectionFunc());

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

	public static Task<List<EchoScopes2Row>> EchoScopes2Async(SqlConnection connection, List<ScopesRow> scopes1, List<ScopesRow> scopes2) => EchoScopes2Async(connection, scopes1, scopes2, CancellationToken.None);
	public static async Task<List<EchoScopes2Row>> EchoScopes2Async(SqlConnection connection, List<ScopesRow> scopes1, List<ScopesRow> scopes2, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStoredProcedure(connection, "tempdb.pingmint.echo_scopes2");

		cmd.Parameters.Add(CreateParameter("@scopes1", new ScopesRowDataTable(scopes1), SqlDbType.Structured, "pingmint.Scopes"));
		cmd.Parameters.Add(CreateParameter("@scopes2", new ScopesRowDataTable(scopes2), SqlDbType.Structured, "pingmint.Scopes"));

		var result = new List<EchoScopes2Row>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			int ordScope = reader.GetOrdinal("Scope");

			do
			{
				result.Add(new EchoScopes2Row
				{
					Scope = GetNonNullField<String>(reader, ordScope),
				});
			} while (await reader.ReadAsync(cancellationToken));
		}
		return result;
	}

	public static Task<Int32> InsertFooAsync(SqlConnection connection, Int32 val) => InsertFooAsync(connection, val, CancellationToken.None);
	public static async Task<Int32> InsertFooAsync(SqlConnection connection, Int32 val, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStoredProcedure(connection, "tempdb.pingmint.insert_foo");

		cmd.Parameters.Add(CreateParameter("@val", val, SqlDbType.Int, 4));

		return await cmd.ExecuteNonQueryAsync(cancellationToken);
	}

	public static Task<List<DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlConnection connection, String text) => DmDescribeFirstResultSetAsync(connection, text, CancellationToken.None);
	public static async Task<List<DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlConnection connection, String text, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT D.name, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set(@text, NULL, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");

		cmd.Parameters.Add(CreateParameter("@text", text, SqlDbType.VarChar));

		var result = new List<DmDescribeFirstResultSetRow>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			int ordName = reader.GetOrdinal("name");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordColumnOrdinal = reader.GetOrdinal("column_ordinal");
			int ordSqlTypeName = reader.GetOrdinal("sql_type_name");

			do
			{
				result.Add(new DmDescribeFirstResultSetRow
				{
					Name = GetField<String>(reader, ordName),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
					IsNullable = GetFieldValue<Boolean>(reader, ordIsNullable),
					ColumnOrdinal = GetFieldValue<Int32>(reader, ordColumnOrdinal),
					SqlTypeName = GetNonNullField<String>(reader, ordSqlTypeName),
				});
			} while (await reader.ReadAsync(cancellationToken));
		}
		return result;
	}

	public static Task<List<DmDescribeFirstResultSetForObjectRow>> DmDescribeFirstResultSetForObjectAsync(SqlConnection connection, Int32 objectid) => DmDescribeFirstResultSetForObjectAsync(connection, objectid, CancellationToken.None);
	public static async Task<List<DmDescribeFirstResultSetForObjectRow>> DmDescribeFirstResultSetForObjectAsync(SqlConnection connection, Int32 objectid, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT D.name, T.system_type_id, T.user_type_id, D.is_nullable, D.column_ordinal, T.name as [sql_type_name] FROM sys.dm_exec_describe_first_result_set_for_object(@objectid, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = ISNULL(D.user_type_id, D.system_type_id)) ORDER BY D.column_ordinal");

		cmd.Parameters.Add(CreateParameter("@objectid", objectid, SqlDbType.Int));

		var result = new List<DmDescribeFirstResultSetForObjectRow>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			int ordName = reader.GetOrdinal("name");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordColumnOrdinal = reader.GetOrdinal("column_ordinal");
			int ordSqlTypeName = reader.GetOrdinal("sql_type_name");

			do
			{
				result.Add(new DmDescribeFirstResultSetForObjectRow
				{
					Name = GetField<String>(reader, ordName),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
					IsNullable = GetFieldValue<Boolean>(reader, ordIsNullable),
					ColumnOrdinal = GetFieldValue<Int32>(reader, ordColumnOrdinal),
					SqlTypeName = GetNonNullField<String>(reader, ordSqlTypeName),
				});
			} while (await reader.ReadAsync(cancellationToken));
		}
		return result;
	}

	public static Task<List<GetParametersForObjectRow>> GetParametersForObjectAsync(SqlConnection connection, Int32 id) => GetParametersForObjectAsync(connection, id, CancellationToken.None);
	public static async Task<List<GetParametersForObjectRow>> GetParametersForObjectAsync(SqlConnection connection, Int32 id, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.parameter_id, P.system_type_id, P.user_type_id, P.name, P.is_output, P.max_length, T.is_table_type, T.name as [Type_Name] FROM sys.parameters AS P JOIN sys.types AS T ON (P.system_type_id = T.system_type_id AND P.user_type_id = T.user_type_id) WHERE P.object_id = @id");

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<GetParametersForObjectRow>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			int ordParameterId = reader.GetOrdinal("parameter_id");
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
					ParameterId = GetNonNullFieldValue<Int32>(reader, ordParameterId),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
					Name = GetField<String>(reader, ordName),
					IsOutput = GetNonNullFieldValue<Boolean>(reader, ordIsOutput),
					MaxLength = GetNonNullFieldValue<Int16>(reader, ordMaxLength),
					IsTableType = GetNonNullFieldValue<Boolean>(reader, ordIsTableType),
					TypeName = GetNonNullField<String>(reader, ordTypeName),
				});
			} while (await reader.ReadAsync(cancellationToken));
		}
		return result;
	}

	public static Task<List<GetProcedureForSchemaRow>> GetProcedureForSchemaAsync(SqlConnection connection, String schema, String proc) => GetProcedureForSchemaAsync(connection, schema, proc, CancellationToken.None);
	public static async Task<List<GetProcedureForSchemaRow>> GetProcedureForSchemaAsync(SqlConnection connection, String schema, String proc, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema AND P.name = @proc ORDER BY P.name");

		cmd.Parameters.Add(CreateParameter("@schema", schema, SqlDbType.VarChar));
		cmd.Parameters.Add(CreateParameter("@proc", proc, SqlDbType.VarChar));

		var result = new List<GetProcedureForSchemaRow>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			int ordName = reader.GetOrdinal("name");
			int ordObjectId = reader.GetOrdinal("object_id");
			int ordSchemaName = reader.GetOrdinal("Schema_Name");
			int ordObsoleteMessage = reader.GetOrdinal("Obsolete_Message");

			do
			{
				result.Add(new GetProcedureForSchemaRow
				{
					Name = GetNonNullField<String>(reader, ordName),
					ObjectId = GetNonNullFieldValue<Int32>(reader, ordObjectId),
					SchemaName = GetNonNullField<String>(reader, ordSchemaName),
					ObsoleteMessage = GetField<String>(reader, ordObsoleteMessage),
				});
			} while (await reader.ReadAsync(cancellationToken));
		}
		return result;
	}

	public static Task<List<GetProceduresForSchemaRow>> GetProceduresForSchemaAsync(SqlConnection connection, String schema) => GetProceduresForSchemaAsync(connection, schema, CancellationToken.None);
	public static async Task<List<GetProceduresForSchemaRow>> GetProceduresForSchemaAsync(SqlConnection connection, String schema, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT P.name, P.object_id, S.name as [Schema_Name], CAST(E.value as VARCHAR(MAX)) AS [Obsolete_Message] FROM sys.procedures AS P INNER JOIN sys.schemas as S ON (P.schema_id = S.schema_id) LEFT OUTER JOIN sys.extended_properties AS E ON (P.object_id = E.major_id AND E.Name = 'Obsolete') WHERE S.name = @schema ORDER BY P.name");

		cmd.Parameters.Add(CreateParameter("@schema", schema, SqlDbType.VarChar));

		var result = new List<GetProceduresForSchemaRow>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			int ordName = reader.GetOrdinal("name");
			int ordObjectId = reader.GetOrdinal("object_id");
			int ordSchemaName = reader.GetOrdinal("Schema_Name");
			int ordObsoleteMessage = reader.GetOrdinal("Obsolete_Message");

			do
			{
				result.Add(new GetProceduresForSchemaRow
				{
					Name = GetNonNullField<String>(reader, ordName),
					ObjectId = GetNonNullFieldValue<Int32>(reader, ordObjectId),
					SchemaName = GetNonNullField<String>(reader, ordSchemaName),
					ObsoleteMessage = GetField<String>(reader, ordObsoleteMessage),
				});
			} while (await reader.ReadAsync(cancellationToken));
		}
		return result;
	}

	public static Task<List<GetSysTypeRow>> GetSysTypeAsync(SqlConnection connection, Int32 id) => GetSysTypeAsync(connection, id, CancellationToken.None);
	public static async Task<List<GetSysTypeRow>> GetSysTypeAsync(SqlConnection connection, Int32 id, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT system_type_id, is_table_type, name FROM sys.types where system_type_id = @id");

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<GetSysTypeRow>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordIsTableType = reader.GetOrdinal("is_table_type");
			int ordName = reader.GetOrdinal("name");

			do
			{
				result.Add(new GetSysTypeRow
				{
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					IsTableType = GetNonNullFieldValue<Boolean>(reader, ordIsTableType),
					Name = GetNonNullField<String>(reader, ordName),
				});
			} while (await reader.ReadAsync(cancellationToken));
		}
		return result;
	}

	public static Task<List<GetSysTypesRow>> GetSysTypesAsync(SqlConnection connection) => GetSysTypesAsync(connection, CancellationToken.None);
	public static async Task<List<GetSysTypesRow>> GetSysTypesAsync(SqlConnection connection, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT name, system_type_id, user_type_id FROM sys.types");

		var result = new List<GetSysTypesRow>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			int ordName = reader.GetOrdinal("name");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");

			do
			{
				result.Add(new GetSysTypesRow
				{
					Name = GetNonNullField<String>(reader, ordName),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
				});
			} while (await reader.ReadAsync(cancellationToken));
		}
		return result;
	}

	public static Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(SqlConnection connection, Int32 id) => GetTableTypeColumnsAsync(connection, id, CancellationToken.None);
	public static async Task<List<GetTableTypeColumnsRow>> GetTableTypeColumnsAsync(SqlConnection connection, Int32 id, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT C.is_nullable, C.max_length, C.name, t.name as [Type_Name], T.system_type_id, T.user_type_id from sys.columns as C join sys.types T ON (C.system_type_id = T.system_type_id) where C.object_id = @id and t.name <> 'sysname' order by c.column_id");

		cmd.Parameters.Add(CreateParameter("@id", id, SqlDbType.Int));

		var result = new List<GetTableTypeColumnsRow>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			int ordIsNullable = reader.GetOrdinal("is_nullable");
			int ordMaxLength = reader.GetOrdinal("max_length");
			int ordName = reader.GetOrdinal("name");
			int ordTypeName = reader.GetOrdinal("Type_Name");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");

			do
			{
				result.Add(new GetTableTypeColumnsRow
				{
					IsNullable = GetFieldValue<Boolean>(reader, ordIsNullable),
					MaxLength = GetNonNullFieldValue<Int16>(reader, ordMaxLength),
					Name = GetField<String>(reader, ordName),
					TypeName = GetNonNullField<String>(reader, ordTypeName),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
				});
			} while (await reader.ReadAsync(cancellationToken));
		}
		return result;
	}

	public static Task<List<GetTableTypesRow>> GetTableTypesAsync(SqlConnection connection) => GetTableTypesAsync(connection, CancellationToken.None);
	public static async Task<List<GetTableTypesRow>> GetTableTypesAsync(SqlConnection connection, CancellationToken cancellationToken)
	{
		using SqlCommand cmd = CreateStatement(connection, "SELECT T.name, T.type_table_object_id, S.name as [Schema_Name], T.system_type_id, T.user_type_id FROM sys.table_types AS T INNER JOIN sys.schemas as S ON (T.schema_id = S.schema_id) ORDER BY S.name, T.name");

		var result = new List<GetTableTypesRow>();
		using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			int ordName = reader.GetOrdinal("name");
			int ordTypeTableObjectId = reader.GetOrdinal("type_table_object_id");
			int ordSchemaName = reader.GetOrdinal("Schema_Name");
			int ordSystemTypeId = reader.GetOrdinal("system_type_id");
			int ordUserTypeId = reader.GetOrdinal("user_type_id");

			do
			{
				result.Add(new GetTableTypesRow
				{
					Name = GetNonNullField<String>(reader, ordName),
					TypeTableObjectId = GetNonNullFieldValue<Int32>(reader, ordTypeTableObjectId),
					SchemaName = GetNonNullField<String>(reader, ordSchemaName),
					SystemTypeId = GetNonNullFieldValue<Byte>(reader, ordSystemTypeId),
					UserTypeId = GetNonNullFieldValue<Int32>(reader, ordUserTypeId),
				});
			} while (await reader.ReadAsync(cancellationToken));
		}
		return result;
	}

}
