using System.Data;
using Microsoft.Data.SqlClient;

namespace Pingmint.CodeGen.Sql;

public static partial class Proxy
{
	public static async Task<List<tempdb.GetSysTypesRow>> GetSysTypesAsync(SqlConnection connection)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT name FROM sys.types";

		var result = new List<tempdb.GetSysTypesRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ordName = reader.GetOrdinal("name");

				do
				{
					var row = new tempdb.GetSysTypesRow
					{
						name = reader.IsDBNull(ordName) ? null! : reader.GetFieldValue<String>(ordName),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<tempdb.DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlConnection connection, String text)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT D.name, D.system_type_id, D.is_nullable, D.column_ordinal, T.name as [type_name] FROM sys.dm_exec_describe_first_result_set(@text, NULL, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id AND T.user_type_id = D.system_type_id) ORDER BY D.column_ordinal";

		cmd.Parameters.Add(CreateParameter("@text", text, SqlDbType.VarChar));

		var result = new List<tempdb.DmDescribeFirstResultSetRow>();
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
					var row = new tempdb.DmDescribeFirstResultSetRow
					{
						name = reader.IsDBNull(ordName) ? null! : reader.GetFieldValue<String>(ordName),
						system_type_id = reader.IsDBNull(ordSystemTypeId) ? null! : reader.GetFieldValue<Int32>(ordSystemTypeId),
						is_nullable = reader.IsDBNull(ordIsNullable) ? null! : reader.GetFieldValue<Boolean>(ordIsNullable),
						column_ordinal = reader.IsDBNull(ordColumnOrdinal) ? null! : reader.GetFieldValue<Int32>(ordColumnOrdinal),
						type_name = reader.IsDBNull(ordTypeName) ? null! : reader.GetFieldValue<String>(ordTypeName),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public partial class tempdb
	{
		public partial class GetSysTypesRow
		{
			public String name { get; set; }
		}
	}
	public partial class tempdb
	{
		public partial class DmDescribeFirstResultSetRow
		{
			public String? name { get; set; }
			public Int32? system_type_id { get; set; }
			public Boolean? is_nullable { get; set; }
			public Int32? column_ordinal { get; set; }
			public String type_name { get; set; }
		}
	}
}
