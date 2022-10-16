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
				int ord_name = reader.GetOrdinal("name");

				do
				{
					var row = new tempdb.GetSysTypesRow
					{
						name = reader.IsDBNull(ord_name) ? null! : reader.GetFieldValue<String>(ord_name),
					};
					result.Add(row);
				} while (await reader.ReadAsync());
			}
		}
		return result;
	}
	public static async Task<List<tempdb.DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlConnection connection)
	{
		using SqlCommand cmd = connection.CreateCommand();
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "SELECT D.name, D.system_type_id, D.is_nullable, D.column_ordinal, T.name as [type_name] FROM sys.dm_exec_describe_first_result_set(@text, NULL, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id) WHERE T.name <> 'sysname' ORDER BY D.column_ordinal";

		var result = new List<tempdb.DmDescribeFirstResultSetRow>();
		using (var reader = await cmd.ExecuteReaderAsync())
		{
			if (await reader.ReadAsync())
			{
				int ord_name = reader.GetOrdinal("name");
				int ord_system_type_id = reader.GetOrdinal("system_type_id");
				int ord_is_nullable = reader.GetOrdinal("is_nullable");
				int ord_column_ordinal = reader.GetOrdinal("column_ordinal");
				int ord_type_name = reader.GetOrdinal("type_name");

				do
				{
					var row = new tempdb.DmDescribeFirstResultSetRow
					{
						name = reader.IsDBNull(ord_name) ? null! : reader.GetFieldValue<String?>(ord_name),
						system_type_id = reader.IsDBNull(ord_system_type_id) ? null! : reader.GetFieldValue<Int32?>(ord_system_type_id),
						is_nullable = reader.IsDBNull(ord_is_nullable) ? null! : reader.GetFieldValue<Boolean?>(ord_is_nullable),
						column_ordinal = reader.IsDBNull(ord_column_ordinal) ? null! : reader.GetFieldValue<Int32?>(ord_column_ordinal),
						type_name = reader.IsDBNull(ord_type_name) ? null! : reader.GetFieldValue<String>(ord_type_name),
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
