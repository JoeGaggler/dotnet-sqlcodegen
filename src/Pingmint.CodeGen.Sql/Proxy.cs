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
	public partial class tempdb
	{
		public partial class GetSysTypesRow
		{
			public String name { get; set; }
		}
	}
}
