using System.Data;
using Microsoft.Data.SqlClient;

namespace Pingmint.CodeGen.Sql;

public static partial class Proxy
{
    public static partial class tempdb
    {
    }

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

    public static async Task<List<tempdb.DmDescribeFirstResultSetRow>> DmDescribeFirstResultSetAsync(SqlConnection conn, System.String? text)
    {
        using SqlCommand cmd = conn.CreateCommand();

        cmd.CommandType = CommandType.Text;
        cmd.CommandText = "SELECT D.name, D.system_type_id, D.is_nullable, D.column_ordinal, T.name as [type_name] FROM sys.dm_exec_describe_first_result_set(@text, NULL, NULL) AS D JOIN sys.types AS T ON (D.system_type_id = T.system_type_id and T.user_type_id = D.system_type_id) ORDER BY D.column_ordinal";

        cmd.Parameters.Add(CreateParameter("@text", text, SqlDbType.VarChar));

        var result = new List<tempdb.DmDescribeFirstResultSetRow>();
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            if (await reader.ReadAsync())
            {
                int ordname = reader.GetOrdinal("name");
                int ordsystemtypeid = reader.GetOrdinal("system_type_id");
                int ordisnullable = reader.GetOrdinal("is_nullable");
                int ordcolumnordinal = reader.GetOrdinal("column_ordinal");
                int ordTypeName = reader.GetOrdinal("type_name");

                do
                {
                    var row = new tempdb.DmDescribeFirstResultSetRow();
                    row.name = (!reader.IsDBNull(ordname) ? reader.GetFieldValue<String>(ordname).Trim() : null);
                    row.system_type_id = (!reader.IsDBNull(ordsystemtypeid) ? reader.GetFieldValue<Int32>(ordsystemtypeid) : null);
                    row.is_nullable = (!reader.IsDBNull(ordisnullable) ? reader.GetFieldValue<Boolean>(ordisnullable) : null);
                    row.column_ordinal = (!reader.IsDBNull(ordcolumnordinal) ? reader.GetFieldValue<Int32>(ordcolumnordinal) : null);
                    row.type_name = (!reader.IsDBNull(ordTypeName) ? reader.GetFieldValue<String>(ordTypeName).Trim() : null! /* BUG! */);
                    result.Add(row);
                } while (await reader.ReadAsync());
            }
        }

        return result;
    }
}
