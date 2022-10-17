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
}
