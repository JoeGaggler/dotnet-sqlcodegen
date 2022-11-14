using System.Data;
using Microsoft.Data.SqlClient;

namespace Pingmint.CodeGen.Sql;

public interface ISqlTypeId
{
    Int32 SchemaId { get; set; }
    Byte SystemTypeId { get; set; }
    Int32 UserTypeId { get; set; }
}

public static class SqlTypeIdExtensions
{
    public static Model.SqlTypeId GetSqlTypeId(this ISqlTypeId sqlTypeId) => new() { SchemaId = sqlTypeId.SchemaId, SystemTypeId = sqlTypeId.SystemTypeId, UserTypeId = sqlTypeId.UserTypeId };
}

partial class DmDescribeFirstResultSetRow : IDmDescribeFirstResultSetRow { }

partial class DmDescribeFirstResultSetForObjectRow : IDmDescribeFirstResultSetRow { }

public interface IDmDescribeFirstResultSetRow : ISqlTypeId
{
    String? Name { get; set; }
    Boolean? IsNullable { get; set; }
    Int32? ColumnOrdinal { get; set; }
    String SqlTypeName { get; set; }
}

partial class GetSysTypesRow : ISqlTypeId { }
partial class GetTableTypesRow : ISqlTypeId { }
partial class GetTableTypeColumnsRow : ISqlTypeId { }
partial class GetParametersForObjectRow : ISqlTypeId { }
