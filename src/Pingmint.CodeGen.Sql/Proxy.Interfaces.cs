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

partial record class DmDescribeFirstResultSetRow : IDmDescribeFirstResultSetRow { }

partial record class DmDescribeFirstResultSetForObjectRow : IDmDescribeFirstResultSetRow { }

public interface IDmDescribeFirstResultSetRow : ISqlTypeId
{
    String? Name { get; set; }
    Boolean? IsNullable { get; set; }
    Int32? ColumnOrdinal { get; set; }
    String SqlTypeName { get; set; }
}

partial record class GetSysTypesRow : ISqlTypeId { }
partial record class GetTableTypesRow : ISqlTypeId { }
partial record class GetTableTypeColumnsRow : ISqlTypeId { }
partial record class GetParametersForObjectRow : ISqlTypeId { }
