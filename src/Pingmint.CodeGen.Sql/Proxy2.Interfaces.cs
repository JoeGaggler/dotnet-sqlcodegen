using System.Data;
using Microsoft.Data.SqlClient;

namespace Pingmint.CodeGen.Sql2;

public interface ISqlTypeId2
{
    Int32 SchemaId { get; set; }
    Byte SystemTypeId { get; set; }
    Int32 UserTypeId { get; set; }
}

public record struct SqlTypeId2 : IComparable<SqlTypeId2> // TODO: rename this back to SqlTypeId
{
    public Int32 SchemaId;
    public Int32 SystemTypeId;
    public Int32 UserTypeId;

    public readonly int CompareTo(SqlTypeId2 other)
    {
        if (SchemaId.CompareTo(other.SchemaId) is var comp1 && comp1 != 0) { return comp1; }
        if (SystemTypeId.CompareTo(other.SystemTypeId) is var comp2 && comp2 != 0) { return comp2; }
        return UserTypeId.CompareTo(other.UserTypeId);
    }
}

public static class SqlTypeIdExtensions2
{
    public static SqlTypeId2 GetSqlTypeId2(this ISqlTypeId2 sqlTypeId) => new() { SchemaId = sqlTypeId.SchemaId, SystemTypeId = sqlTypeId.SystemTypeId, UserTypeId = sqlTypeId.UserTypeId };
}

partial record class DmDescribeFirstResultSetRow : IDmDescribeFirstResultSetRow { }

// partial record class DmDescribeFirstResultSetForObjectRow : IDmDescribeFirstResultSetRow { }

public interface IDmDescribeFirstResultSetRow : ISqlTypeId2
{
    String? Name { get; set; }
    Boolean? IsNullable { get; set; }
    Int32? ColumnOrdinal { get; set; }
    String SqlTypeName { get; set; }
}

// partial record class GetSysTypesRow : ISqlTypeId2 { }
// partial record class GetTableTypesRow : ISqlTypeId2 { }
// partial record class GetTableTypeColumnsRow : ISqlTypeId2 { }
// partial record class GetParametersForObjectRow : ISqlTypeId2 { }
