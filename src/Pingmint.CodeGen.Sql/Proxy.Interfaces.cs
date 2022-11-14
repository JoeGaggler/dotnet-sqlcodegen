using System.Data;
using Microsoft.Data.SqlClient;

namespace Pingmint.CodeGen.Sql;

partial class DmDescribeFirstResultSetRow : IDmDescribeFirstResultSetRow, ISqlTypeId
{

}

partial class DmDescribeFirstResultSetForObjectRow : IDmDescribeFirstResultSetRow, ISqlTypeId
{

}

public interface IDmDescribeFirstResultSetRow
{
    String? Name { get; set; }
    Int32 SchemaId { get; set; }
    Byte SystemTypeId { get; set; }
    Int32 UserTypeId { get; set; }
    Boolean? IsNullable { get; set; }
    Int32? ColumnOrdinal { get; set; }
    String SqlTypeName { get; set; }
}

partial class GetSysTypesRow : ISqlTypeId { }
partial class GetTableTypesRow : ISqlTypeId { }
partial class GetTableTypeColumnsRow : ISqlTypeId { }
partial class GetParametersForObjectRow : ISqlTypeId { }

public interface ISqlTypeId
{
    Int32 SchemaId { get; set; }
    Byte SystemTypeId { get; set; }
    Int32 UserTypeId { get; set; }

    Model.SqlTypeId SqlTypeId
    {
        get
        {
            return new() { SchemaId = SchemaId, SystemTypeId = SystemTypeId, UserTypeId = UserTypeId };
        }
    }
}
