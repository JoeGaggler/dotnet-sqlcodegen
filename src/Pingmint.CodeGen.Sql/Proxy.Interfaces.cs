using System.Data;
using Microsoft.Data.SqlClient;

namespace Pingmint.CodeGen.Sql;

partial class DmDescribeFirstResultSetRow : IDmDescribeFirstResultSetRow
{

}

partial class DmDescribeFirstResultSetForObjectRow : IDmDescribeFirstResultSetRow
{

}

public interface IDmDescribeFirstResultSetRow
{
    String? Name { get; set; }
    Byte SystemTypeId { get; set; }
    Int32 UserTypeId { get; set; }
    Boolean? IsNullable { get; set; }
    Int32? ColumnOrdinal { get; set; }
    String SqlTypeName { get; set; }
}
