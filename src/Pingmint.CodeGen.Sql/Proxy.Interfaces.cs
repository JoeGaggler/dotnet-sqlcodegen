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
    Int32? SystemTypeId { get; set; }
    Boolean? IsNullable { get; set; }
    Int32? ColumnOrdinal { get; set; }
    String TypeName { get; set; }
}
