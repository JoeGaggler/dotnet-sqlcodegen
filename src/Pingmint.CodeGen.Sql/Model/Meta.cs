using System.Data;

namespace Pingmint.CodeGen.Sql.Model;

public class ResultSetMeta
{
    public List<Column>? Columns { get; set; }
}

public class Column
{
    public String Name { get; set; }
    public SqlDbType Type { get; set; }
    public Boolean IsNullable { get; set; }
}
