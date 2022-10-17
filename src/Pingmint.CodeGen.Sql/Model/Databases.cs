namespace Pingmint.CodeGen.Sql.Model;

public class Databases
{
    // scalar

    // mapping

    // sequence
    public List<DatabasesItem>? Items { get; set; }
}

public class DatabasesItem
{
    // scalar

    // mapping
    public String? Name { get; set; }
    public DatabasesItemStatements? Statements { get; set; }

    // sequence
}

public class DatabasesItemStatements
{
    // scalar

    // mapping

    // sequence
    public List<Statement>? Items { get; set; }
}

public class Statement
{
    // scalar

    // mapping
    public String? Name { get; set; }
    public String? Text { get; set; }
    public Parameters? Parameters { get; set; }

    // sequence

    // meta
    public ResultSetMeta ResultSet { get; set; }
}

public class Parameters
{
    // scalar

    // mapping

    // sequence
    public List<Parameter>? Items { get; set; }
}

public class Parameter
{
    // scalar

    // mapping
    public String? Name { get; set; }
    public String? Type { get; set; }

    // sequence

    // meta
    public System.Data.SqlDbType SqlDbType { get; set; }
}

// public class Class
// {
//     // scalar

//     // mapping

//     // sequence
// }
