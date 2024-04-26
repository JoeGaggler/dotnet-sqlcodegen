namespace Pingmint.CodeGen.Sql.Model.Yaml;

public class Config
{
    // scalar

    // mapping
    public Connection? Connection { get; set; }
    public CSharp? CSharp { get; set; }
    public Databases? Databases { get; set; }

    // sequence
}

public class Connection
{
    // scalar
    public String? ConnectionString { get; set; }

    // mapping

    // sequence
}

public class CSharp
{
    // scalar

    // mapping
    public String? Namespace { get; set; }
    public String? ClassName { get; set; }
    public String? TypeKeyword { get; set; }

    // sequence
}

// public class Class
// {
//     // scalar

//     // mapping

//     // sequence
// }


// public class Class
// {
//     // scalar

//     // mapping

//     // sequence
// }

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
    public String? SqlName { get; set; }
    public DatabasesItemProcedures? Procedures { get; set; }
    public DatabasesItemStatements? Statements { get; set; }
    public DatabaseItemsConstants? Constants { get; set; }

    // sequence
}

public class DatabasesItemProcedures
{
    // scalar

    // mapping
    public List<Procedure>? Included { get; set; }
    public List<Procedure>? Excluded { get; set; }

    // sequence is forwarded to "Included"
}

public class Procedure
{
    // scalar
    public String? Text { get; set; }

    // mapping
    public Parameters? Parameters { get; set; } // TODO: this is not yet in YAML

    // sequence

    // meta
    public List<Column>? Columns { get; set; }
}

public class DatabaseItemsConstants
{
    // scalar

    // mapping

    // sequence
    public List<Constant>? Items { get; set; }
}

public class Constant
{
    // scalar

    // mapping
    public String? Name { get; set; }
    public String? Query { get; set; }
    public Attributes? Attributes { get; set; }

    // sequence
}

public class Attributes
{
    // scalar

    // mapping
    public String? Name { get; set; }
    public String? Value { get; set; }

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
    public Boolean IsTableType { get; set; }
    public Int32? MaxLength { get; set; }
    public SqlTypeId SqlTypeId { get; set; }
}
