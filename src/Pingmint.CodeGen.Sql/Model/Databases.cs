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
    public String? ClassName { get; set; }
    public DatabaseItemTableTypes? TableTypes { get; set; }
    public DatabasesItemProcedures? Procedures { get; set; }
    public DatabasesItemStatements? Statements { get; set; }

    // sequence

    // meta
    public DatabaseTypesMeta AllTypes { get; set; }
}

public class DatabaseItemTableTypes
{
    // scalar

    // mapping

    // sequence
    public List<TableType>? Items { get; set; }
}

public class TableType
{
    // scalar

    // mapping

    // sequence

    // meta
    public String SchemaName { get; set; }
    public String TypeName { get; set; }
    public List<Column>? Columns { get; set; }
    public Int32 SqlSystemTypeId { get; set; }
    public Int32 SqlUserTypeId { get; set; }
}

public class DatabasesItemProcedures
{
    // scalar

    // mapping

    // sequence
    public List<Procedure>? Items { get; set; }
}

public class Procedure
{
    // scalar
    public String? Text { get; set; }

    // mapping
    public Parameters? Parameters { get; set; } // TODO: this is not yet in YAML

    // sequence

    // meta
    public String? Name { get; set; }
    public String? Schema { get; set; }
    public ResultSetMeta ResultSet { get; set; }
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
    public Int32? MaxLength { get; set; }
    public SqlTypeId SqlTypeId { get; set; }
}

// public class Class
// {
//     // scalar

//     // mapping

//     // sequence
// }
