using System.Data;

namespace Pingmint.CodeGen.Sql.Model;

public record struct SqlTypeId : IComparable<SqlTypeId>
{
    public Int32 SchemaId;
    public Int32 SystemTypeId;
    public Int32 UserTypeId;

    public readonly int CompareTo(SqlTypeId other)
    {
        if (SchemaId.CompareTo(other.SchemaId) is var comp1 && comp1 != 0) { return comp1; }
        if (SystemTypeId.CompareTo(other.SystemTypeId) is var comp2 && comp2 != 0) { return comp2; }
        return UserTypeId.CompareTo(other.UserTypeId);
    }
}

public class Column
{
    public String Name { get; set; }
    public SqlDbType Type { get; set; }
    public Boolean IsNullable { get; set; }
    public short? MaxLength { get; set; }
    public SqlTypeId SqlTypeId { get; set; }
}

public class ConfigMemo
{
    public String? Namespace { get; set; }
    public String? ClassName { get; set; }

    /// <summary>
    /// Databases sorted by its SQL name
    /// </summary>
    /// <typeparam name="String">SQL name of the database</typeparam>
    /// <typeparam name="DatabaseMemo">Database memo</typeparam>
    public SortedDictionary<String, DatabaseMemo> Databases { get; } = new();
}

public class DatabaseMemo
{
    public String SqlName { get; set; }
    public String ClassName { get; set; }

    /// <summary>
    /// Record classes sorted by its C# class name
    /// </summary>
    public SortedDictionary<String, RecordMemo> Records { get; } = new();
    public SortedDictionary<Int32, SchemaMemo> Schemas { get; } = new();
    public SortedDictionary<String, CommandMemo> Statements { get; } = new();
    public SortedDictionary<SqlTypeId, TypeMemo> Types { get; } = new();
}

public class TypeMemo
{
    public String SqlName { get; set; }
    public SqlTypeId SqlTypeId { get; set; }
    public SqlDbType SqlDbType { get; set; }
    public Type? DotnetType { get; set; }
}

public class SchemaMemo
{
    public Int32 SchemaId { get; set; }
    public String SqlName { get; set; }
    public String ClassName { get; set; }

    public SortedDictionary<SqlTypeId, TableTypeMemo> TableTypes { get; } = new();
    public SortedDictionary<String, CommandMemo> Procedures { get; } = new();
    public SortedDictionary<String, RecordMemo> Records { get; } = new();
}

public class RecordMemo
{
    public String Name { get; set; }

    public List<PropertyMemo> Properties { get; } = new();
    public TableTypeMemo? ParentTableType { get; set; } = null;
}

public class PropertyMemo
{
    public Boolean IsNullable { get; set; }
    public Type? Type { get; set; }
    public String Name { get; set; }
}

public class TableTypeMemo
{
    public String TypeName { get; set; }
    public String SchemaName { get; set; }
    public String RowClassName { get; set; }
    public String RowClassRef { get; set; }
    public String DataTableClassName { get; set; }
    public String DataTableClassRef { get; set; }
    public List<ColumnMemo> Columns { get; set; }
    public Boolean IsReferenced { get; set; } = false;
    public SqlTypeId SqlTypeId { get; set; }
}

public class CommandMemo
{
    public CommandType CommandType { get; set; }
    public String CommandText { get; set; }
    public String MethodName { get; set; }
    public String? RowClassName { get; set; }
    public Boolean IsNonQuery { get; set; }
    public List<ColumnMemo> Columns { get; set; }
    public List<ParametersMemo> Parameters { get; set; }
}

public class ColumnMemo
{
    public String OrdinalVarName { get; set; }
    public String ColumnName { get; set; }
    public Boolean ColumnIsNullable { get; set; }
    public Type PropertyType { get; set; }
    public String PropertyTypeName { get; set; }
    public String PropertyName { get; set; }
    public String FieldTypeName { get; set; }
    public short? MaxLength { get; set; }
}

public class ParametersMemo
{
    public String ParameterName { get; set; }
    public SqlDbType ParameterType { get; set; }
    public String ParameterTableRef { get; set; }
    public String ArgumentType { get; set; }
    public String ArgumentName { get; set; }
    public String ArgumentExpression { get; set; }
    public Int32? MaxLength { get; set; }
    public SqlTypeId SqlTypeId { get; set; }
}
