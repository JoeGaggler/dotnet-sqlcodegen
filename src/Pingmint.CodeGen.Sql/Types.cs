using Pingmint.CodeGen.Sql.Model;
//using Pingmint.CodeGen.Sql.Refactor;
using System.Data;

namespace Pingmint.CodeGen.Sql;

public record class SqlStatementParameter(String Name, String Type);

public class Method
{
    /// <summary>
    /// Name of the method
    /// </summary>
    public String? Name { get; set; }

    /// <summary>
    /// SQL command text
    /// </summary>
    public string CommandText { get; internal set; }

    /// <summary>
    /// Data type returned by the method
    /// </summary>
    public String? DataType { get; set; }

    public Boolean IsStoredProc { get; set; }

    public List<MethodParameter> CSharpParameters { get; set; } = new();

    public List<CommandParameter> SqlParameters { get; set; } = new();

    /// <summary>
    /// Type of recordset
    /// </summary>
    public Record? Record { get; internal set; }
}

public class MethodParameter
{
    /// <summary>
    /// Name of the parameter
    /// </summary>
    public String? CSharpName { get; set; }

    /// <summary>
    /// Type of the parameter
    /// </summary>
    public String? CSharpType { get; set; }
}

public class CommandParameter
{
    /// <summary>
    /// Name of the parameter in the SQL command
    /// </summary>
    public String? SqlName { get; set; }

    /// <summary>
    /// SqlDbType of the parameter in the SQL command
    /// </summary>
    public SqlDbType? SqlDbType { get; set; }

    /// <summary>
    /// Argument to CreateParameter if needed, e.g. for table types
    /// </summary>
    public String? SqlTypeName { get; set; }

    /// <summary>
    /// Reference to the value passed in to the command
    /// </summary>
    public string CSharpExpression { get; internal set; }

    public short? MaxLength { get; internal set; }
}

public class Record
{
    /// <summary>
    /// Name of the C# type
    /// </summary>
    public String? CSharpName { get; set; }

    public List<RecordProperty> Properties { get; } = new();

    public Boolean IsTableType = false;
    public String? TableTypeCSharpName { get; set; }
}

public class RecordProperty
{
    /// <summary>
    /// Name of the C# property
    /// </summary>
    public String? FieldName { get; set; }

    /// <summary>
    /// Type of the C# property
    /// </summary>
    public String FieldType { get; set; }

    public String FieldTypeForGeneric { get; set; }

    /// <summary>
    /// Name of the SQL column in the recordset
    /// </summary>
    public string ColumnName { get; set; }
    public bool FieldTypeIsValueType { get; internal set; }
    public bool ColumnIsNullable { get; internal set; }
    public short? MaxLength { get; init; }
}
