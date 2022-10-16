namespace Pingmint.CodeGen.Sql.Model;

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

// public class Class
// {
//     // scalar

//     // mapping

//     // sequence
// }
