# dotnet-sqlcodegen

A dotnet tool that generates C# methods to execute SQL statements and procedures, using metadata fetched from a SQL Server database.

# Preamble

This tool is similar to other *object-relational mapping* (ORM) tools in the dotnet platform, but reverses the typical mapping: instead of defining database queries in C# that translate to SQL, this tool generates C# APIs from SQL queries. This ensures that SQL Server is the source of truth.

# Getting started
## Installation

Install the `pingmint.codegen.sql` dotnet tool either globally or locally.

For example as a global tool:
```bash
dotnet tool install -g pingmint.codegen.sql
```

## Definition

Create a `database.yml` definition file in your project:

```yml
connection: > # provide a valid SQL Server connection string
    Server=localhost;
    Database=tempdb;

csharp:
  namespace: Sample # namespace for all generated code
  class: Proxy      # name of the generated class containing the API methods

databases:
  - database: tempdb # SQL database containing the statements and procedures
    statements:
      - name: Hello # name of the generated method
        text: select 'Hello, ' + @name as [greeting]
        parameters:
          - name: name
            type: varchar(100)

    procedures:
      - dbo.*       # include all procedures in `dbo` schema
      - dbo.my_proc # include one specific procedure
```

Run the tool to generate the C# code:

```bash
# global tool
sqlcodegen database.yml Database.cs

# local tool
dotnet tool run sqlcodegen database.yml Database.cs
```

You can now call the SQL statement or your own procedure from your C# code using static methods on the generated `Sample.Proxy` class:

```csharp
// create and open a connection to the database
SqlConnection connection = SomeConnectionFactory();

// synchronous
var message1 = Proxy.Hello(connection, "Joe");
var records1 = Proxy.MyProc(connection);

// asynchronous
var message2 = await Proxy.HelloAsync(connection, "Joe", cancellationToken);
var records2 = await Proxy.MyProcAsync(connection, cancellationToken);
```

# Features

## Constants

```yml
databases:
  - database: tempdb
    constants:
      - name: StatusCodes
        query: SELECT id, status FROM Status_Codes
        attributes:
          name: status
          value: id
```

Output:
```csharp
public static partial class StatusCodes
{
  public const Int32 Created = 1;
  public const Int32 Loading = 2;
  public const Int32 Visible = 3;
  public const Int32 Deleted = 4;
  // etc
}

```

## Multiple connection strings

Each database can have its own connection string. Add a `connection` property to an item in the `databases` list to override the default connection string.

```yml
connection: > # default connection string
  Server=localhost;
  Database=db2;
databases:
  - database: db1
    connection: > # connection string override
      Server=localhost;
      Database=db1;
  - database: db2 # uses default
```

## SqlClient options

Some versions of [SqlClient](https://github.com/dotnet/SqlClient) have known issues that require mitigations via different code generation, which are controllable via the `sqlclient` section in the `database.yml` file.

### `async`

Set `async: false` to avoid using the async methods of the `SqlClient` package, while still generating the async API methods. This is useful when you want to use async methods in calling code, but want to temporarily avoid the known issues in `SqlClient`:

```yml
sqlclient:
  async: false
```
