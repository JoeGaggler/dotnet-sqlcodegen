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
var message2 = await Proxy.HelloAsync(connection, "Joe");
var records2 = await Proxy.MyProcAsync(connection);
```
