using Pingmint.CodeGen.Sql.Model;
using Pingmint.CodeGen.Sql.Model.Yaml;
using Microsoft.Data.SqlClient;
using System.Data;

using static System.Console;
using static Pingmint.CodeGen.Sql.Globals;

namespace Pingmint.CodeGen.Sql;

internal sealed class Program
{
    internal static async Task Main(string[] args)
    {
        if (args.Length < 1)
        {
            throw new InvalidOperationException(); // TODO: print help message
        }

        string path = args[0];
        if (!File.Exists(path))
        {
            throw new InvalidOperationException($"File not found: {path}");
        }
        var yaml = File.ReadAllText(path);

        var config = ParseYaml(yaml);
        if (config is not { CSharp: { Namespace: { Length: > 0 } } }) { throw new InvalidOperationException("Failed to parse YAML."); }

        var t0 = DateTime.Now;

        const int chunkSize = 20;

        // TODO: Add CancellationToken to all async methods
        // TODO: Add optional Transaction to all methods

        var sync = new ConsoleSynchronizationContext();
        sync.Go(async () =>
        {
            var codeFile = new CodeFile();
            codeFile.Namespace = config.CSharp.Namespace;
            codeFile.ClassName = config.CSharp.ClassName;
            codeFile.TypeKeyword = config.CSharp.TypeKeyword;
            codeFile.AllowAsync = config?.SqlClient?.Async != "false";

            if (config?.Databases?.Items is { } databases)
            {
                foreach (var database in databases)
                {
                    var databaseName = database.SqlName ?? throw new InvalidOperationException("Database name is required.");
                    var analyzer = new Analyzer(databaseName, codeFile, config, database.Connection?.ConnectionString ?? config.Connection?.ConnectionString);

                    // TODO: reenable ConsoleSynchronizationContext
                    var tasks = new Task<int>[chunkSize];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = Task.FromResult(i);
                    }

                    if (database.Constants?.Items is { } constants)
                    {
                        foreach (var constant in constants)
                        {
                            var index = await await Task.WhenAny(tasks);
                            tasks[index] = analyzer.AnalyzeConstantAsync(databaseName, constant.Name, constant.Query, constant.Attributes.Name, constant.Attributes.Value).ContinueWith(_ => index);
                        }
                    }

                    if (database.Statements?.Items is { } statements)
                    {
                        foreach (var statement in statements)
                        {
                            var parameters = statement.Parameters?.Items.Select(p => new SqlStatementParameter(p.Name, p.Type)).ToList() ?? new();

                            var index = await await Task.WhenAny(tasks);
                            tasks[index] = analyzer.AnalyzeStatementAsync(databaseName, statement.Name, statement.Text, parameters).ContinueWith(_ => index);
                        }
                    }

                    if (database.Procedures?.Included is { } included)
                    {
                        List<(String Schema, String Name)> excludeSchemaProcList = new();
                        if (database.Procedures?.Excluded is { } excludeProcs)
                        {
                            foreach (var item in excludeProcs)
                            {
                                var (schema, procName) = ParseSchemaItem(item.Text);
                                if (schema is not null && procName is not null)
                                {
                                    excludeSchemaProcList.Add((schema, procName));
                                }
                            }
                        }
                        Boolean IsExcluded(String procSchema, String procName)
                        {
                            foreach (var (exSchema, exName) in excludeSchemaProcList)
                            {
                                if (procSchema != exSchema) continue;
                                if (procName == exName) return true;
                                if (exName == "*") return true;
                            }
                            return false;
                        }

                        // TODO: bottleneck
                        var actualIncluded = new List<(String, String, Int32)>();
                        foreach (var include in included)
                        {
                            if (String.IsNullOrEmpty(include.Text)) { continue; }
                            using var sql = new SqlConnection(config.Connection.ConnectionString);
                            await sql.OpenAsync();
                            await sql.ChangeDatabaseAsync(databaseName);
                            var (schema, procName) = ParseSchemaItem(include.Text);
                            if (procName == "*")
                            {
                                foreach (var row in await Database.GetProceduresForSchemaAsync(sql, schema, CancellationToken.None))
                                {
                                    if (IsExcluded(schema, row.Name)) { continue; }
                                    var newProc = new Procedure();
                                    actualIncluded.Add((schema, row.Name, row.ObjectId));
                                }
                            }
                            else
                            {
                                if (IsExcluded(schema, procName)) { continue; }
                                WriteLine("Database.GetProcedureForSchemaAsync");
                                if ((await Database.GetProcedureForSchemaAsync(sql, schema, procName, CancellationToken.None)).FirstOrDefault() is not { } row) { continue; }
                                actualIncluded.Add((schema, procName, row.ObjectId));
                            }
                        }

                        foreach (var (schema, procName, objectId) in actualIncluded)
                        {
                            var index = await await Task.WhenAny(tasks);
                            tasks[index] = analyzer.AnalyzeProcedureAsync(databaseName, schema, procName, objectId).ContinueWith(_ => index);
                        }
                    }

                    await Task.WhenAll(tasks);
                }
            }

            using TextWriter textWriter = args.Length switch
            {
                > 1 => new StreamWriter(args[1]),
                _ => Console.Out
            };
            textWriter.Write(codeFile.GenerateCode());
            await textWriter.FlushAsync();
            textWriter.Close();

        });

        var t1 = DateTime.Now;
        Console.WriteLine($"Elapsed: {(t1 - t0).TotalSeconds:0.0} seconds");
    }

    private static Config ParseYaml(String yaml)
    {
        using var stringReader = new StringReader(yaml);
        var parser = new YamlDotNet.Core.Parser(stringReader);
        var doc = new Yaml.DocumentYaml();
        var visitor = new Yaml.YamlVisitor(doc);
        while (parser.MoveNext())
        {
            parser.Current!.Accept(visitor);
        }
        var model = doc.Model;
        return model;
    }
}
