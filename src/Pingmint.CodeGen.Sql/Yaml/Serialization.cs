using Pingmint.CodeGen.Sql.Model.Yaml;
using Pingmint.Yaml;
using Model = Pingmint.CodeGen.Sql.Model.Yaml;

namespace Pingmint.Yaml;

internal sealed class DocumentYaml : IDocument
{
    public Model.Config Model { get; private set; } = new();

    public IMapping? StartMapping() => new ConfigMapping(m => this.Model = m);

    public ISequence? StartSequence() => null;
}

internal sealed class ConfigMapping : Mapping<Model.Config> // passthrough
{
    public ConfigMapping(Action<Model.Config> callback) : base(callback, new()) { } // passthrough

    protected override IMapping? StartMapping(string key) => key switch
    {
        "csharp" => new CSharpMapping(m => this.Model.CSharp = m),
        _ => null,
    };

    protected override ISequence? StartSequence(string key) => key switch
    {
        "databases" => new DatabasesSequence(m => this.Model.Databases = new() { Items = m }),
        _ => null,
    };

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            case "connection": this.Model.Connection = new() { ConnectionString = value }; return true;
            default: return false;
        }
    }
}

internal sealed class CSharpMapping : Mapping<Model.CSharp>
{
    public CSharpMapping(Action<Model.CSharp> callback) : base(callback, new()) { }

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            case "namespace": this.Model.Namespace = value; return true;
            case "class": this.Model.ClassName = value; return true;
            case "row type": this.Model.TypeKeyword = value; return true;
            default: return false;
        }
    }
}

internal sealed class DatabasesSequence : Sequence<List<Model.DatabasesItem>>
{
    public DatabasesSequence(Action<List<Model.DatabasesItem>> callback) : base(callback, new()) { }

    protected override IMapping? StartMapping() => new DatabaseMapping(m => this.Model.Add(m));
}

internal sealed class DatabaseMapping : Mapping<Model.DatabasesItem>
{
    public DatabaseMapping(Action<Model.DatabasesItem> callback) : base(callback, new()) { }

    protected override ISequence? StartSequence(String key) => key switch
    {
        "procedures" => new ProceduresSequence(m => this.Model.Procedures = new() { Included = m }),
        "statements" => new StatementsSequence(m => this.Model.Statements = m),
        "constants" => new ConstantsSequence(m => this.Model.Constants = m),
        _ => null,
    };

    protected override IMapping? StartMapping(string key) => key switch
    {
        "procedures" => new ProceduresMapping(m => this.Model.Procedures = m),
        _ => null,
    };

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            case "database": { this.Model.SqlName = value; return true; }
            default: return false;
        }
    }
}

internal sealed class ProceduresMapping : Mapping<Model.DatabasesItemProcedures>
{
    public ProceduresMapping(Action<Model.DatabasesItemProcedures> callback) : base(callback, new()) { }

    protected override ISequence? StartSequence(string key) => key switch
    {
        "include" => new ProceduresSequence(m => this.Model.Included = m),
        "exclude" => new ProceduresSequence(m => this.Model.Excluded = m),
        _ => null,
    };
}

internal sealed class ProceduresSequence : Sequence<List<Model.Procedure>>
{
    public ProceduresSequence(Action<List<Model.Procedure>> callback) : base(callback, new()) { }

    protected override IMapping? StartMapping() => new ProcedureMapping(m => this.Model.Add(m));

    protected override bool Add(string value)
    {
        this.Model.Add(new() { Text = value });
        return true;
    }
}

internal sealed class ProcedureMapping : Mapping<Model.Procedure>
{
    public ProcedureMapping(Action<Model.Procedure> callback) : base(callback, new()) { }

    protected override ISequence? StartSequence(string key) => key switch
    {
        // "parameters" => new ParametersSequence(this.Model.Parameters = new()),
        _ => null,
    };

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            // case "name": { this.Model.Name = value; return true; }
            // case "text": { this.Model.Text = value; return true; }
            default: return false;
        }
    }
}

internal sealed class ConstantsSequence : Sequence<Model.DatabaseItemsConstants>
{
    public ConstantsSequence(Action<Model.DatabaseItemsConstants> callback) : base(callback, new() { Items = new() }) { }

    protected override IMapping? StartMapping() => new ConstantMapping(m => this.Model.Items.Add(m));
}

internal sealed class ConstantMapping : Mapping<Model.Constant>
{
    public ConstantMapping(Action<Model.Constant> callback) : base(callback, new()) { }

    protected override IMapping? StartMapping(string key) => key switch
    {
        "attributes" => new AttributesMapping(m => this.Model.Attributes = m),
        _ => null,
    };

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            case "name": { this.Model.Name = value; return true; }
            case "query": { this.Model.Query = value; return true; }
            default: return false;
        }
    }
}

internal sealed class AttributesMapping : Mapping<Model.Attributes>
{
    public AttributesMapping(Action<Model.Attributes> callback) : base(callback, new()) { }

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            case "name": { this.Model.Name = value; return true; }
            case "value": { this.Model.Value = value; return true; }
            default: return false;
        }
    }
}


internal sealed class StatementsSequence : Sequence<Model.DatabasesItemStatements>
{
    public StatementsSequence(Action<Model.DatabasesItemStatements> callback) : base(callback, new() { Items = new() }) { }

    protected override IMapping? StartMapping() => new StatementMapping(m => this.Model.Items.Add(m));
}

internal sealed class StatementMapping : Mapping<Model.Statement>
{
    public StatementMapping(Action<Model.Statement> callback) : base(callback, new()) { }

    protected override ISequence? StartSequence(string key) => key switch
    {
        "parameters" => new ParametersSequence(m => this.Model.Parameters = new() { Items = m }),
        _ => null,
    };

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            case "name": { this.Model.Name = value; return true; }
            case "text": { this.Model.Text = value; return true; }
            default: return false;
        }
    }
}

internal sealed class ParametersSequence : Sequence<List<Model.Parameter>>
{
    public ParametersSequence(Action<List<Model.Parameter>> callback) : base(callback, new()) { }

    protected override IMapping? StartMapping() => new ParameterMapping(m => this.Model.Add(m));
}

internal sealed class ParameterMapping : Mapping<Model.Parameter>
{
    public ParameterMapping(Action<Model.Parameter> callback) : base(callback, new()) { }

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            case "name": { this.Model.Name = value; return true; }
            case "type": { this.Model.Type = value; return true; }
            default: return false;
        }
    }
}
