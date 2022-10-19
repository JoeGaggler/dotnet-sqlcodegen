using Pingmint.Yaml;
using Model = Pingmint.CodeGen.Sql.Model;

namespace Pingmint.Yaml;

internal sealed class DocumentYaml : IDocument
{
    public Model.Config Model { get; private set; } = new();

    public IMapping? StartMapping() => new ConfigMapping(this.Model);

    public ISequence? StartSequence() => null;
}

internal sealed class ConfigMapping : Mapping<Model.Config, Model.Config> // passthrough
{
    public ConfigMapping(Model.Config parent) : base(parent, parent) { } // passthrough

    protected override IMapping? StartMapping(string key) => key switch
    {
        "csharp" => new CSharpMapping(this.Model),
        _ => null,
    };

    protected override ISequence? StartSequence(string key) => key switch
    {
        "databases" => new DatabasesSequence(this.Model),
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

    protected override void Pop(Model.Config parentModel, Model.Config model) { } // passthrough already done
}

internal sealed class CSharpMapping : Mapping<Model.Config, Model.CSharp>
{
    public CSharpMapping(Model.Config parent) : base(parent, new()) { }

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            case "namespace": this.Model.Namespace = value; return true;
            case "class": this.Model.ClassName = value; return true;
            default: return false;
        }
    }

    protected override void Pop(Model.Config parentModel, Model.CSharp model) => parentModel.CSharp = model;
}

internal sealed class DatabasesSequence : Sequence<Model.Config, Model.Databases>
{
    public DatabasesSequence(Model.Config parent) : base(parent, new()) { }

    protected override IMapping? StartMapping() => new DatabaseMapping(this.Model);

    protected override void Pop(Model.Config parentModel, Model.Databases model) => parentModel.Databases = model;
}

internal sealed class DatabaseMapping : Mapping<Model.Databases, List<Model.DatabasesItem>>
{
    public DatabaseMapping(Model.Databases parent) : base(parent, new()) { }

    protected override IMapping? StartMapping(String key) => new DatabaseItemMapping(this.Model, key);

    protected override void Pop(Model.Databases parentModel, List<Model.DatabasesItem> model) => this.Parent.Items = this.Model;
}

internal sealed class DatabaseItemMapping : Mapping<List<Model.DatabasesItem>, Model.DatabasesItem>
{
    public DatabaseItemMapping(List<Model.DatabasesItem> parent, String key) : base(parent, new() { Name = key }) { }

    protected override ISequence? StartSequence(String key) => key switch
    {
        "procs" => new ProceduresSequence(this.Model),
        "procedures" => new ProceduresSequence(this.Model),
        "statements" => new StatementsSequence(this.Model),
        _ => null,
    };

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            case "class": { this.Model.ClassName = value; return true; }
            default: return false;
        }
    }

    protected override void Pop(List<Model.DatabasesItem> parentModel, Model.DatabasesItem model) => parentModel.Add(this.Model);
}

internal sealed class ProceduresSequence : Sequence<Model.DatabasesItem, Model.DatabasesItemProcedures>
{
    public ProceduresSequence(Model.DatabasesItem parent) : base(parent, new() { Items = new() }) { }

    protected override IMapping? StartMapping() => new ProcedureMapping(this.Model.Items);

    protected override bool Add(string value)
    {
        this.Model.Items!.Add(new() { Text = value }); // TODO: remove null-forgiveness
        return true;
    }

    protected override void Pop(Model.DatabasesItem parentModel, Model.DatabasesItemProcedures model) => parentModel.Procedures = model;
}

internal sealed class ProcedureMapping : Mapping<List<Model.Procedure>, Model.Procedure>
{
    public ProcedureMapping(List<Model.Procedure> parent) : base(parent, new()) { }

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

    protected override void Pop(List<Model.Procedure> parentModel, Model.Procedure model) => parentModel.Add(model);
}

internal sealed class StatementsSequence : Sequence<Model.DatabasesItem, Model.DatabasesItemStatements>
{
    public StatementsSequence(Model.DatabasesItem parent) : base(parent, new() { Items = new() }) { }

    protected override IMapping? StartMapping() => new StatementMapping(this.Model.Items);

    protected override void Pop(Model.DatabasesItem parentModel, Model.DatabasesItemStatements model) => parentModel.Statements = model;
}

internal sealed class StatementMapping : Mapping<List<Model.Statement>, Model.Statement>
{
    public StatementMapping(List<Model.Statement> parent) : base(parent, new()) { }

    protected override ISequence? StartSequence(string key) => key switch
    {
        "parameters" => new ParametersSequence(this.Model.Parameters = new()),
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

    protected override void Pop(List<Model.Statement> parentModel, Model.Statement model) => parentModel.Add(model);
}

internal sealed class ParametersSequence : Sequence<Model.Parameters, List<Model.Parameter>>
{
    public ParametersSequence(Model.Parameters parent) : base(parent, new()) { }

    protected override IMapping? StartMapping() => new ParameterMapping(this.Model);

    protected override void Pop(Model.Parameters parentModel, List<Model.Parameter> model) => parentModel.Items = model;
}

internal sealed class ParameterMapping : Mapping<List<Model.Parameter>, Model.Parameter>
{
    public ParameterMapping(List<Model.Parameter> parent) : base(parent, new()) { }

    protected override bool Add(string key, string value)
    {
        switch (key)
        {
            case "name": { this.Model.Name = value; return true; }
            case "type": { this.Model.Type = value; return true; }
            default: return false;
        }
    }

    protected override void Pop(List<Model.Parameter> parentModel, Model.Parameter model) => parentModel.Add(model);
}
