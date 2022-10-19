using Pingmint.CodeGen.Sql.Model;
using YamlDotNet.Core.Events;

namespace Pingmint.Yaml;

internal interface IDocument
{
    IMapping? StartMapping();

    ISequence? StartSequence();
}

internal interface IMapping
{
    IMapping? StartMapping(String key);
    ISequence? StartSequence(String key);

    Boolean Add(String key, String value);
    void Pop();
}

internal interface ISequence
{
    IMapping? StartMapping();
    ISequence? StartSequence();
    Boolean Add(String value);
    void Pop();
}

internal sealed class YamlVisitor : IParsingEventVisitor
{
    private IDocument doc;

    public YamlVisitor(IDocument doc)
    {
        this.doc = doc;
    }

    private Boolean debug = false;
    private int __INDENTATION_LEVEL = 0;
    private void Debug(String message) { if (debug) Console.Out.WriteLine($"{new String(' ', __INDENTATION_LEVEL * 2)}{message}"); }
    private void Debug(String message, ParsingEvent e)
    {
        var n = e.NestingIncrease;
        if (n < 0) __INDENTATION_LEVEL += n;
        if (debug) Console.Out.WriteLine($"{new String(' ', __INDENTATION_LEVEL * 2)}{message}");
        if (n > 0) __INDENTATION_LEVEL += n;
    }

    private enum Mode { None, Mapping, Sequence }
    private Mode currentMode = Mode.None;
    private Stack<Mode> stackMode = new Stack<Mode>();
    private IMapping currentMapping;
    private Stack<IMapping> stackMapping = new Stack<IMapping>();
    private ISequence currentSequence;
    private Stack<ISequence> stackSequence = new Stack<ISequence>();
    private void Push(IMapping visitor)
    {
        Push(Mode.Mapping);
        stackMapping.Push(this.currentMapping);
        this.currentMapping = visitor;
    }

    private void Push(ISequence visitor)
    {
        Push(Mode.Sequence);
        stackSequence.Push(this.currentSequence);
        this.currentSequence = visitor;
    }

    private void Push(Mode newMode)
    {
        // Debug($"Push: {currentMode} -> {newMode}");
        stackMode.Push(currentMode);
        currentMode = newMode;
    }

    private void Pop()
    {
        var old = currentMode;
        currentMode = stackMode.Pop();
        switch (old)
        {
            case Mode.Mapping: currentMapping.Pop(); currentMapping = stackMapping.Pop(); break;
            case Mode.Sequence: currentSequence.Pop(); currentSequence = stackSequence.Pop(); break;
            default: throw new InvalidOperationException("Unexpected pop");
        }
        // Debug($"Pop: {currentMode} <- {old}");
        this.scalar = null;
        this.scalarIsKey = true;
    }

    private String? scalar = null;
    private Boolean scalarIsKey = true;

    public void Visit(AnchorAlias e) { }
    public void Visit(StreamStart e)
    {
        Debug($"Stream Start", e);
    }
    public void Visit(StreamEnd e)
    {
        Debug($"Stream End", e);
    }
    public void Visit(DocumentStart e)
    {
        Debug($"Doc Start", e);
    }
    public void Visit(DocumentEnd e)
    {
        Debug($"Doc End", e);
    }
    public void Visit(Scalar e)
    {
        Debug($"Scalar: {e.Value}", e);
        switch (this.currentMode)
        {
            case Mode.Mapping:
                {
                    if (scalarIsKey)
                    {
                        this.scalar = e.Value;
                        this.scalarIsKey = false;
                    }
                    else
                    {
                        if (!this.currentMapping.Add(this.scalar, e.Value))
                        {
                            throw new NotImplementedException($"{currentMapping.GetType().Name} add scalar: {this.scalar} :: {e.Value}");
                        }
                        Debug($"({currentMapping.GetType().Name}) Add mapping: {this.scalar} :: {e.Value}");
                        this.scalar = null;
                        this.scalarIsKey = true;
                    }
                    break;
                }
            case Mode.Sequence:
                {
                    if (!this.currentSequence.Add(e.Value))
                    {
                        throw new NotImplementedException($"{currentSequence.GetType().Name} add scalar: {e.Value}");
                    }
                    Debug($"({currentSequence.GetType().Name}) Add item: {e.Value}");
                    this.scalar = null;
                    break;
                }
            default: throw new InvalidOperationException("Unexpected mode");
        }
    }
    public void Visit(SequenceStart e)
    {
        switch (this.currentMode)
        {
            case Mode.Mapping:
                {
                    if (this.scalarIsKey) throw new InvalidOperationException($"Double mapping? {this.scalar}");
                    if (this.currentMapping.StartSequence(this.scalar) is not { } next)
                    {
                        throw new NotImplementedException($"{currentMapping.GetType().Name} sequence for {this.scalar}");
                    }
                    Push(next);
                    break;
                }
            case Mode.Sequence:
                {
                    if (this.currentSequence.StartSequence() is not { } next)
                    {
                        throw new NotImplementedException($"{currentSequence.GetType().Name} sequence");
                    }
                    Push(next);
                    break;
                }
            default:
                {
                    if (this.doc.StartSequence() is not { } next)
                    {
                        throw new NotImplementedException($"{doc.GetType().Name} sequence");
                    }
                    Push(next);
                    break;
                }
        }
        Debug($"Seq Start - {this.currentSequence.GetType().Name}", e);
    }
    public void Visit(SequenceEnd e)
    {
        Debug($"Seq End", e);
        Pop();
    }
    public void Visit(MappingStart e)
    {
        switch (this.currentMode)
        {
            case Mode.Mapping:
                {
                    if (this.scalarIsKey) throw new InvalidOperationException($"Double mapping? {this.scalar}");
                    if (this.currentMapping.StartMapping(this.scalar) is not { } next)
                    {
                        throw new NotImplementedException($"{currentMapping.GetType().Name} mapping for {this.scalar}");
                    }
                    Push(next);
                    break;
                }
            case Mode.Sequence:
                {
                    if (this.currentSequence.StartMapping() is not { } next)
                    {
                        throw new NotImplementedException($"{currentSequence.GetType().Name} mapping");
                    }
                    Push(next);
                    break;
                }
            default:
                {
                    if (this.doc.StartMapping() is not { } next)
                    {
                        throw new NotImplementedException($"{doc.GetType().Name} mapping");
                    }
                    Push(next);
                    break;
                }
        }
        Debug($"Map Start - {this.currentMapping.GetType().Name}", e);
        this.scalar = null;
        this.scalarIsKey = true;
    }
    public void Visit(MappingEnd e)
    {
        Debug($"Map End", e);
        Pop();
    }
    public void Visit(Comment e) { }

    // Subparsers
    private interface ISubparser
    {
        void Visit(Scalar e);
        void Visit(SequenceStart e);
        void Visit(SequenceEnd e);
    }
}

internal abstract class Sequence<TParentModel, TModel> : ISequence
{
    protected TParentModel Parent { get; private init; }

    protected TModel Model { get; private init; }

    public Sequence(TParentModel parent, TModel model) { this.Parent = parent; this.Model = model; }

    protected virtual IMapping? StartMapping() => null;

    IMapping? ISequence.StartMapping() => this.StartMapping();

    protected virtual ISequence? StartSequence() => null;

    ISequence? ISequence.StartSequence() => this.StartSequence();

    protected virtual Boolean Add(String value) => false;

    Boolean ISequence.Add(String value) => this.Add(value);

    void ISequence.Pop() => this.Pop(this.Parent, this.Model);

    protected abstract void Pop(TParentModel parentModel, TModel model);
}

internal abstract class Mapping<TParentModel, TModel> : IMapping
{
    protected TParentModel Parent { get; private init; }

    protected TModel Model { get; init; }

    public Mapping(TParentModel parent, TModel model) { this.Parent = parent; this.Model = model; }

    protected virtual IMapping? StartMapping(String key) => null;

    IMapping? IMapping.StartMapping(String key) => this.StartMapping(key);

    protected virtual ISequence? StartSequence(String key) => null;

    ISequence? IMapping.StartSequence(String key) => this.StartSequence(key);

    protected virtual Boolean Add(String key, String value) => false;

    Boolean IMapping.Add(String key, String value) => this.Add(key, value);

    void IMapping.Pop() => this.Pop(this.Parent, this.Model);

    protected abstract void Pop(TParentModel parentModel, TModel model);
}
