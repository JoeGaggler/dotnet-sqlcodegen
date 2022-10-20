using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pingmint.CodeGen.Sql;

public class CodeWriter
{
    private Int32 currentIndentation = 0;

    private readonly StringBuilder stringBuilder = new();

    public override string ToString() => this.stringBuilder.ToString();

    public void Indent() => currentIndentation++;

    public void Dedent() => currentIndentation--;

    public void Text(String text) => this.stringBuilder.Append(text);

    public void Text(String format, params String[] args) => this.stringBuilder.AppendFormat(format, args);

    public void StartLine() => Text(new String('\t', currentIndentation));

    public void Line() => this.stringBuilder.AppendLine();

    public void Line(String text)
    {
        StartLine();
        this.stringBuilder.AppendLine(text);
    }

    public void Line(String format, params String[] args)
    {
        StartLine();
        this.stringBuilder.AppendLine(String.Format(format, args));
    }

    public void UsingNamespace(String namespaceIdentifier) => Line("using {0};", namespaceIdentifier);


    public void FileNamespace(String namespaceIdentifer) => Line($"namespace {namespaceIdentifer};");

    public IDisposable Namespace(String namespaceIdentifier) => new NamespaceScope(this, namespaceIdentifier);

    private sealed class NamespaceScope : IDisposable
    {
        private readonly CodeWriter writer;

        public NamespaceScope(CodeWriter writer, String namespaceIdentifer)
        {
            this.writer = writer;

            this.writer.Line("namespace {0}", namespaceIdentifer);
            this.writer.Line("{");
            this.writer.Indent();

        }

        public void Dispose()
        {
            this.writer.Dedent();
            this.writer.Line("}");
        }
    }
}

public static class CodeWriterExtensions
{
    public static void Comment(this CodeWriter writer, String comment) => writer.Line("// {0}", comment);

    public static IDisposable CreateBraceScope(this CodeWriter writer, String? preamble = null, String? withClosingBrace = null) => new BraceScope(writer, preamble, withClosingBrace);

    public sealed class BraceScope : IDisposable
    {
        private readonly CodeWriter writer;
        private readonly String? withClosingBrace;

        public BraceScope(CodeWriter codeGenerator, String? preamble = null, String? withClosingBrace = null)
        {
            this.writer = codeGenerator;
            this.withClosingBrace = withClosingBrace;

            if (preamble != null)
            {
                this.writer.Line(preamble + "{");
            }
            else
            {
                this.writer.Line("{");
            }
            this.writer.Indent();
        }

        public void Dispose()
        {
            this.writer.Dedent();
            if (this.withClosingBrace == null)
            {
                this.writer.Line("}");
            }
            else
            {
                this.writer.Line("}}{0}", this.withClosingBrace);
            }
        }
    }
    public static IDisposable Class(this CodeWriter writer, String modifiers, String name)
    {
        writer.Line("{0} class {1}", modifiers, name);
        return new BraceScope(writer);
    }

    public static IDisposable Class(this CodeWriter writer, String modifiers, String name, String implements)
    {
        writer.Line("{0} class {1} : {2}", modifiers, name, implements);
        return new BraceScope(writer);
    }

    public static IDisposable PartialClass(this CodeWriter writer, String modifiers, String name)
    {
        writer.Line("{0} partial class {1}", modifiers, name);
        return new BraceScope(writer);
    }

    public static IDisposable PartialClass(this CodeWriter writer, String modifiers, String name, String implements)
    {
        writer.Line("{0} partial class {1} : {2}", modifiers, name, implements);
        return new BraceScope(writer);
    }

    public static IDisposable Using(this CodeWriter writer, String disposable)
    {
        writer.Line("using ({0})", disposable);
        return new BraceScope(writer);
    }

    public static IDisposable Method(this CodeWriter writer, String modifiers, String returnType, String name, String args)
    {
        writer.Line("{0} {1} {2}({3})", modifiers, returnType, name, args);
        return new BraceScope(writer);
    }

    public static IDisposable While(this CodeWriter writer, String whileCondition) => new BraceScope(writer, String.Format("while ({0})", whileCondition));

    public static IDisposable DoWhile(this CodeWriter writer, String whileCondition)
    {
        writer.Line("do");
        return new BraceScope(writer, preamble: null, withClosingBrace: String.Format(" while ({0});", whileCondition));
    }

    public static IDisposable Switch(this CodeWriter writer, String switchCondition) => new BraceScope(writer, String.Format("switch ({0})", switchCondition));

    public static IDisposable SwitchCase(this CodeWriter writer, string caseString) => new BraceScope(writer, String.Format("case {0}:", caseString));

    public static IDisposable SwitchCase(this CodeWriter writer, string caseStringFormat, params String[] caseStringArgs) => new BraceScope(writer, String.Format("case {0}:", String.Format(caseStringFormat, caseStringArgs)));

    public static IDisposable SwitchDefault(this CodeWriter writer) => new BraceScope(writer, "default:");

    public static IDisposable Constructor(this CodeWriter writer, String access, String typeName, String parameters = "") => new BraceScope(writer, String.Format("{0} {1}({2})", access, typeName, parameters));

    public static IDisposable ForEach(this CodeWriter writer, String enumerable)
    {
        writer.Line("foreach ({0})", enumerable);
        return new BraceScope(writer);
    }

    public static IDisposable If(this CodeWriter writer, String condition)
    {
        writer.Line("if ({0})", condition);
        return new BraceScope(writer);
    }

    public static IDisposable If(this CodeWriter writer, String conditionFormat, params String[] conditionArgs)
    {
        writer.Line("if ({0})", String.Format(conditionFormat, conditionArgs));
        return new BraceScope(writer);
    }

    public static IDisposable ElseIf(this CodeWriter writer, String condition) => new BraceScope(writer, String.Format("else if ({0})", condition));

    public static IDisposable ElseIf(this CodeWriter writer, String conditionFormat, params String[] conditionArgs) => new BraceScope(writer, String.Format("else if ({0})", String.Format(conditionFormat, conditionArgs)));

    public static IDisposable Else(this CodeWriter writer) => new BraceScope(writer, "else");

    public static void Return(this CodeWriter writer, string returnValue) => writer.Line("return {0};", returnValue);
}
