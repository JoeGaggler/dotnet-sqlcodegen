using System.Data;

namespace Pingmint.CodeGen.Sql;

public static class Globals
{
    public static String GetCamelCase(String originalName)
    {
        var sb = new System.Text.StringBuilder(originalName.Length);

        bool firstChar = true;
        bool firstWord = true;
        foreach (var ch in originalName)
        {
            if (firstChar)
            {
                if (firstWord)
                {
                    if (!Char.IsLetter(ch)) { continue; }
                    sb.Append(Char.ToLowerInvariant(ch));
                    firstWord = false;
                }
                else
                {
                    sb.Append(Char.ToUpperInvariant(ch));
                }
                firstChar = false;
            }
            else if (Char.IsLetterOrDigit(ch))
            {
                sb.Append(ch);
            }
            else
            {
                firstChar = true;
            }
        }

        return sb.ToString();
    }

    public static String GetPascalCase(String originalName)
    {
        var sb = new System.Text.StringBuilder(originalName.Length);

        bool firstChar = true;
        foreach (var ch in originalName)
        {
            if (firstChar)
            {
                if (!Char.IsLetter(ch))
                {
                    if (sb.Length == 0)
                    {
                        continue;
                    }
                    else
                    {
                        sb.Append(Char.ToUpperInvariant(ch));
                        continue;
                    }
                }

                sb.Append(Char.ToUpperInvariant(ch));
                firstChar = false;
            }
            else if (Char.IsLetterOrDigit(ch))
            {
                sb.Append(ch);
            }
            else
            {
                firstChar = true;
            }
        }

        return sb.ToString();
    }

    public static String GetStringForType(Type type, Boolean isColumnNullable) => (isColumnNullable) ?
        $"{GetShortestNameForType(type)}?" :
        $"{GetShortestNameForType(type)}";

    public static String GetShortestNameForType(Type type) => type switch
    {
        var x when x == typeof(DateTime) => "DateTime",
        var x when x == typeof(DateTimeOffset) => "DateTimeOffset",
        var x when x == typeof(TimeSpan) => "TimeSpan",
        var x when x == typeof(Int16) => "Int16",
        var x when x == typeof(Int32) => "Int32",
        var x when x == typeof(Int64) => "Int64",
        var x when x == typeof(Single) => "Single",
        var x when x == typeof(Double) => "Double",
        var x when x == typeof(Decimal) => "Decimal",
        var x when x == typeof(String) => "String",
        var x when x == typeof(Boolean) => "Boolean",
        var x when x == typeof(Byte) => "Byte",
        var x when x == typeof(Guid) => "Guid",
        var x when x == typeof(Byte[]) => "Byte[]",
        _ => throw new ArgumentException($"GetShortestNameForType({type.FullName}) not defined."),
    };
}
