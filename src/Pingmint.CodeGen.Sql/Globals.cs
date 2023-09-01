using System.Data;

namespace Pingmint.CodeGen.Sql;

public interface IDmDescribeFirstResultSetRow
{
    Int32 SchemaId { get; init; }
    Byte SystemTypeId { get; init; }
    Int32 UserTypeId { get; init; }
    String? Name { get; init; }
    Boolean? IsNullable { get; init; }
    Int32? ColumnOrdinal { get; init; }
    String SqlTypeName { get; init; }
}
public partial record class DmDescribeFirstResultSetRow : IDmDescribeFirstResultSetRow { }
public partial record class DmDescribeFirstResultSetForObjectRow : IDmDescribeFirstResultSetRow { }

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

    // TODO: this is not deterministic, so it is unstable across regenerations (use the original SQL namespace instead?)
    public static String GetUniqueName(String baseName, HashSet<String> hashSet)
    {
        String recordName = baseName;
        for (int i = 1; !hashSet.Add(recordName); i++) // Fairly safe to assume that we would never see more duplicate types than ints
        {
            recordName = GetPascalCase(baseName + i.ToString());
        }
        return recordName;
    }

    public static (String?, String) ParseSchemaItem(String text)
    {
        if (text.IndexOf('.') is int i and > 0)
        {
            var schema = text[..i];
            var item = text[(i + 1)..];
            return (schema, item);
        }
        else
        {
            return (null, text); // schema-less
        }
    }
}
