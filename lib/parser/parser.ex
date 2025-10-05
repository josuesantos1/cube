defmodule Parser.Parser do
  def parse(data) do
    data
    |> String.trim()
    |> parse_command()
  end

  defp parse_command("SET " <> rest) do
    case parse_key_value(rest) do
      {:ok, key, value} -> {:ok, %{command: :set, key: key, value: value}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_command("GET " <> rest) do
    case parse_key(rest) do
      {:ok, key, ""} -> {:ok, %{command: :get, key: key}}
      {:ok, _key, _rest} -> {:error, "extra data after key in GET command"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_command(_) do
    {:error, "unknown command"}
  end

  defp parse_key_value(input) do
    input = String.trim_leading(input)

    case parse_key(input) do
      {:ok, key, rest} ->
        rest = String.trim_leading(rest)

        case parse_value(rest) do
          {:ok, %Parser.Value{type: :nil}, _remaining} ->
            {:error, "Cannot SET key to NIL"}

          {:ok, value, _remaining} ->
            {:ok, key, value}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_key(input) do
    case Regex.run(~r/^([A-Za-z_][A-Za-z0-9_]*)(.*)$/, input) do
      [_, key, rest] -> {:ok, key, String.trim_leading(rest)}
      nil -> {:error, "invalid key - must be simple string (e.g. ABC, my_key)"}
    end
  end

  defp parse_value(input) do
    input = String.trim_leading(input)

    cond do
      String.starts_with?(input, "\"") ->
        parse_string(String.slice(input, 1..-1//1), "")

      String.starts_with?(input, "true") ->
        {:ok, %Parser.Value{type: :boolean, value: true}, String.slice(input, 4..-1//1)}

      String.starts_with?(input, "false") ->
        {:ok, %Parser.Value{type: :boolean, value: false}, String.slice(input, 5..-1//1)}

      String.starts_with?(input, "nil") ->
        {:ok, %Parser.Value{type: :nil, value: nil}, String.slice(input, 3..-1//1)}

      true ->
        parse_integer(input)
    end
  end

  defp parse_integer(input) do
    case Regex.run(~r/^(-?\d+)(.*)$/, input) do
      [_, number, rest] ->
        {:ok, %Parser.Value{type: :integer, value: String.to_integer(number)}, rest}

      nil ->
        {:error, "invalid value - expected integer, string, boolean or nil"}
    end
  end

  defp parse_string("", _) do
    {:error, "unclosed string - missing closing quote"}
  end

  defp parse_string("\\\"" <> rest, acc) do
    parse_string(rest, acc <> "\"")
  end

  defp parse_string("\\\\" <> rest, acc) do
    parse_string(rest, acc <> "\\")
  end

  defp parse_string("\\n" <> rest, acc) do
    parse_string(rest, acc <> "\n")
  end

  defp parse_string("\\t" <> rest, acc) do
    parse_string(rest, acc <> "\t")
  end

  defp parse_string("\"" <> rest, acc) do
    {:ok, %Parser.Value{type: :string, value: acc}, rest}
  end

  defp parse_string(<<char::utf8, rest::binary>>, acc) do
    parse_string(rest, acc <> <<char::utf8>>)
  end
end
