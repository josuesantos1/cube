defmodule Parser.Parser do
  def parse(data) do
    data
    |> String.trim()
    |> parse_command()
  end

  defp parse_command("SET " <> rest) do
    if length(String.split(rest)) == 1 do
      IO.inspect(length(String.split(rest)), label: "rest")
      {:error, "SET &lt;key&gt; &lt;value&gt; - Syntax error"}
    else


    case parse_key_value(rest) do

      {:ok, key, value} ->
        if value.type == nil do
          {:error, "Cannot SET key to NIL"}
        else
          {:ok, %{command: :set, key: key, value: value}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
  end

  defp parse_command("GET " <> rest) do
    case parse_key(String.trim(rest)) do
      {:ok, key, remaining} ->
        if String.trim(remaining) == "" do
          {:ok, %{command: :get, key: key}}
        else
          {:error, "extra data after key in GET command"}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_command("BEGIN" <> rest) do
    case String.trim(rest) do
      "" -> {:ok, %{command: :begin}}
      _ -> {:error, "BEGIN command does not accept arguments"}
    end
  end

  defp parse_command("COMMIT" <> rest) do
    case String.trim(rest) do
      "" -> {:ok, %{command: :commit}}
      _ -> {:error, "COMMIT command does not accept arguments"}
    end
  end

  defp parse_command("ROLLBACK" <> rest) do
    case String.trim(rest) do
      "" -> {:ok, %{command: :rollback}}
      _ -> {:error, "ROLLBACK command does not accept arguments"}
    end
  end

  defp parse_command("") do
    {:error, "No command - empty input"}
  end

  defp parse_command(data) do
    {:error, "No command #{String.split(data) |> List.first()}"}
  end



  defp parse_key_value(input) do
    input = String.trim_leading(input)

    case parse_key(input) do
      {:ok, key, rest} ->
        rest = String.trim_leading(rest)

        case parse_value(rest) do
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
    input = String.trim_leading(input)

    cond do
      String.starts_with?(input, "NIL") -> {:error, "NIL is not valid as key"}

      String.starts_with?(input, "\"") ->
        case parse_string(String.slice(input, 1..-1//1), "") do
          {:ok, %Parser.Value{value: key_value}, rest} ->
            {:ok, key_value, rest}

          {:error, reason} ->
            {:error, reason}
        end

      true ->
        case Regex.run(~r/^([A-Za-z_][A-Za-z0-9_]*)(.*)$/, input) do
          [_, key, rest] -> {:ok, key, String.trim_leading(rest)}
          nil -> {:error, "Value #{String.split(input) |> List.first()} is not valid as key"}
        end
    end
  end

  defp parse_value(input) do
    input = String.trim_leading(input)

    cond do
      String.starts_with?(input, "\"") ->
        parse_string(String.slice(input, 1..-1//1), "")

      String.starts_with?(input, "TRUE") ->
        {:ok, %Parser.Value{type: :boolean, value: true}, String.slice(input, 4..-1//1)}

      String.starts_with?(input, "FALSE") ->
        {:ok, %Parser.Value{type: :boolean, value: false}, String.slice(input, 5..-1//1)}

      String.starts_with?(input, "NIL") ->
        {:ok, %Parser.Value{type: nil, value: nil}, String.slice(input, 3..-1//1)}

      true ->
        parse_integer(input)
    end
  end

  defp parse_integer(input) do
    case Regex.run(~r/^(-?\d+)(.*)$/, input) do
      [_, number, rest] ->
        {:ok, %Parser.Value{type: :integer, value: String.to_integer(number)}, rest}

      nil ->
        {:error, "invalid value - expected integer, string, boolean or NIL"}
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
