defmodule Parser.Data do
  def encoding(%{command: :set, key: key, value: %Parser.Value{type: type, value: value}}) do
    if byte_size(key) > 512 do
      raise ArgumentError, "Key length exceeds maximum of 512 bytes"
    end

    key_hexadecimal = Base.encode16(key, case: :upper)

    key_length = key_hexadecimal
      |> byte_size()
      |> Integer.to_string(16)
      |> String.pad_leading(3, "0")

    length_value = value |> to_string() |> byte_size() |> Integer.to_string(16) |> String.pad_leading(8, "0")

    IO.puts("Encoding data: key_length=#{key_length}, key_hex=#{key_hexadecimal}, type=#{type}, length_value=#{length_value}, value=#{value}")

    type = case type do
      :string -> "0"
      :integer -> "1"
      :float -> "2"
      :boolean -> "3"
      :nil -> "4"
      _ -> raise ArgumentError, "Unsupported type: #{type}"
    end

    %Parser.Query{
      command: key_length <> key_hexadecimal <> type <> length_value <> to_string(value) <> "\n",
      type: :set,
      shard: get_shard(key_hexadecimal)
    }
  end

  def encoding(%{command: :get, key: key}) do
    if byte_size(key) > 512 do
      raise ArgumentError, "Key length exceeds maximum of 512 bytes"
    end

    key_hexadecimal = Base.encode16(key, case: :upper)

    key_length = key_hexadecimal
      |> byte_size()
      |> Integer.to_string(16)
      |> String.pad_leading(3, "0")

    %Parser.Query{command: key_length <> key_hexadecimal, type: :get, shard: get_shard(key_hexadecimal)}
  end

  def decoding_string(data_string) do
    tag_length = String.slice(data_string, 0, 3) |> String.to_integer(16)
    tag = String.slice(data_string, 3, tag_length)
    type = String.slice(data_string, 3 + tag_length, 1)

    length = String.slice(data_string, 4 + tag_length, 11)
     |> String.to_integer(16)

    value = String.slice(data_string, 12 + tag_length, length)

    IO.puts("Decoding data: tag_length=#{tag_length}, tag=#{tag}, type=#{type}, length=#{length}, value=#{value}")

    value
  end

  defp get_shard(id) do
    :erlang.phash2(id, 20)
    |> rem(20)
    |> Integer.to_string()
    |> String.pad_leading(2, "0")
  end
end
