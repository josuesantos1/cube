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

    %Parser.Query{
      command: key_length <> key_hexadecimal <> to_string(type) <> length_value <> to_string(value) <> "\n",
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

    length = String.slice(data_string, 4 + tag_length, 4)
     |> String.to_integer(16)

    value = String.slice(data_string, 8 + tag_length, length)

    %Parser.Value{
      key: Base.decode16(tag, case: :upper) |> elem(1),
      value: value,
      type: type
    }
  end

  defp get_shard(id) do
    :erlang.phash2(id, 20)
    |> rem(20)
    |> Integer.to_string()
    |> String.pad_leading(2, "0")
  end
end
