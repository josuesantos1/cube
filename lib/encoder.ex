defmodule Encoder do
  def encode_set(key, %Parser.Value{type: type, value: value}) do
    if byte_size(key) > 512 do
      raise ArgumentError, "Key length exceeds maximum of 512 bytes"
    end

    key_hexadecimal = Base.encode16(key, case: :upper)

    key_length =
      key_hexadecimal
      |> byte_size()
      |> Integer.to_string(16)
      |> String.pad_leading(3, "0")

      value = value
    |> to_string()
    |> Base.encode16()

    length_value = value |> byte_size() |> Integer.to_string(16) |> String.pad_leading(8, "0")

    type_encoded =
      case type do
        :string -> "0"
        :integer -> "1"
        :float -> "2"
        :boolean -> "3"
        :nil -> "4"
        _ -> raise ArgumentError, "Unsupported type: #{type}"
      end

    shard = get_shard(key_hexadecimal)
    command = key_length <> key_hexadecimal <> type_encoded <> length_value <> value <> "\n"

    {command, shard}
  end

  def encode_get(key) do
    if byte_size(key) > 512 do
      raise ArgumentError, "Key length exceeds maximum of 512 bytes"
    end

    key_hexadecimal = Base.encode16(key, case: :upper)

    key_length =
      key_hexadecimal
      |> byte_size()
      |> Integer.to_string(16)
      |> String.pad_leading(3, "0")

    shard = get_shard(key_hexadecimal)
    command = key_length <> key_hexadecimal

    {command, shard}
  end

  def decode(data_string) do
    tag_length = String.slice(data_string, 0, 3) |> String.to_integer(16)
    _tag = String.slice(data_string, 3, tag_length)
    _type = String.slice(data_string, 3 + tag_length, 1)

    length =
      String.slice(data_string, 4 + tag_length, 8)
      |> String.to_integer(16)

    value_hex = String.slice(data_string, 12 + tag_length, length)

    case Base.decode16(value_hex, case: :mixed) do
      {:ok, decoded} -> decoded
      :error -> value_hex
    end
  end

  def extract_key_prefix(command) do
    key_length = String.slice(command, 0, 3) |> String.to_integer(16)
    String.slice(command, 0, 3 + key_length)
  end

  defp get_shard(id) do
    :erlang.phash2(id, 20)
    |> rem(20)
    |> Integer.to_string()
    |> String.pad_leading(2, "0")
  end
end
