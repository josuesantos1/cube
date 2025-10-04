defmodule Parser.Data do
  def encoding(data, key, type) do
    if byte_size(key) > 512 do
      raise ArgumentError, "Key length exceeds maximum of 512 bytes"
    end

    key_hexadecimal = Base.encode16(key, case: :upper)

    key_length = key_hexadecimal
      |> byte_size()
      |> Integer.to_string(16)
      |> String.pad_leading(3, "0")

    %Parser.Lttlv{
      tag_lenght: key_length,
      tag: key_hexadecimal,
      type: type,
      length: byte_size(data),
      value: data
    }
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
end
