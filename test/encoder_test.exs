defmodule EncoderTest do
  use ExUnit.Case, async: true

  describe "encode_set/2" do
    test "encodes string value" do
      value = %Parser.Value{type: :string, value: "test"}
      {command, shard} = Encoder.encode_set("key", value)

      assert is_binary(command)
      assert String.ends_with?(command, "\n")

      assert String.contains?(command, Base.encode16("key", case: :upper))

      assert String.match?(shard, ~r/^\d{2}$/)
    end

    test "encodes integer value" do
      value = %Parser.Value{type: :integer, value: 42}
      {command, _shard} = Encoder.encode_set("age", value)

      assert String.contains?(command, Base.encode16("42"))
      assert String.contains?(command, "1")
    end

    test "encodes boolean value" do
      value = %Parser.Value{type: :boolean, value: true}
      {command, _shard} = Encoder.encode_set("active", value)

      assert String.contains?(command, Base.encode16("true"))
      assert String.contains?(command, "3")
    end

    test "encodes Unicode characters" do
      value = %Parser.Value{type: :string, value: "你好"}
      {command, _shard} = Encoder.encode_set("greeting", value)

      assert is_binary(command)
      assert String.ends_with?(command, "\n")
    end

    test "raises error for key longer than 512 bytes" do
      long_key = String.duplicate("a", 513)
      value = %Parser.Value{type: :string, value: "test"}

      assert_raise ArgumentError, ~r/Key length exceeds maximum/, fn ->
        Encoder.encode_set(long_key, value)
      end
    end

    test "different keys produce different shards" do
      value = %Parser.Value{type: :string, value: "test"}

      {_, shard1} = Encoder.encode_set("key1", value)
      {_, shard2} = Encoder.encode_set("key2", value)
      {_, shard3} = Encoder.encode_set("key3", value)

      shards = [shard1, shard2, shard3]
      assert length(Enum.uniq(shards)) >= 1
    end

    test "same key always produces same shard" do
      value = %Parser.Value{type: :string, value: "test"}

      {_, shard1} = Encoder.encode_set("consistent_key", value)
      {_, shard2} = Encoder.encode_set("consistent_key", value)
      {_, shard3} = Encoder.encode_set("consistent_key", value)

      assert shard1 == shard2
      assert shard2 == shard3
    end

    test "distributes keys across 20 shards" do
      value = %Parser.Value{type: :string, value: "test"}

      shards =
        Enum.map(1..100, fn i ->
          {_, shard} = Encoder.encode_set("key#{i}", value)
          shard
        end)

      unique_shards = Enum.uniq(shards)

      assert length(unique_shards) > 5

      assert Enum.all?(unique_shards, fn shard ->
               shard_num = String.to_integer(shard)
               shard_num >= 0 and shard_num < 20
             end)
    end
  end

  describe "encode_get/1" do
    test "encodes key for GET operation" do
      {command, shard} = Encoder.encode_get("testkey")

      assert String.contains?(command, Base.encode16("testkey", case: :upper))

      assert String.match?(shard, ~r/^\d{2}$/)
    end

    test "GET and SET use same shard for same key" do
      {_, get_shard} = Encoder.encode_get("mykey")

      value = %Parser.Value{type: :string, value: "test"}
      {_, set_shard} = Encoder.encode_set("mykey", value)

      assert get_shard == set_shard
    end

    test "raises error for key longer than 512 bytes" do
      long_key = String.duplicate("x", 513)

      assert_raise ArgumentError, ~r/Key length exceeds maximum/, fn ->
        Encoder.encode_get(long_key)
      end
    end
  end

  describe "decode/1" do
    test "decodes encoded string value" do
      value = %Parser.Value{type: :string, value: "Hello World"}
      {command, _} = Encoder.encode_set("test", value)

      command_trimmed = String.trim(command)

      decoded = Encoder.decode(command_trimmed)

      assert decoded == "Hello World"
    end

    test "decodes encoded integer value" do
      value = %Parser.Value{type: :integer, value: 12345}
      {command, _} = Encoder.encode_set("number", value)

      decoded = Encoder.decode(String.trim(command))

      assert decoded == "12345"
    end

    test "decodes encoded boolean value" do
      value = %Parser.Value{type: :boolean, value: false}
      {command, _} = Encoder.encode_set("flag", value)

      decoded = Encoder.decode(String.trim(command))

      assert decoded == "false"
    end

    test "decodes Unicode characters" do
      value = %Parser.Value{type: :string, value: "Olá Mundo 你好"}
      {command, _} = Encoder.encode_set("multilang", value)

      decoded = Encoder.decode(String.trim(command))

      assert decoded == "Olá Mundo 你好"
    end

    test "decode is inverse of encode" do
      original_value = "Test Value with Special Chars: !@#$%"
      value = %Parser.Value{type: :string, value: original_value}

      {command, _} = Encoder.encode_set("key", value)
      decoded = Encoder.decode(String.trim(command))

      assert decoded == original_value
    end
  end

  describe "extract_key_prefix/1" do
    test "extracts key prefix from encoded command" do
      value = %Parser.Value{type: :string, value: "value"}
      {command, _} = Encoder.encode_set("mykey", value)

      prefix = Encoder.extract_key_prefix(command)

      assert is_binary(prefix)
      assert String.length(prefix) > 3 # tamanho (3) + pelo menos 1 byte de chave
    end

    test "same key produces same prefix" do
      value1 = %Parser.Value{type: :string, value: "value1"}
      value2 = %Parser.Value{type: :integer, value: 999}

      {command1, _} = Encoder.encode_set("samekey", value1)
      {command2, _} = Encoder.encode_set("samekey", value2)

      prefix1 = Encoder.extract_key_prefix(command1)
      prefix2 = Encoder.extract_key_prefix(command2)

      assert prefix1 == prefix2
    end

    test "different keys produce different prefixes" do
      value = %Parser.Value{type: :string, value: "test"}

      {command1, _} = Encoder.encode_set("key1", value)
      {command2, _} = Encoder.encode_set("key2", value)

      prefix1 = Encoder.extract_key_prefix(command1)
      prefix2 = Encoder.extract_key_prefix(command2)

      assert prefix1 != prefix2
    end

    test "works with GET commands" do
      {get_command, _} = Encoder.encode_get("testkey")

      prefix = Encoder.extract_key_prefix(get_command)

      assert is_binary(prefix)
      assert String.length(prefix) > 0
    end
  end

  describe "LTTLV format consistency" do
    test "encoded command has consistent structure" do
      value = %Parser.Value{type: :string, value: "test"}
      {command, _} = Encoder.encode_set("key", value)

      cmd = String.trim(command)

      key_length_hex = String.slice(cmd, 0, 3)
      key_length = String.to_integer(key_length_hex, 16)

      assert key_length > 0

      key_hex = String.slice(cmd, 3, key_length)
      assert String.length(key_hex) == key_length

      type_pos = 3 + key_length
      type = String.slice(cmd, type_pos, 1)
      assert type in ["0", "1", "2", "3", "4"]
    end

    test "encoded command length is deterministic" do
      value = %Parser.Value{type: :string, value: "fixed"}

      {command1, _} = Encoder.encode_set("key", value)
      {command2, _} = Encoder.encode_set("key", value)

      assert byte_size(command1) == byte_size(command2)
    end
  end

  describe "edge cases - special values" do
    test "encodes empty string" do
      value = %Parser.Value{type: :string, value: ""}
      {command, _} = Encoder.encode_set("empty", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == ""
    end

    test "encodes nil value" do
      value = %Parser.Value{type: :nil, value: nil}
      {command, _} = Encoder.encode_set("null_key", value)
      assert String.contains?(command, "4")
    end

    test "encodes zero integer" do
      value = %Parser.Value{type: :integer, value: 0}
      {command, _} = Encoder.encode_set("zero", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == "0"
    end

    test "encodes negative integer" do
      value = %Parser.Value{type: :integer, value: -42}
      {command, _} = Encoder.encode_set("negative", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == "-42"
    end

    test "encodes very large integer" do
      value = %Parser.Value{type: :integer, value: 999_999_999_999}
      {command, _} = Encoder.encode_set("big", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == "999999999999"
    end
  end

  describe "edge cases - special keys" do
    test "encodes single character key" do
      value = %Parser.Value{type: :string, value: "x"}
      {command, _} = Encoder.encode_set("a", value)
      prefix = Encoder.extract_key_prefix(command)
      assert String.length(prefix) > 0
    end

    test "encodes maximum length key (512 bytes)" do
      max_key = String.duplicate("x", 512)
      value = %Parser.Value{type: :string, value: "test"}
      {command, _} = Encoder.encode_set(max_key, value)
      assert is_binary(command)
    end

    test "encodes key with numbers" do
      value = %Parser.Value{type: :string, value: "test"}
      {command, _} = Encoder.encode_set("key123", value)
      assert String.contains?(command, Base.encode16("key123", case: :upper))
    end

    test "encodes key with underscores" do
      value = %Parser.Value{type: :string, value: "test"}
      {command, _} = Encoder.encode_set("my_key_name", value)
      assert String.contains?(command, Base.encode16("my_key_name", case: :upper))
    end
  end

  describe "special characters in values" do
    test "encodes newline in value" do
      value = %Parser.Value{type: :string, value: "Line1\nLine2"}
      {command, _} = Encoder.encode_set("multiline", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == "Line1\nLine2"
    end

    test "encodes tab in value" do
      value = %Parser.Value{type: :string, value: "Col1\tCol2"}
      {command, _} = Encoder.encode_set("tabbed", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == "Col1\tCol2"
    end

    test "encodes quotes in value" do
      value = %Parser.Value{type: :string, value: ~s(He said "Hello")}
      {command, _} = Encoder.encode_set("quoted", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == ~s(He said "Hello")
    end

    test "encodes backslash in value" do
      value = %Parser.Value{type: :string, value: "path\\to\\file"}
      {command, _} = Encoder.encode_set("path", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == "path\\to\\file"
    end

    test "encodes emoji in value" do
      value = %Parser.Value{type: :string, value: "Hello 👋 World 🌍"}
      {command, _} = Encoder.encode_set("emoji", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == "Hello 👋 World 🌍"
    end

    test "encodes null byte in value" do
      value = %Parser.Value{type: :string, value: "before\0after"}
      {command, _} = Encoder.encode_set("nullbyte", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == "before\0after"
    end
  end

  describe "consistency and invariants" do
    test "encode-decode roundtrip preserves all types" do
      test_cases = [
        {%Parser.Value{type: :string, value: "test string"}, "test string"},
        {%Parser.Value{type: :integer, value: 42}, "42"},
        {%Parser.Value{type: :boolean, value: true}, "true"},
        {%Parser.Value{type: :boolean, value: false}, "false"}
      ]

      Enum.each(test_cases, fn {val, expected} ->
        {command, _} = Encoder.encode_set("key", val)
        decoded = Encoder.decode(String.trim(command))
        assert decoded == expected
      end)
    end

    test "different values with same key produce same prefix" do
      v1 = %Parser.Value{type: :string, value: "value1"}
      v2 = %Parser.Value{type: :string, value: "value2"}

      {cmd1, _} = Encoder.encode_set("same_key", v1)
      {cmd2, _} = Encoder.encode_set("same_key", v2)

      prefix1 = Encoder.extract_key_prefix(cmd1)
      prefix2 = Encoder.extract_key_prefix(cmd2)

      assert prefix1 == prefix2
    end

    test "command always ends with newline" do
      value = %Parser.Value{type: :string, value: "test"}
      {command, _} = Encoder.encode_set("key", value)
      assert String.ends_with?(command, "\n")
    end

    test "type field is single character" do
      test_types = [
        {%Parser.Value{type: :string, value: "x"}, "0"},
        {%Parser.Value{type: :integer, value: 1}, "1"},
        {%Parser.Value{type: :boolean, value: true}, "3"},
        {%Parser.Value{type: :nil, value: nil}, "4"}
      ]

      Enum.each(test_types, fn {val, expected_type} ->
        {command, _} = Encoder.encode_set("key", val)
        cmd = String.trim(command)
        key_length = String.slice(cmd, 0, 3) |> String.to_integer(16)
        type_pos = 3 + key_length
        type = String.slice(cmd, type_pos, 1)
        assert type == expected_type
      end)
    end
  end

  describe "performance and limits" do
    test "handles very long string values" do
      long_value = String.duplicate("x", 10_000)
      value = %Parser.Value{type: :string, value: long_value}
      {command, _} = Encoder.encode_set("long", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == long_value
    end

    test "value length field can represent large values" do
      value = %Parser.Value{type: :string, value: "test"}
      {command, _} = Encoder.encode_set("key", value)
      cmd = String.trim(command)
      key_length = String.slice(cmd, 0, 3) |> String.to_integer(16)
      length_field = String.slice(cmd, 3 + key_length + 1, 8)
      assert String.length(length_field) == 8
    end

    test "shard distribution is relatively uniform" do
      value = %Parser.Value{type: :string, value: "test"}

      shard_counts =
        1..1000
        |> Enum.map(fn i ->
          {_, shard} = Encoder.encode_set("key#{i}", value)
          String.to_integer(shard)
        end)
        |> Enum.frequencies()

      Enum.each(shard_counts, fn {_shard, count} ->
        assert count >= 30 and count <= 70,
               "Shard distribution unbalanced: #{count} keys"
      end)
    end
  end

  describe "security and validation" do
    test "rejects key with 513 bytes" do
      long_key = String.duplicate("a", 513)
      value = %Parser.Value{type: :string, value: "test"}

      assert_raise ArgumentError, fn ->
        Encoder.encode_set(long_key, value)
      end
    end

    test "handles binary data in values" do
      binary_value = <<0, 1, 2, 3, 255>>
      value = %Parser.Value{type: :string, value: binary_value}
      {command, _} = Encoder.encode_set("binary", value)
      decoded = Encoder.decode(String.trim(command))
      assert decoded == binary_value
    end

    test "decode handles malformed hex gracefully" do
      malformed = "00661626300000000ZINVALID"

      assert_raise ArgumentError, fn ->
        Encoder.decode(malformed)
      end
    end
  end
end
