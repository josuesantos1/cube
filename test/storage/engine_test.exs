defmodule Storage.EngineTest do
  use ExUnit.Case, async: false

  setup do
    on_exit(fn ->
      File.rm_rf!("test_shard_*.txt")
    end)

    :ok
  end

  describe "get/3" do
    test "returns NIL for non-existent key" do
      filter = Filter.new()
      result = Storage.Engine.get("test_shard_get_nil", "nonexistent", filter)

      assert {:ok, "NIL"} = result
    end

    test "returns value for existing key" do
      shard_id = "test_shard_get_existing"
      filter = Filter.new()

      value = %Parser.Value{type: :string, value: "test_value"}
      {:ok, _old, _new, updated_filter} = Storage.Engine.set(shard_id, "mykey", value, filter)

      result = Storage.Engine.get(shard_id, "mykey", updated_filter)
      assert {:ok, "test_value"} = result
    end

    test "bloom filter prevents disk read for non-existent key" do
      shard_id = "test_shard_bloom_optimization"
      filter = Filter.new()

      value = %Parser.Value{type: :string, value: "existing"}
      {:ok, _old, _new, updated_filter} = Storage.Engine.set(shard_id, "key1", value, filter)

      result = Storage.Engine.get(shard_id, "key2", updated_filter)
      assert {:ok, "NIL"} = result
    end

    test "handles Unicode keys" do
      shard_id = "test_shard_unicode_key"
      filter = Filter.new()

      value = %Parser.Value{type: :string, value: "你好"}
      {:ok, _old, _new, updated_filter} = Storage.Engine.set(shard_id, "中文", value, filter)

      result = Storage.Engine.get(shard_id, "中文", updated_filter)
      assert {:ok, "你好"} = result
    end

    test "retrieves latest value when key is updated" do
      shard_id = "test_shard_latest_value"
      filter = Filter.new()

      v1 = %Parser.Value{type: :string, value: "first"}
      {:ok, _, _, filter2} = Storage.Engine.set(shard_id, "key", v1, filter)

      v2 = %Parser.Value{type: :string, value: "second"}
      {:ok, _, _, filter3} = Storage.Engine.set(shard_id, "key", v2, filter2)

      result = Storage.Engine.get(shard_id, "key", filter3)
      assert {:ok, "second"} = result
    end
  end

  describe "set/4" do
    test "sets new key and returns NIL as old value" do
      shard_id = "test_shard_set_new"
      filter = Filter.new()

      value = %Parser.Value{type: :string, value: "new_value"}
      result = Storage.Engine.set(shard_id, "newkey", value, filter)

      assert {:ok, "NIL", "new_value", updated_filter} = result
      assert %Filter{} = updated_filter
    end

    test "updates existing key and returns old value" do
      shard_id = "test_shard_set_update"
      filter = Filter.new()

      v1 = %Parser.Value{type: :string, value: "original"}
      {:ok, _, _, filter2} = Storage.Engine.set(shard_id, "key", v1, filter)

      v2 = %Parser.Value{type: :string, value: "updated"}
      result = Storage.Engine.set(shard_id, "key", v2, filter2)

      assert {:ok, "original", "updated", _filter3} = result
    end

    test "updates bloom filter with new key" do
      shard_id = "test_shard_bloom_update"
      filter = Filter.new()

      value = %Parser.Value{type: :string, value: "test"}
      {command, _} = Encoder.encode_set("mykey", value)
      key_prefix = Encoder.extract_key_prefix(command)

      refute Filter.contains?(filter, key_prefix)

      {:ok, _, _, updated_filter} = Storage.Engine.set(shard_id, "mykey", value, filter)

      assert Filter.contains?(updated_filter, key_prefix)
    end

    test "persists data to file" do
      shard_id = "test_shard_persist"
      filter = Filter.new()

      value = %Parser.Value{type: :string, value: "persisted"}
      Storage.Engine.set(shard_id, "key", value, filter)

      assert File.exists?("#{shard_id}_data.txt")
    end

    test "handles different value types" do
      shard_id = "test_shard_types"
      filter = Filter.new()

      test_cases = [
        {%Parser.Value{type: :string, value: "string"}, "string"},
        {%Parser.Value{type: :integer, value: 42}, "42"},
        {%Parser.Value{type: :boolean, value: true}, "TRUE"},
        {%Parser.Value{type: :boolean, value: false}, "FALSE"},
        {%Parser.Value{type: nil, value: nil}, "NIL"}
      ]

      Enum.reduce(test_cases, filter, fn {val, expected}, acc_filter ->
        key = "key_#{expected}"
        {:ok, _old, new_val, new_filter} = Storage.Engine.set(shard_id, key, val, acc_filter)
        assert new_val == expected
        new_filter
      end)
    end

    test "handles empty string value" do
      shard_id = "test_shard_empty_string"
      filter = Filter.new()

      value = %Parser.Value{type: :string, value: ""}
      {:ok, _, new_val, updated_filter} = Storage.Engine.set(shard_id, "empty", value, filter)

      assert new_val == ""

      result = Storage.Engine.get(shard_id, "empty", updated_filter)
      assert {:ok, ""} = result
    end

    test "handles special characters in values" do
      shard_id = "test_shard_special_chars"
      filter = Filter.new()

      special_values = [
        "Line1\nLine2",
        "Tab\there",
        ~s(Quote"here),
        "Back\\slash",
        "Emoji 🎉"
      ]

      Enum.reduce(special_values, filter, fn val, acc_filter ->
        value = %Parser.Value{type: :string, value: val}
        key = "key_#{:erlang.phash2(val)}"
        {:ok, _, _, new_filter} = Storage.Engine.set(shard_id, key, value, acc_filter)

        result = Storage.Engine.get(shard_id, key, new_filter)
        assert {:ok, ^val} = result

        new_filter
      end)
    end
  end

  describe "encode_value/1" do
    test "encodes string value" do
      value = %Parser.Value{type: :string, value: "test string"}
      assert Storage.Engine.encode_value(value) == "test string"
    end

    test "encodes integer value" do
      value = %Parser.Value{type: :integer, value: 42}
      assert Storage.Engine.encode_value(value) == "42"
    end

    test "encodes negative integer" do
      value = %Parser.Value{type: :integer, value: -100}
      assert Storage.Engine.encode_value(value) == "-100"
    end

    test "encodes zero" do
      value = %Parser.Value{type: :integer, value: 0}
      assert Storage.Engine.encode_value(value) == "0"
    end

    test "encodes boolean TRUE" do
      value = %Parser.Value{type: :boolean, value: true}
      assert Storage.Engine.encode_value(value) == "TRUE"
    end

    test "encodes boolean FALSE" do
      value = %Parser.Value{type: :boolean, value: false}
      assert Storage.Engine.encode_value(value) == "FALSE"
    end

    test "encodes nil value" do
      value = %Parser.Value{type: nil, value: nil}
      assert Storage.Engine.encode_value(value) == "NIL"
    end

    test "encodes empty string" do
      value = %Parser.Value{type: :string, value: ""}
      assert Storage.Engine.encode_value(value) == ""
    end

    test "encodes Unicode" do
      value = %Parser.Value{type: :string, value: "你好世界"}
      assert Storage.Engine.encode_value(value) == "你好世界"
    end
  end

  describe "load_filter/1" do
    test "loads empty filter for non-existent shard" do
      filter = Storage.Engine.load_filter("nonexistent_shard")

      assert %Filter{} = filter
      refute Filter.contains?(filter, "any_key")
    end

    test "loads filter with existing keys" do
      shard_id = "test_shard_load_filter"
      filter = Filter.new()

      v1 = %Parser.Value{type: :string, value: "value1"}
      v2 = %Parser.Value{type: :string, value: "value2"}
      v3 = %Parser.Value{type: :string, value: "value3"}

      {:ok, _, _, f1} = Storage.Engine.set(shard_id, "key1", v1, filter)
      {:ok, _, _, f2} = Storage.Engine.set(shard_id, "key2", v2, f1)
      {:ok, _, _, _f3} = Storage.Engine.set(shard_id, "key3", v3, f2)

      loaded_filter = Storage.Engine.load_filter(shard_id)

      {cmd1, _} = Encoder.encode_get("key1")
      {cmd2, _} = Encoder.encode_get("key2")
      {cmd3, _} = Encoder.encode_get("key3")
      {cmd4, _} = Encoder.encode_get("key4")

      assert Filter.contains?(loaded_filter, Encoder.extract_key_prefix(cmd1))
      assert Filter.contains?(loaded_filter, Encoder.extract_key_prefix(cmd2))
      assert Filter.contains?(loaded_filter, Encoder.extract_key_prefix(cmd3))
      refute Filter.contains?(loaded_filter, Encoder.extract_key_prefix(cmd4))
    end

    test "loaded filter works for get operations" do
      shard_id = "test_shard_loaded_filter"
      filter = Filter.new()

      value = %Parser.Value{type: :string, value: "recoverable"}
      {:ok, _, _, _} = Storage.Engine.set(shard_id, "persist", value, filter)

      loaded_filter = Storage.Engine.load_filter(shard_id)
      result = Storage.Engine.get(shard_id, "persist", loaded_filter)

      assert {:ok, "recoverable"} = result
    end

    test "handles shard with updated keys" do
      shard_id = "test_shard_updated_keys"
      filter = Filter.new()

      v1 = %Parser.Value{type: :string, value: "first"}
      {:ok, _, _, f1} = Storage.Engine.set(shard_id, "key", v1, filter)

      v2 = %Parser.Value{type: :string, value: "second"}
      {:ok, _, _, _f2} = Storage.Engine.set(shard_id, "key", v2, f1)

      loaded_filter = Storage.Engine.load_filter(shard_id)
      result = Storage.Engine.get(shard_id, "key", loaded_filter)

      assert {:ok, "second"} = result
    end
  end

  describe "integration - get/set cycle" do
    test "complete cycle: set, get, update, get" do
      shard_id = "test_shard_cycle"
      filter = Filter.new()

      v1 = %Parser.Value{type: :integer, value: 100}
      {:ok, "NIL", "100", f1} = Storage.Engine.set(shard_id, "counter", v1, filter)

      {:ok, "100"} = Storage.Engine.get(shard_id, "counter", f1)

      v2 = %Parser.Value{type: :integer, value: 200}
      {:ok, "100", "200", f2} = Storage.Engine.set(shard_id, "counter", v2, f1)

      {:ok, "200"} = Storage.Engine.get(shard_id, "counter", f2)
    end

    test "multiple keys in same shard" do
      shard_id = "test_shard_multiple_keys"
      filter = Filter.new()

      v1 = %Parser.Value{type: :string, value: "Alice"}
      v2 = %Parser.Value{type: :integer, value: 30}
      v3 = %Parser.Value{type: :boolean, value: true}

      {:ok, _, _, f1} = Storage.Engine.set(shard_id, "name", v1, filter)
      {:ok, _, _, f2} = Storage.Engine.set(shard_id, "age", v2, f1)
      {:ok, _, _, f3} = Storage.Engine.set(shard_id, "active", v3, f2)

      assert {:ok, "Alice"} = Storage.Engine.get(shard_id, "name", f3)
      assert {:ok, "30"} = Storage.Engine.get(shard_id, "age", f3)
      assert {:ok, "TRUE"} = Storage.Engine.get(shard_id, "active", f3)
    end

    test "persistence survives filter reload" do
      shard_id = "test_shard_persistence"
      filter = Filter.new()

      data = [
        {"user1", %Parser.Value{type: :string, value: "Alice"}},
        {"user2", %Parser.Value{type: :string, value: "Bob"}},
        {"count", %Parser.Value{type: :integer, value: 42}}
      ]

      _final_filter =
        Enum.reduce(data, filter, fn {key, val}, acc ->
          {:ok, _, _, new_filter} = Storage.Engine.set(shard_id, key, val, acc)
          new_filter
        end)

      loaded_filter = Storage.Engine.load_filter(shard_id)

      assert {:ok, "Alice"} = Storage.Engine.get(shard_id, "user1", loaded_filter)
      assert {:ok, "Bob"} = Storage.Engine.get(shard_id, "user2", loaded_filter)
      assert {:ok, "42"} = Storage.Engine.get(shard_id, "count", loaded_filter)
    end
  end

  describe "edge cases" do
    test "handles very long key" do
      shard_id = "test_shard_long_key"
      filter = Filter.new()

      long_key = String.duplicate("x", 500)
      value = %Parser.Value{type: :string, value: "long_key_value"}

      {:ok, _, _, updated_filter} = Storage.Engine.set(shard_id, long_key, value, filter)
      result = Storage.Engine.get(shard_id, long_key, updated_filter)

      assert {:ok, "long_key_value"} = result
    end

    test "handles very long value" do
      shard_id = "test_shard_long_value"
      filter = Filter.new()

      long_value = String.duplicate("y", 5000)
      value = %Parser.Value{type: :string, value: long_value}

      {:ok, _, _, updated_filter} = Storage.Engine.set(shard_id, "key", value, filter)
      result = Storage.Engine.get(shard_id, "key", updated_filter)

      assert {:ok, ^long_value} = result
    end

    test "handles rapid updates to same key" do
      shard_id = "test_shard_rapid_updates"
      filter = Filter.new()

      final_filter =
        Enum.reduce(1..100, filter, fn i, acc ->
          value = %Parser.Value{type: :integer, value: i}
          {:ok, _, _, new_filter} = Storage.Engine.set(shard_id, "counter", value, acc)
          new_filter
        end)

      result = Storage.Engine.get(shard_id, "counter", final_filter)
      assert {:ok, "100"} = result
    end

    test "handles many keys in single shard" do
      shard_id = "test_shard_many_keys"
      filter = Filter.new()

      final_filter =
        Enum.reduce(1..100, filter, fn i, acc ->
          value = %Parser.Value{type: :integer, value: i}
          {:ok, _, _, new_filter} = Storage.Engine.set(shard_id, "key#{i}", value, acc)
          new_filter
        end)

      result1 = Storage.Engine.get(shard_id, "key1", final_filter)
      result50 = Storage.Engine.get(shard_id, "key50", final_filter)
      result100 = Storage.Engine.get(shard_id, "key100", final_filter)

      assert {:ok, "1"} = result1
      assert {:ok, "50"} = result50
      assert {:ok, "100"} = result100
    end

    test "handles key with only numbers (as string)" do
      shard_id = "test_shard_numeric_key"
      filter = Filter.new()

      value = %Parser.Value{type: :string, value: "numeric"}
      {:ok, _, _, updated_filter} = Storage.Engine.set(shard_id, "12345", value, filter)

      result = Storage.Engine.get(shard_id, "12345", updated_filter)
      assert {:ok, "numeric"} = result
    end

    test "handles binary data in values" do
      shard_id = "test_shard_binary"
      filter = Filter.new()

      binary_data = <<0, 1, 2, 3, 255>>
      value = %Parser.Value{type: :string, value: binary_data}

      {:ok, _, _, updated_filter} = Storage.Engine.set(shard_id, "binary", value, filter)
      result = Storage.Engine.get(shard_id, "binary", updated_filter)

      assert {:ok, ^binary_data} = result
    end
  end
end
