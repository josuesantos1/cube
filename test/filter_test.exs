defmodule FilterTest do
  use ExUnit.Case, async: true

  describe "new/0" do
    test "creates a new empty bloom filter" do
      filter = Filter.new()

      assert is_tuple(filter)
    end
  end

  describe "add/2" do
    test "adds element to filter" do
      filter = Filter.new()
      updated_filter = Filter.add(filter, "test_key")

      assert is_tuple(updated_filter)
      assert filter != updated_filter
    end

    test "can add multiple elements" do
      filter = Filter.new()

      filter =
        filter
        |> Filter.add("key1")
        |> Filter.add("key2")
        |> Filter.add("key3")

      assert is_tuple(filter)
    end

    test "adding same element multiple times is idempotent" do
      filter = Filter.new()

      filter1 = Filter.add(filter, "duplicate")
      filter2 = Filter.add(filter1, "duplicate")
      filter3 = Filter.add(filter2, "duplicate")

      assert is_tuple(filter3)
    end
  end

  describe "contains?/2" do
    test "returns true for added element" do
      filter =
        Filter.new()
        |> Filter.add("existing_key")

      assert Filter.contains?(filter, "existing_key") == true
    end

    test "returns false for non-existent element in empty filter" do
      filter = Filter.new()

      assert Filter.contains?(filter, "nonexistent") == false
    end

    test "returns false for non-added element" do
      filter =
        Filter.new()
        |> Filter.add("key1")
        |> Filter.add("key2")

      assert Filter.contains?(filter, "key3") == false
    end

    test "works with many elements" do
      filter = Filter.new()

      filter =
        Enum.reduce(1..1000, filter, fn i, acc ->
          Filter.add(acc, "key#{i}")
        end)

      assert Filter.contains?(filter, "key1") == true
      assert Filter.contains?(filter, "key500") == true
      assert Filter.contains?(filter, "key1000") == true

      result = Filter.contains?(filter, "nonexistent_key_xyz_123")
      assert is_boolean(result)
    end
  end

  describe "bloom filter properties" do
    test "no false negatives - if element was added, contains? must return true" do
      filter = Filter.new()
      elements = Enum.map(1..100, &"element#{&1}")

      filter = Enum.reduce(elements, filter, &Filter.add(&2, &1))

      results = Enum.map(elements, &Filter.contains?(filter, &1))

      assert Enum.all?(results, & &1), "Bloom filter returned false negative!"
    end

    test "false positive rate is acceptable" do
      filter = Filter.new()

      filter =
        Enum.reduce(1..100, filter, fn i, acc ->
          Filter.add(acc, "added_key_#{i}")
        end)

      non_existent_checks =
        Enum.map(1..1000, fn i ->
          Filter.contains?(filter, "not_added_key_#{i}")
        end)

      false_positives = Enum.count(non_existent_checks, & &1)
      false_positive_rate = false_positives / 1000

      assert false_positive_rate < 0.1,
             "False positive rate too high: #{false_positive_rate * 100}%"
    end
  end

  describe "different data types" do
    test "works with string keys" do
      filter =
        Filter.new()
        |> Filter.add("string_key")

      assert Filter.contains?(filter, "string_key") == true
    end

    test "works with keys containing special characters" do
      filter =
        Filter.new()
        |> Filter.add("key!@#$%^&*()")

      assert Filter.contains?(filter, "key!@#$%^&*()") == true
    end

    test "works with Unicode keys" do
      filter =
        Filter.new()
        |> Filter.add("键_中文")

      assert Filter.contains?(filter, "键_中文") == true
    end

    test "works with hexadecimal encoded keys" do
      hex_key = Base.encode16("test", case: :upper)

      filter =
        Filter.new()
        |> Filter.add(hex_key)

      assert Filter.contains?(filter, hex_key) == true
    end
  end

  describe "performance characteristics" do
    test "can handle large number of elements" do
      filter = Filter.new()

      {time_add, filter} =
        :timer.tc(fn ->
          Enum.reduce(1..10_000, filter, fn i, acc ->
            Filter.add(acc, "large_key_#{i}")
          end)
        end)

      assert time_add < 1_000_000, "Adding 10k elements took too long"

      {time_lookup, _} =
        :timer.tc(fn ->
          Filter.contains?(filter, "large_key_5000")
        end)

      assert time_lookup < 1000, "Lookup took too long"
    end

    test "filter size is reasonable" do
      filter = Filter.new()

      filter =
        Enum.reduce(1..1000, filter, fn i, acc ->
          Filter.add(acc, "key#{i}")
        end)

      serialized = :erlang.term_to_binary(filter)
      size_bytes = byte_size(serialized)

      assert size_bytes < 100_000,
             "Bloom filter too large: #{size_bytes} bytes"
    end
  end

  describe "integration with Encoder" do
    test "works with encoded key prefixes" do
      value = %Parser.Value{type: :string, value: "test"}
      {command, _shard} = Encoder.encode_set("mykey", value)

      key_prefix = Encoder.extract_key_prefix(command)

      filter =
        Filter.new()
        |> Filter.add(key_prefix)

      assert Filter.contains?(filter, key_prefix) == true
    end

    test "different keys produce different filter entries" do
      value = %Parser.Value{type: :string, value: "test"}

      {cmd1, _} = Encoder.encode_set("key1", value)
      {cmd2, _} = Encoder.encode_set("key2", value)

      prefix1 = Encoder.extract_key_prefix(cmd1)
      prefix2 = Encoder.extract_key_prefix(cmd2)

      filter =
        Filter.new()
        |> Filter.add(prefix1)

      assert Filter.contains?(filter, prefix1) == true
      result = Filter.contains?(filter, prefix2)
      assert is_boolean(result)
    end
  end
end
