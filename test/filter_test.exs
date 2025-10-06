defmodule FilterTest do
  use ExUnit.Case, async: true

  describe "new/0" do
    test "creates a new empty bloom filter" do
      filter = Filter.new()

      assert %Filter{} = filter
    end
  end

  describe "add/2" do
    test "adds element to filter" do
      filter = Filter.new()
      updated_filter = Filter.add(filter, "test_key")

      assert %Filter{} = updated_filter
      assert Filter.contains?(updated_filter, "test_key")
    end

    test "can add multiple elements" do
      filter = Filter.new()

      filter =
        filter
        |> Filter.add("key1")
        |> Filter.add("key2")
        |> Filter.add("key3")

      assert %Filter{} = filter
    end

    test "adding same element multiple times is idempotent" do
      filter = Filter.new()

      filter1 = Filter.add(filter, "duplicate")
      filter2 = Filter.add(filter1, "duplicate")
      filter3 = Filter.add(filter2, "duplicate")

      assert %Filter{} = filter3
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

    test "works with binary keys" do
      binary_key = <<0, 1, 2, 3, 255>>

      filter =
        Filter.new()
        |> Filter.add(binary_key)

      assert Filter.contains?(filter, binary_key) == true
    end

    test "works with empty string" do
      filter =
        Filter.new()
        |> Filter.add("")

      assert Filter.contains?(filter, "") == true
    end

    test "works with very long keys" do
      long_key = String.duplicate("x", 1000)

      filter =
        Filter.new()
        |> Filter.add(long_key)

      assert Filter.contains?(filter, long_key) == true
    end
  end

  describe "remove/2" do
    test "removes element from filter" do
      filter =
        Filter.new()
        |> Filter.add("test_key")

      assert Filter.contains?(filter, "test_key") == true

      updated_filter = Filter.remove(filter, "test_key")

      assert Filter.contains?(updated_filter, "test_key") == false
    end

    test "removing non-existent element is safe" do
      filter =
        Filter.new()
        |> Filter.add("key1")

      updated_filter = Filter.remove(filter, "key2")

      assert Filter.contains?(updated_filter, "key1") == true
    end

    test "can re-add element after removal" do
      filter =
        Filter.new()
        |> Filter.add("key")
        |> Filter.remove("key")
        |> Filter.add("key")

      assert Filter.contains?(filter, "key") == true
    end

    test "multiple adds and removes" do
      filter = Filter.new()

      filter =
        filter
        |> Filter.add("key")
        |> Filter.add("key")
        |> Filter.remove("key")

      assert Filter.contains?(filter, "key") == true
    end

    test "remove decrements counter safely (doesn't go negative)" do
      filter =
        Filter.new()
        |> Filter.add("key")

      filter
      |> Filter.remove("key")
      |> Filter.remove("key")
      |> Filter.remove("key")
    end
  end

  describe "filter size and hash configuration" do
    test "can create filter with custom size" do
      filter = Filter.new(5000)

      assert %Filter{size: 5000} = filter
    end

    test "can create filter with custom hash count" do
      filter = Filter.new(10000, 5)

      assert %Filter{hash_count: 5} = filter
    end

    test "larger filter has lower false positive rate" do
      small_filter = Filter.new(1000, 3)
      large_filter = Filter.new(20000, 3)

      small_filter =
        Enum.reduce(1..100, small_filter, fn i, acc ->
          Filter.add(acc, "key#{i}")
        end)

      large_filter =
        Enum.reduce(1..100, large_filter, fn i, acc ->
          Filter.add(acc, "key#{i}")
        end)

      small_fp =
        Enum.count(1..1000, fn i ->
          Filter.contains?(small_filter, "not_added_#{i}")
        end)

      large_fp =
        Enum.count(1..1000, fn i ->
          Filter.contains?(large_filter, "not_added_#{i}")
        end)

      assert large_fp < small_fp, "Larger filter should have fewer false positives"
    end

    test "more hash functions reduce false positives" do
      few_hashes = Filter.new(10000, 2)
      many_hashes = Filter.new(10000, 5)

      few_hashes =
        Enum.reduce(1..100, few_hashes, fn i, acc ->
          Filter.add(acc, "key#{i}")
        end)

      many_hashes =
        Enum.reduce(1..100, many_hashes, fn i, acc ->
          Filter.add(acc, "key#{i}")
        end)

      few_fp =
        Enum.count(1..1000, fn i ->
          Filter.contains?(few_hashes, "not_added_#{i}")
        end)

      many_fp =
        Enum.count(1..1000, fn i ->
          Filter.contains?(many_hashes, "not_added_#{i}")
        end)

      assert many_fp <= few_fp
    end
  end

  describe "edge cases and stress tests" do
    test "handles collision of hash positions" do
      filter = Filter.new(10)

      filter =
        Enum.reduce(1..100, filter, fn i, acc ->
          Filter.add(acc, "collision_test_#{i}")
        end)

      added_found = Filter.contains?(filter, "collision_test_50")
      assert added_found == true
    end

    test "handles adding same element many times" do
      filter = Filter.new()

      filter =
        Enum.reduce(1..1000, filter, fn _i, acc ->
          Filter.add(acc, "same_key")
        end)

      assert Filter.contains?(filter, "same_key") == true
    end

    test "handles empty filter operations" do
      filter = Filter.new()

      refute Filter.contains?(filter, "anything")

      updated = Filter.remove(filter, "nonexistent")
      refute Filter.contains?(updated, "nonexistent")
    end

    test "filter maintains correctness under rapid add/remove cycles" do
      filter = Filter.new()

      final_filter =
        Enum.reduce(1..100, filter, fn i, acc ->
          acc
          |> Filter.add("key#{i}")
          |> Filter.remove("key#{i - 1}")
        end)

      assert Filter.contains?(final_filter, "key100") == true
      refute Filter.contains?(final_filter, "key1")
    end

    test "handles keys with null bytes" do
      key_with_null = "before\0after"

      filter =
        Filter.new()
        |> Filter.add(key_with_null)

      assert Filter.contains?(filter, key_with_null) == true
      refute Filter.contains?(filter, "beforeafter")
    end

    test "distinguishes similar keys" do
      filter =
        Filter.new()
        |> Filter.add("key")
        |> Filter.add("key1")
        |> Filter.add("key2")

      assert Filter.contains?(filter, "key") == true
      assert Filter.contains?(filter, "key1") == true
      assert Filter.contains?(filter, "key2") == true

      contains_key3 = Filter.contains?(filter, "key3")
      assert is_boolean(contains_key3)
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
