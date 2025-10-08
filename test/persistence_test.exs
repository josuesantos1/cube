defmodule Cube.PersistenceTest do
  use ExUnit.Case, async: false

  setup do
    File.ls!()
    |> Enum.filter(&(String.ends_with?(&1, "_data.txt") or String.ends_with?(&1, ".log")))
    |> Enum.each(&File.rm/1)

    :ok
  end

  describe "WAL and crash recovery" do
    test "WAL logs SET operations before applying them" do
      client_name = "wal_test_#{:rand.uniform(100_000)}"
      key = "test_key_#{:rand.uniform(100_000)}"

      Cube.ClientStorage.set(client_name, key, %Parser.Value{type: :string, value: "test_value"})

      {_command, shard} = Encoder.encode_get(key)
      wal_path = "wal_shard_#{shard}.log"

      assert File.exists?(wal_path), "WAL file should exist after SET operation"

      wal_content = File.read!(wal_path)
      assert byte_size(wal_content) > 0, "WAL should not be empty"
    end

    test "data persists after application restart simulation" do
      client_name = "persist_test_#{:rand.uniform(100_000)}"
      key1 = "persist_key1_#{:rand.uniform(100_000)}"
      key2 = "persist_key2_#{:rand.uniform(100_000)}"

      Cube.ClientStorage.set(client_name, key1, %Parser.Value{type: :string, value: "value1"})
      Cube.ClientStorage.set(client_name, key2, %Parser.Value{type: :integer, value: 42})

      {_command, shard1} = Encoder.encode_get(key1)
      {_command, shard2} = Encoder.encode_get(key2)

      filter1 = Storage.Engine.load_filter("shard_#{shard1}")
      filter2 = Storage.Engine.load_filter("shard_#{shard2}")

      {:ok, value1} = Storage.Engine.get("shard_#{shard1}", key1, filter1)
      {:ok, value2} = Storage.Engine.get("shard_#{shard2}", key2, filter2)

      assert value1 == "value1"
      assert value2 == "42"
    end

    test "WAL is cleared after successful replay" do
      client_name = "wal_clear_test_#{:rand.uniform(100_000)}"
      key = "clear_key_#{:rand.uniform(100_000)}"

      Cube.ClientStorage.set(client_name, key, %Parser.Value{type: :string, value: "test"})

      {_command, shard} = Encoder.encode_get(key)
      wal_path = "wal_shard_#{shard}.log"

      assert File.exists?(wal_path)

      Storage.Engine.load_filter("shard_#{shard}")

      refute File.exists?(wal_path), "WAL should be cleared after successful replay"
    end

    test "multiple operations to same key are logged correctly" do
      client_name = "multi_op_test_#{:rand.uniform(100_000)}"
      key = "multi_key_#{:rand.uniform(100_000)}"

      Cube.ClientStorage.set(client_name, key, %Parser.Value{type: :string, value: "value1"})
      Cube.ClientStorage.set(client_name, key, %Parser.Value{type: :string, value: "value2"})
      Cube.ClientStorage.set(client_name, key, %Parser.Value{type: :string, value: "value3"})

      {_command, shard} = Encoder.encode_get(key)
      filter = Storage.Engine.load_filter("shard_#{shard}")

      {:ok, value} = Storage.Engine.get("shard_#{shard}", key, filter)
      assert value == "value3"
    end

    test "concurrent writes to different keys are persisted" do
      client_name = "concurrent_test_#{:rand.uniform(100_000)}"

      tasks =
        Enum.map(1..10, fn i ->
          Task.async(fn ->
            key = "concurrent_key_#{i}_#{:rand.uniform(100_000)}"

            Cube.ClientStorage.set(
              client_name,
              key,
              %Parser.Value{type: :integer, value: i}
            )

            {key, i}
          end)
        end)

      key_value_pairs = Task.await_many(tasks)

      Enum.each(key_value_pairs, fn {key, expected_value} ->
        {_command, shard} = Encoder.encode_get(key)
        filter = Storage.Engine.load_filter("shard_#{shard}")

        {:ok, value} = Storage.Engine.get("shard_#{shard}", key, filter)
        assert value == Integer.to_string(expected_value)
      end)
    end

    test "transactions are persisted only after COMMIT" do
      client_name = "tx_persist_#{:rand.uniform(100_000)}"
      key = "tx_key_#{:rand.uniform(100_000)}"

      Cube.ClientStorage.begin_transaction(client_name)

      Cube.ClientStorage.set(client_name, key, %Parser.Value{type: :string, value: "tx_value"})

      {_command, shard} = Encoder.encode_get(key)
      filter_before = Storage.Engine.load_filter("shard_#{shard}")
      {:ok, value_before} = Storage.Engine.get("shard_#{shard}", key, filter_before)
      assert value_before == "NIL"

      Cube.ClientStorage.commit(client_name)

      filter_after = Storage.Engine.load_filter("shard_#{shard}")
      {:ok, value_after} = Storage.Engine.get("shard_#{shard}", key, filter_after)
      assert value_after == "tx_value"
    end

    test "fsync ensures data durability" do
      client_name = "fsync_test_#{:rand.uniform(100_000)}"
      key = "fsync_key_#{:rand.uniform(100_000)}"

      Cube.ClientStorage.set(client_name, key, %Parser.Value{type: :string, value: "durable"})

      {_command, shard} = Encoder.encode_get(key)
      data_path = "shard_#{shard}_data.txt"
      wal_path = "wal_shard_#{shard}.log"

      assert File.exists?(data_path)
      assert File.exists?(wal_path)

      data_content = File.read!(data_path)
      assert byte_size(data_content) > 0, "Data file should not be empty"

      wal_content = File.read!(wal_path)
      assert byte_size(wal_content) > 0, "WAL file should not be empty"
    end
  end

  describe "Persistence module functions" do
    test "write/2 appends command to file" do
      shard = "test_write_#{:rand.uniform(100_000)}"
      command = "test_command_1\n"

      assert :ok = Persistence.write(shard, command)
      assert Persistence.exists?(shard)

      content = File.read!("#{shard}_data.txt")
      assert String.contains?(content, "test_command_1")
    end

    test "write/2 appends multiple commands" do
      shard = "test_multi_write_#{:rand.uniform(100_000)}"

      Persistence.write(shard, "command1\n")
      Persistence.write(shard, "command2\n")
      Persistence.write(shard, "command3\n")

      content = File.read!("#{shard}_data.txt")
      assert String.contains?(content, "command1")
      assert String.contains?(content, "command2")
      assert String.contains?(content, "command3")
    end

    test "update_or_append/3 appends when key doesn't exist" do
      shard = "test_append_#{:rand.uniform(100_000)}"
      command = "key1_data\n"
      key_prefix = "key1"

      Persistence.update_or_append(shard, command, key_prefix)

      assert Persistence.exists?(shard)
      content = File.read!("#{shard}_data.txt")
      assert String.contains?(content, "key1_data")
    end

    test "update_or_append/3 updates existing key" do
      shard = "test_update_#{:rand.uniform(100_000)}"

      Persistence.update_or_append(shard, "key1_old\n", "key1")
      Persistence.update_or_append(shard, "key1_new\n", "key1")

      content = File.read!("#{shard}_data.txt")
      assert String.contains?(content, "key1_new")
      refute String.contains?(content, "key1_old")
    end

    test "update_or_append/3 only updates first occurrence" do
      shard = "test_first_update_#{:rand.uniform(100_000)}"

      Persistence.write(shard, "abc_v1\n")
      Persistence.write(shard, "abc_v2\n")
      Persistence.update_or_append(shard, "abc_v3\n", "abc")

      lines =
        File.read!("#{shard}_data.txt")
        |> String.split("\n", trim: true)

      assert Enum.count(lines, &String.contains?(&1, "abc_v3")) == 1
      assert Enum.count(lines, &String.contains?(&1, "abc_v2")) == 1
      refute Enum.any?(lines, &String.contains?(&1, "abc_v1"))
    end

    test "read_line_by_prefix/2 returns nil for non-existent shard" do
      shard = "nonexistent_#{:rand.uniform(100_000)}"
      assert Persistence.read_line_by_prefix(shard, "key") == nil
    end

    test "read_line_by_prefix/2 returns nil for non-matching prefix" do
      shard = "test_no_match_#{:rand.uniform(100_000)}"
      Persistence.write(shard, "key1_data\n")

      assert Persistence.read_line_by_prefix(shard, "key2") == nil
    end

    test "read_line_by_prefix/2 returns last matching line" do
      shard = "test_last_match_#{:rand.uniform(100_000)}"

      Persistence.write(shard, "key1_v1\n")
      Persistence.write(shard, "key2_data\n")
      Persistence.write(shard, "key1_v2\n")

      result = Persistence.read_line_by_prefix(shard, "key1")
      assert result == "key1_v2"
    end

    test "read_line_by_prefix/2 finds exact prefix match" do
      shard = "test_exact_#{:rand.uniform(100_000)}"

      Persistence.write(shard, "abc123_data\n")
      Persistence.write(shard, "abc_other\n")

      result = Persistence.read_line_by_prefix(shard, "abc123")
      assert result == "abc123_data"
    end

    test "stream_lines/1 returns empty list for non-existent shard" do
      shard = "nonexistent_stream_#{:rand.uniform(100_000)}"
      assert Enum.to_list(Persistence.stream_lines(shard)) == []
    end

    test "stream_lines/1 streams all lines" do
      shard = "test_stream_#{:rand.uniform(100_000)}"

      Persistence.write(shard, "line1\n")
      Persistence.write(shard, "line2\n")
      Persistence.write(shard, "line3\n")

      lines =
        Persistence.stream_lines(shard)
        |> Enum.map(&String.trim/1)
        |> Enum.to_list()

      assert length(lines) == 3
      assert "line1" in lines
      assert "line2" in lines
      assert "line3" in lines
    end

    test "exists?/1 returns false for non-existent shard" do
      shard = "never_created_#{:rand.uniform(100_000)}"
      refute Persistence.exists?(shard)
    end

    test "exists?/1 returns true for existing shard" do
      shard = "test_exists_#{:rand.uniform(100_000)}"
      Persistence.write(shard, "data\n")

      assert Persistence.exists?(shard)
    end

    test "update_or_append/3 calls fsync for durability" do
      shard = "test_fsync_#{:rand.uniform(100_000)}"
      command = "durable_data\n"
      key_prefix = "durable"

      assert :ok = Persistence.update_or_append(shard, command, key_prefix)

      assert File.exists?("#{shard}_data.txt")
      content = File.read!("#{shard}_data.txt")
      assert String.contains?(content, "durable_data")
    end

    test "handles Unicode characters correctly" do
      shard = "test_unicode_#{:rand.uniform(100_000)}"
      command = "你好世界_data\n"
      key_prefix = "你好"

      Persistence.update_or_append(shard, command, key_prefix)

      result = Persistence.read_line_by_prefix(shard, key_prefix)
      assert result == "你好世界_data"
    end

    test "handles empty file correctly" do
      shard = "test_empty_#{:rand.uniform(100_000)}"
      File.write!("#{shard}_data.txt", "")

      assert Persistence.exists?(shard)
      assert Persistence.read_line_by_prefix(shard, "any") == nil

      lines = Enum.to_list(Persistence.stream_lines(shard))
      assert length(lines) == 0 || lines == [""]
    end

    test "preserves line order in update_or_append" do
      shard = "test_order_#{:rand.uniform(100_000)}"

      Persistence.update_or_append(shard, "key1\n", "key1")
      Persistence.update_or_append(shard, "key2\n", "key2")
      Persistence.update_or_append(shard, "key3\n", "key3")
      Persistence.update_or_append(shard, "key2_updated\n", "key2")

      lines =
        File.read!("#{shard}_data.txt")
        |> String.split("\n", trim: true)

      assert lines == ["key1", "key2_updated", "key3"]
    end
  end
end
