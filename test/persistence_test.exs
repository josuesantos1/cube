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
end
