defmodule Cube.ClientStorageTest do
  use ExUnit.Case, async: false

  setup do
    File.rm_rf!("test_data")
    File.mkdir_p!("test_data")

    original_dir = File.cwd!()
    File.cd!("test_data")

    on_exit(fn ->
      File.cd!(original_dir)
      File.rm_rf!("test_data")
    end)

    :ok
  end

  describe "GET command" do
    test "returns NIL for non-existent key" do
      {:ok, pid} = Cube.ClientStorage.start_link("test_client_get_nil")

      result = Cube.ClientStorage.get(pid, "nonexistent")

      assert {:ok, "NIL"} = result
    end

    test "returns value for existing key" do
      {:ok, pid} = Cube.ClientStorage.start_link("test_client_get_existing")

      value = %Parser.Value{type: :string, value: "Alice"}
      Cube.ClientStorage.set(pid, "name", value)

      result = Cube.ClientStorage.get(pid, "name")

      assert {:ok, "Alice"} = result
    end

    test "returns integer value" do
      {:ok, pid} = Cube.ClientStorage.start_link("test_client_get_int")

      value = %Parser.Value{type: :integer, value: 42}
      Cube.ClientStorage.set(pid, "age", value)

      result = Cube.ClientStorage.get(pid, "age")

      assert {:ok, "42"} = result
    end

    test "returns boolean value" do
      {:ok, pid} = Cube.ClientStorage.start_link("test_client_get_bool")

      value = %Parser.Value{type: :boolean, value: true}
      Cube.ClientStorage.set(pid, "active", value)

      result = Cube.ClientStorage.get(pid, "active")

      assert {:ok, "true"} = result
    end
  end

  describe "SET command" do
    test "sets new key and returns NIL and new value" do
      {:ok, pid} = Cube.ClientStorage.start_link("test_client_set_new")

      value = %Parser.Value{type: :string, value: "Bob"}
      result = Cube.ClientStorage.set(pid, "user", value)

      assert {:ok, "NIL", "Bob"} = result
    end

    test "overwrites existing key and returns old and new values" do
      {:ok, pid} = Cube.ClientStorage.start_link("test_client_set_overwrite")

      value1 = %Parser.Value{type: :string, value: "Original"}
      Cube.ClientStorage.set(pid, "data", value1)

      value2 = %Parser.Value{type: :string, value: "Updated"}
      result = Cube.ClientStorage.set(pid, "data", value2)

      assert {:ok, "Original", "Updated"} = result
    end

    test "persists data to file" do
      client_name = "test_client_persist"
      {:ok, pid} = Cube.ClientStorage.start_link(client_name)

      value = %Parser.Value{type: :integer, value: 100}
      Cube.ClientStorage.set(pid, "count", value)

      shard_files = File.ls!() |> Enum.filter(&String.contains?(&1, client_name))
      assert length(shard_files) > 0
    end
  end

  describe "client isolation" do
    test "different clients don't see each other's data" do
      {:ok, pid_alice} = Cube.ClientStorage.start_link("alice")
      {:ok, pid_bob} = Cube.ClientStorage.start_link("bob")

      value = %Parser.Value{type: :string, value: "Alice's secret"}
      Cube.ClientStorage.set(pid_alice, "secret", value)

      result = Cube.ClientStorage.get(pid_bob, "secret")
      assert {:ok, "NIL"} = result

      result = Cube.ClientStorage.get(pid_alice, "secret")
      assert {:ok, "Alice's secret"} = result
    end

    test "clients can have same key with different values" do
      {:ok, pid_alice} = Cube.ClientStorage.start_link("alice2")
      {:ok, pid_bob} = Cube.ClientStorage.start_link("bob2")

      value_alice = %Parser.Value{type: :string, value: "Alice"}
      value_bob = %Parser.Value{type: :string, value: "Bob"}

      Cube.ClientStorage.set(pid_alice, "name", value_alice)
      Cube.ClientStorage.set(pid_bob, "name", value_bob)

      {:ok, alice_name} = Cube.ClientStorage.get(pid_alice, "name")
      {:ok, bob_name} = Cube.ClientStorage.get(pid_bob, "name")

      assert alice_name == "Alice"
      assert bob_name == "Bob"
    end

    test "clients write to separate shard files" do
      {:ok, pid_alice} = Cube.ClientStorage.start_link("alice_files")
      {:ok, pid_bob} = Cube.ClientStorage.start_link("bob_files")

      value = %Parser.Value{type: :string, value: "test"}
      Cube.ClientStorage.set(pid_alice, "key1", value)
      Cube.ClientStorage.set(pid_bob, "key2", value)

      files = File.ls!()
      alice_files = Enum.filter(files, &String.contains?(&1, "alice_files"))
      bob_files = Enum.filter(files, &String.contains?(&1, "bob_files"))

      assert length(alice_files) > 0
      assert length(bob_files) > 0

      assert MapSet.disjoint?(MapSet.new(alice_files), MapSet.new(bob_files))
    end
  end

  describe "persistence and recovery" do
    test "data persists after process restart" do
      client_name = "persistent_client_#{:rand.uniform(100000)}"
      {:ok, pid} = Cube.ClientSupervisor.get_or_start_client(client_name)

      value = %Parser.Value{type: :string, value: "Persisted Data"}
      Cube.ClientStorage.set(pid, "persistent_key", value)

      DynamicSupervisor.terminate_child(Cube.ClientSupervisor, pid)
      :timer.sleep(100)

      {:ok, new_pid} = Cube.ClientSupervisor.get_or_start_client(client_name)

      result = Cube.ClientStorage.get(new_pid, "persistent_key")
      assert {:ok, "Persisted Data"} = result
    end
  end

  describe "bloom filter optimization" do
    test "bloom filter correctly identifies non-existent keys" do
      {:ok, pid} = Cube.ClientStorage.start_link("bloom_test")

      Enum.each(1..10, fn i ->
        value = %Parser.Value{type: :integer, value: i}
        Cube.ClientStorage.set(pid, "key#{i}", value)
      end)

      result = Cube.ClientStorage.get(pid, "nonexistent_key_xyz")
      assert {:ok, "NIL"} = result
    end
  end

  describe "concurrent operations" do
    test "handles concurrent SETs from same client" do
      {:ok, pid} = Cube.ClientStorage.start_link("concurrent_client")

      tasks =
        Enum.map(1..20, fn i ->
          Task.async(fn ->
            value = %Parser.Value{type: :integer, value: i}
            Cube.ClientStorage.set(pid, "key#{i}", value)
          end)
        end)

      results = Task.await_many(tasks)

      assert Enum.all?(results, fn
               {:ok, _, _} -> true
               _ -> false
             end)
    end
  end

  describe "edge cases" do
    test "handles Unicode keys" do
      {:ok, pid} = Cube.ClientStorage.start_link("unicode_client")

      value = %Parser.Value{type: :string, value: "测试"}
      result = Cube.ClientStorage.set(pid, "键", value)

      assert {:ok, "NIL", "测试"} = result

      {:ok, retrieved} = Cube.ClientStorage.get(pid, "键")
      assert retrieved == "测试"
    end

    test "handles long key names" do
      {:ok, pid} = Cube.ClientStorage.start_link("long_key_client")

      long_key = String.duplicate("a", 100)
      value = %Parser.Value{type: :string, value: "value"}

      result = Cube.ClientStorage.set(pid, long_key, value)
      assert {:ok, "NIL", "value"} = result
    end

    test "handles large values" do
      {:ok, pid} = Cube.ClientStorage.start_link("large_value_client")

      large_value = String.duplicate("x", 10000)
      value = %Parser.Value{type: :string, value: large_value}

      Cube.ClientStorage.set(pid, "large", value)
      {:ok, retrieved} = Cube.ClientStorage.get(pid, "large")

      assert retrieved == large_value
    end
  end

  describe "transactions - BEGIN" do
    test "BEGIN starts a new transaction" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_begin_#{:rand.uniform(100000)}")

      result = Cube.ClientStorage.begin_transaction(pid)
      assert :ok = result
    end

    test "BEGIN fails if already in transaction" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_begin_twice_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)
      result = Cube.ClientStorage.begin_transaction(pid)

      assert {:error, "Already in transaction"} = result
    end

    test "can start new transaction after COMMIT" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_begin_after_commit_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)
      Cube.ClientStorage.commit(pid)

      result = Cube.ClientStorage.begin_transaction(pid)
      assert :ok = result
    end

    test "can start new transaction after ROLLBACK" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_begin_after_rollback_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)
      Cube.ClientStorage.rollback(pid)

      result = Cube.ClientStorage.begin_transaction(pid)
      assert :ok = result
    end
  end

  describe "transactions - COMMIT" do
    test "COMMIT fails when not in transaction" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_commit_no_tx_#{:rand.uniform(100000)}")

      result = Cube.ClientStorage.commit(pid)
      assert {:error, "No transaction in progress"} = result
    end

    test "COMMIT persists writes to storage" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_commit_persist_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)

      v1 = %Parser.Value{type: :string, value: "Alice"}
      Cube.ClientStorage.set(pid, "name", v1)

      Cube.ClientStorage.commit(pid)

      {:ok, value} = Cube.ClientStorage.get(pid, "name")
      assert value == "Alice"
    end

    test "COMMIT succeeds when no conflicts" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_commit_success_#{:rand.uniform(100000)}")

      v1 = %Parser.Value{type: :integer, value: 100}
      Cube.ClientStorage.set(pid, "balance", v1)

      Cube.ClientStorage.begin_transaction(pid)
      {:ok, _old_balance} = Cube.ClientStorage.get(pid, "balance")

      v2 = %Parser.Value{type: :integer, value: 150}
      Cube.ClientStorage.set(pid, "balance", v2)

      result = Cube.ClientStorage.commit(pid)
      assert :ok = result
    end

    test "COMMIT clears transaction state" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_clear_state_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)
      v1 = %Parser.Value{type: :string, value: "test"}
      Cube.ClientStorage.set(pid, "key", v1)
      Cube.ClientStorage.commit(pid)

      result = Cube.ClientStorage.commit(pid)
      assert {:error, "No transaction in progress"} = result
    end
  end

  describe "transactions - ROLLBACK" do
    test "ROLLBACK fails when not in transaction" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_rollback_no_tx_#{:rand.uniform(100000)}")

      result = Cube.ClientStorage.rollback(pid)
      assert {:error, "No transaction in progress"} = result
    end

    test "ROLLBACK discards uncommitted writes" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_rollback_discard_#{:rand.uniform(100000)}")

      v1 = %Parser.Value{type: :string, value: "original"}
      Cube.ClientStorage.set(pid, "data", v1)

      Cube.ClientStorage.begin_transaction(pid)
      v2 = %Parser.Value{type: :string, value: "modified"}
      Cube.ClientStorage.set(pid, "data", v2)
      Cube.ClientStorage.rollback(pid)

      {:ok, value} = Cube.ClientStorage.get(pid, "data")
      assert value == "original"
    end

    test "ROLLBACK clears transaction state" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_rollback_clear_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)
      Cube.ClientStorage.rollback(pid)

      result = Cube.ClientStorage.rollback(pid)
      assert {:error, "No transaction in progress"} = result
    end
  end

  describe "transactions - snapshot isolation" do
    test "reads inside transaction see snapshot" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_snapshot_#{:rand.uniform(100000)}")

      v1 = %Parser.Value{type: :integer, value: 100}
      Cube.ClientStorage.set(pid, "value", v1)

      Cube.ClientStorage.begin_transaction(pid)
      {:ok, snapshot_value} = Cube.ClientStorage.get(pid, "value")

      assert snapshot_value == "100"
    end

    test "writes inside transaction are visible to same transaction" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_write_visible_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)

      v1 = %Parser.Value{type: :string, value: "first"}
      Cube.ClientStorage.set(pid, "key", v1)

      {:ok, value} = Cube.ClientStorage.get(pid, "key")
      assert value == "first"
    end

    test "multiple writes to same key in transaction" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_multi_write_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)

      v1 = %Parser.Value{type: :integer, value: 1}
      Cube.ClientStorage.set(pid, "counter", v1)

      v2 = %Parser.Value{type: :integer, value: 2}
      Cube.ClientStorage.set(pid, "counter", v2)

      v3 = %Parser.Value{type: :integer, value: 3}
      Cube.ClientStorage.set(pid, "counter", v3)

      {:ok, value} = Cube.ClientStorage.get(pid, "counter")
      assert value == "3"

      Cube.ClientStorage.commit(pid)

      {:ok, committed_value} = Cube.ClientStorage.get(pid, "counter")
      assert committed_value == "3"
    end
  end

  describe "transactions - edge cases" do
    test "transaction with only reads commits successfully" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_read_only_#{:rand.uniform(100000)}")

      v1 = %Parser.Value{type: :string, value: "test"}
      Cube.ClientStorage.set(pid, "key", v1)

      Cube.ClientStorage.begin_transaction(pid)
      Cube.ClientStorage.get(pid, "key")
      Cube.ClientStorage.get(pid, "key")

      result = Cube.ClientStorage.commit(pid)
      assert :ok = result
    end

    test "transaction with only writes commits successfully" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_write_only_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)

      v1 = %Parser.Value{type: :string, value: "write1"}
      Cube.ClientStorage.set(pid, "key1", v1)

      v2 = %Parser.Value{type: :string, value: "write2"}
      Cube.ClientStorage.set(pid, "key2", v2)

      result = Cube.ClientStorage.commit(pid)
      assert :ok = result
    end

    test "empty transaction commits successfully" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_empty_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)
      result = Cube.ClientStorage.commit(pid)

      assert :ok = result
    end

    test "reading non-existent key then writing it commits successfully" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_read_nil_write_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)

      {:ok, "NIL"} = Cube.ClientStorage.get(pid, "new_key")

      v1 = %Parser.Value{type: :string, value: "created"}
      Cube.ClientStorage.set(pid, "new_key", v1)

      result = Cube.ClientStorage.commit(pid)
      assert :ok = result
    end

    test "write then read same key in transaction returns written value" do
      {:ok, pid} = Cube.ClientStorage.start_link("tx_write_read_#{:rand.uniform(100000)}")

      Cube.ClientStorage.begin_transaction(pid)

      v1 = %Parser.Value{type: :string, value: "written"}
      {:ok, "NIL", "written"} = Cube.ClientStorage.set(pid, "key", v1)

      {:ok, value} = Cube.ClientStorage.get(pid, "key")
      assert value == "written"
    end
  end
end
