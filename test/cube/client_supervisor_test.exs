defmodule Cube.ClientSupervisorTest do
  use ExUnit.Case, async: false

  setup do
    client_name = "test_client_#{:rand.uniform(1_000_000)}"
    on_exit(fn -> cleanup_client(client_name) end)
    {:ok, client_name: client_name}
  end

  defp cleanup_client(client_name) do
    case Registry.lookup(Cube.ClientRegistry, client_name) do
      [{pid, _}] ->
        DynamicSupervisor.terminate_child(Cube.ClientSupervisor, pid)

      [] ->
        :ok
    end

    for i <- 0..19 do
      shard_file = "shard_#{String.pad_leading(Integer.to_string(i), 2, "0")}_#{client_name}_data.txt"
      if File.exists?(shard_file), do: File.rm!(shard_file)
    end
  end

  describe "start_link/1" do
    test "starts the supervisor" do
      assert Process.whereis(Cube.ClientSupervisor) != nil
      assert Process.alive?(Process.whereis(Cube.ClientSupervisor))
    end

    test "supervisor is a DynamicSupervisor" do
      pid = Process.whereis(Cube.ClientSupervisor)
      count = DynamicSupervisor.count_children(pid)
      assert is_map(count)
      assert Map.has_key?(count, :active)
    end
  end

  describe "get_or_start_client/1" do
    test "starts a new client process", %{client_name: client_name} do
      assert {:ok, pid} = Cube.ClientSupervisor.get_or_start_client(client_name)
      assert Process.alive?(pid)
    end

    test "registers client in Registry", %{client_name: client_name} do
      {:ok, _pid} = Cube.ClientSupervisor.get_or_start_client(client_name)
      assert [{_pid, _}] = Registry.lookup(Cube.ClientRegistry, client_name)
    end

    test "returns existing client if already started", %{client_name: client_name} do
      {:ok, pid1} = Cube.ClientSupervisor.get_or_start_client(client_name)
      {:ok, pid2} = Cube.ClientSupervisor.get_or_start_client(client_name)
      assert pid1 == pid2
    end

    test "starts different clients independently" do
      client1 = "client_1_#{:rand.uniform(100_000)}"
      client2 = "client_2_#{:rand.uniform(100_000)}"

      on_exit(fn ->
        cleanup_client(client1)
        cleanup_client(client2)
      end)

      {:ok, pid1} = Cube.ClientSupervisor.get_or_start_client(client1)
      {:ok, pid2} = Cube.ClientSupervisor.get_or_start_client(client2)

      assert pid1 != pid2
      assert Process.alive?(pid1)
      assert Process.alive?(pid2)
    end

    test "client name is stored in client state", %{client_name: client_name} do
      {:ok, pid} = Cube.ClientSupervisor.get_or_start_client(client_name)
      state = :sys.get_state(pid)
      assert state.client_name == client_name
    end

    test "handles concurrent client starts", %{client_name: client_name} do
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            Cube.ClientSupervisor.get_or_start_client(client_name)
          end)
        end

      results = Task.await_many(tasks)
      pids = Enum.map(results, fn {:ok, pid} -> pid end)
      unique_pids = Enum.uniq(pids)

      assert length(unique_pids) == 1
    end

    test "handles atom client names" do
      atom_client = :atom_client_test

      result = Cube.ClientSupervisor.get_or_start_client(atom_client)
      assert {:ok, pid} = result
      assert Process.alive?(pid)

      on_exit(fn ->
        case Registry.lookup(Cube.ClientRegistry, atom_client) do
          [{pid, _}] -> DynamicSupervisor.terminate_child(Cube.ClientSupervisor, pid)
          [] -> :ok
        end
      end)
    end
  end

  describe "supervision tree" do
    test "supervisor restarts crashed client", %{client_name: client_name} do
      {:ok, pid1} = Cube.ClientSupervisor.get_or_start_client(client_name)
      Process.exit(pid1, :kill)
      Process.sleep(50)

      {:ok, pid2} = Cube.ClientSupervisor.get_or_start_client(client_name)
      assert pid1 != pid2
      assert Process.alive?(pid2)
    end

    test "client can be terminated and restarted", %{client_name: client_name} do
      {:ok, pid1} = Cube.ClientSupervisor.get_or_start_client(client_name)
      :ok = DynamicSupervisor.terminate_child(Cube.ClientSupervisor, pid1)
      Process.sleep(50)
      refute Process.alive?(pid1)

      {:ok, pid2} = Cube.ClientSupervisor.get_or_start_client(client_name)
      assert Process.alive?(pid2)
      assert pid1 != pid2
    end

    test "supervisor manages multiple clients simultaneously" do
      clients = for i <- 1..5, do: "multi_client_#{i}_#{:rand.uniform(100_000)}"

      on_exit(fn -> Enum.each(clients, &cleanup_client/1) end)

      pids =
        Enum.map(clients, fn client ->
          {:ok, pid} = Cube.ClientSupervisor.get_or_start_client(client)
          pid
        end)

      assert length(pids) == 5
      assert Enum.all?(pids, &Process.alive?/1)
      assert length(Enum.uniq(pids)) == 5
    end

    test "supervisor uses one_for_one strategy" do
      client1 = "strategy_client_1_#{:rand.uniform(100_000)}"
      client2 = "strategy_client_2_#{:rand.uniform(100_000)}"

      on_exit(fn ->
        cleanup_client(client1)
        cleanup_client(client2)
      end)

      {:ok, pid1} = Cube.ClientSupervisor.get_or_start_client(client1)
      {:ok, pid2} = Cube.ClientSupervisor.get_or_start_client(client2)

      Process.exit(pid1, :kill)
      Process.sleep(50)

      assert Process.alive?(pid2)
    end
  end

  describe "client isolation" do
    test "each client has isolated data" do
      client1 = "isolated_1_#{:rand.uniform(100_000)}"
      client2 = "isolated_2_#{:rand.uniform(100_000)}"

      on_exit(fn ->
        cleanup_client(client1)
        cleanup_client(client2)
      end)

      {:ok, pid1} = Cube.ClientSupervisor.get_or_start_client(client1)
      {:ok, pid2} = Cube.ClientSupervisor.get_or_start_client(client2)

      Cube.ClientStorage.set(pid1, "key", %Parser.Value{type: :string, value: "value1"})
      Cube.ClientStorage.set(pid2, "key", %Parser.Value{type: :string, value: "value2"})

      {:ok, result1} = Cube.ClientStorage.get(pid1, "key")
      {:ok, result2} = Cube.ClientStorage.get(pid2, "key")

      assert result1 == "value1"
      assert result2 == "value2"
    end

    test "clients have separate transaction states" do
      client1 = "tx_isolated_1_#{:rand.uniform(100_000)}"
      client2 = "tx_isolated_2_#{:rand.uniform(100_000)}"

      on_exit(fn ->
        cleanup_client(client1)
        cleanup_client(client2)
      end)

      {:ok, pid1} = Cube.ClientSupervisor.get_or_start_client(client1)
      {:ok, pid2} = Cube.ClientSupervisor.get_or_start_client(client2)

      Cube.ClientStorage.begin_transaction(pid1)
      state1 = :sys.get_state(pid1)
      state2 = :sys.get_state(pid2)

      assert state1.transaction != nil
      assert state2.transaction == nil
    end
  end

  describe "registry integration" do
    test "client is findable via Registry after start", %{client_name: client_name} do
      {:ok, original_pid} = Cube.ClientSupervisor.get_or_start_client(client_name)
      [{found_pid, _}] = Registry.lookup(Cube.ClientRegistry, client_name)
      assert original_pid == found_pid
    end

    test "Registry is cleaned up after client termination", %{client_name: client_name} do
      {:ok, pid} = Cube.ClientSupervisor.get_or_start_client(client_name)
      DynamicSupervisor.terminate_child(Cube.ClientSupervisor, pid)
      Process.sleep(50)

      assert Registry.lookup(Cube.ClientRegistry, client_name) == []
    end

    test "Registry entry is recreated after restart", %{client_name: client_name} do
      {:ok, pid1} = Cube.ClientSupervisor.get_or_start_client(client_name)
      DynamicSupervisor.terminate_child(Cube.ClientSupervisor, pid1)
      Process.sleep(50)

      {:ok, pid2} = Cube.ClientSupervisor.get_or_start_client(client_name)
      [{found_pid, _}] = Registry.lookup(Cube.ClientRegistry, client_name)

      assert found_pid == pid2
      assert pid1 != pid2
    end

    test "multiple lookups return same pid", %{client_name: client_name} do
      {:ok, _pid} = Cube.ClientSupervisor.get_or_start_client(client_name)

      [{pid1, _}] = Registry.lookup(Cube.ClientRegistry, client_name)
      [{pid2, _}] = Registry.lookup(Cube.ClientRegistry, client_name)

      assert pid1 == pid2
    end
  end

  describe "edge cases" do
    test "handles client names with special characters" do
      special_names = [
        "client@123",
        "client.test",
        "client-name",
        "client_with_underscore"
      ]

      on_exit(fn -> Enum.each(special_names, &cleanup_client/1) end)

      results =
        Enum.map(special_names, fn name ->
          Cube.ClientSupervisor.get_or_start_client(name)
        end)

      assert Enum.all?(results, fn {:ok, pid} -> Process.alive?(pid) end)
    end

    test "handles very long client names" do
      long_name = "client_" <> String.duplicate("x", 200)
      on_exit(fn -> cleanup_client(long_name) end)

      assert {:ok, pid} = Cube.ClientSupervisor.get_or_start_client(long_name)
      assert Process.alive?(pid)
    end

    test "handles rapid successive calls", %{client_name: client_name} do
      results =
        for _ <- 1..100 do
          Cube.ClientSupervisor.get_or_start_client(client_name)
        end

      pids = Enum.map(results, fn {:ok, pid} -> pid end)
      assert length(Enum.uniq(pids)) == 1
    end

    test "handles client restart during active transaction", %{client_name: client_name} do
      {:ok, pid1} = Cube.ClientSupervisor.get_or_start_client(client_name)
      Cube.ClientStorage.begin_transaction(pid1)
      Cube.ClientStorage.set(pid1, "key", %Parser.Value{type: :string, value: "value"})

      Process.exit(pid1, :kill)
      Process.sleep(50)

      {:ok, pid2} = Cube.ClientSupervisor.get_or_start_client(client_name)
      state = :sys.get_state(pid2)

      assert state.transaction == nil
    end
  end

  describe "stress testing" do
    test "handles many concurrent clients" do
      clients = for i <- 1..50, do: "stress_client_#{i}_#{:rand.uniform(100_000)}"
      on_exit(fn -> Enum.each(clients, &cleanup_client/1) end)

      tasks =
        Enum.map(clients, fn client ->
          Task.async(fn ->
            Cube.ClientSupervisor.get_or_start_client(client)
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      pids = Enum.map(results, fn {:ok, pid} -> pid end)

      assert length(pids) == 50
      assert Enum.all?(pids, &Process.alive?/1)
      assert length(Enum.uniq(pids)) == 50
    end

    test "survives rapid client churn" do
      client = "churn_client_#{:rand.uniform(100_000)}"
      on_exit(fn -> cleanup_client(client) end)

      for _ <- 1..20 do
        {:ok, pid} = Cube.ClientSupervisor.get_or_start_client(client)
        DynamicSupervisor.terminate_child(Cube.ClientSupervisor, pid)
        Process.sleep(10)
      end

      {:ok, final_pid} = Cube.ClientSupervisor.get_or_start_client(client)
      assert Process.alive?(final_pid)
    end
  end

  describe "supervisor state" do
    test "supervisor reports correct child count" do
      clients = for i <- 1..3, do: "count_client_#{i}_#{:rand.uniform(100_000)}"
      on_exit(fn -> Enum.each(clients, &cleanup_client/1) end)

      Enum.each(clients, &Cube.ClientSupervisor.get_or_start_client/1)

      count = DynamicSupervisor.count_children(Cube.ClientSupervisor)
      assert count.active >= 3
    end

    test "which_children returns client processes" do
      client = "which_client_#{:rand.uniform(100_000)}"
      on_exit(fn -> cleanup_client(client) end)

      {:ok, pid} = Cube.ClientSupervisor.get_or_start_client(client)
      children = DynamicSupervisor.which_children(Cube.ClientSupervisor)

      assert Enum.any?(children, fn
               {:undefined, ^pid, :worker, [Cube.ClientStorage]} -> true
               _ -> false
             end)
    end
  end
end
