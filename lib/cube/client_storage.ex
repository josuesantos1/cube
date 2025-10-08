defmodule Cube.ClientStorage do
  @moduledoc """
  Stateless client storage handling GET/SET operations and transactions.
  Transaction state is stored in an Agent process.
  """
  use GenServer

  @transaction_timeout 3_600_000

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_) do
    schedule_cleanup()
    {:ok, %{transactions: %{}, begin_timestamps: %{}}}
  end

  @impl true
  def handle_info(:cleanup_stale_transactions, state) do
    now = System.monotonic_time(:millisecond)
    cutoff = now - @transaction_timeout

    new_timestamps =
      state.begin_timestamps
      |> Enum.filter(fn {_client, ts} -> ts > cutoff end)
      |> Map.new()

    new_transactions = Map.take(state.transactions, Map.keys(new_timestamps))

    schedule_cleanup()
    {:noreply, %{transactions: new_transactions, begin_timestamps: new_timestamps}}
  end

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup_stale_transactions, 60_000)
  end

  def get(client_name, key) do
    case get_transaction(client_name) do
      nil ->
        Cube.GlobalStorage.get(key)

      transaction ->
        case Map.get(transaction.writes, key) do
          nil ->
            {value, updated_reads} = get_cached_or_snapshot_value(client_name, transaction, key)
            updated_transaction = %{transaction | reads: updated_reads}
            put_transaction(client_name, updated_transaction)
            {:ok, value}

          written_value ->
            {:ok, written_value}
        end
    end
  end

  defp get_begin_timestamp(client_name) do
    GenServer.call(__MODULE__, {:get_begin_timestamp, client_name})
  end

  defp get_cached_or_snapshot_value(client_name, transaction, key) do
    case Map.get(transaction.reads, key) do
      nil ->
        begin_ts = get_begin_timestamp(client_name)
        case Cube.GlobalStorage.get(key, begin_ts) do
          {:ok, value} -> {value, Map.put(transaction.reads, key, value)}
          {:error, _reason} -> {"NIL", Map.put(transaction.reads, key, "NIL")}
        end

      cached_value ->
        {cached_value, transaction.reads}
    end
  end

  def set(client_name, key, value) do
    new_value_str = Storage.Engine.encode_value(value)

    case get_transaction(client_name) do
      nil ->
        {:ok, old_value, ^new_value_str} = Cube.GlobalStorage.set(key, value)
        {:ok, old_value, new_value_str}

      transaction ->
        {old_value, updated_reads} =
          case Map.get(transaction.writes, key) do
            nil ->
              get_cached_or_snapshot_value(client_name, transaction, key)

            previous_write ->
              {previous_write, transaction.reads}
          end

        updated_transaction = %{
          transaction
          | writes: Map.put(transaction.writes, key, new_value_str),
            reads: updated_reads
        }

        put_transaction(client_name, updated_transaction)
        {:ok, old_value, new_value_str}
    end
  end

  def begin_transaction(client_name) do
    case get_transaction(client_name) do
      nil ->
        transaction = %{
          reads: %{},
          writes: %{}
        }

        put_transaction(client_name, transaction)
        put_begin_timestamp(client_name)
        :ok

      _active ->
        {:error, "Already in transaction"}
    end
  end

  defp put_begin_timestamp(client_name) do
    GenServer.call(__MODULE__, {:put_begin_timestamp, client_name})
  end

  def commit(client_name) do
    case get_transaction(client_name) do
      nil ->
        {:error, "No transaction in progress"}

      transaction ->
        conflicts =
          transaction.reads
          |> Enum.filter(fn {key, expected_value} ->
            case Cube.GlobalStorage.get(key) do
              {:ok, current_value} -> current_value != expected_value
              {:error, _} -> false
            end
          end)

        case conflicts do
          [] ->
            Enum.each(transaction.writes, fn {key, value_str} ->
              parsed_value =
                cond do
                  value_str == "NIL" ->
                    %Parser.Value{type: nil, value: nil}

                  value_str == "TRUE" ->
                    %Parser.Value{type: :boolean, value: true}

                  value_str == "FALSE" ->
                    %Parser.Value{type: :boolean, value: false}

                  String.match?(value_str, ~r/^\d+$/) ->
                    %Parser.Value{type: :integer, value: String.to_integer(value_str)}

                  true ->
                    %Parser.Value{type: :string, value: value_str}
                end

              Cube.GlobalStorage.set(key, parsed_value)
            end)

            cleanup_transaction(client_name)
            :ok

          conflicting_keys ->
            cleanup_transaction(client_name)
            conflicting_key_names = Enum.map(conflicting_keys, fn {key, _} -> key end)
            error_msg = "Atomicity failure (#{Enum.join(conflicting_key_names, ", ")})"
            {:error, error_msg}
        end
    end
  end

  def rollback(client_name) do
    case get_transaction(client_name) do
      nil ->
        {:error, "No transaction in progress"}

      _transaction ->
        cleanup_transaction(client_name)
        :ok
    end
  end

  defp get_transaction(client_name) do
    GenServer.call(__MODULE__, {:get_transaction, client_name})
  end

  defp put_transaction(client_name, transaction) do
    GenServer.call(__MODULE__, {:put_transaction, client_name, transaction})
  end

  defp cleanup_transaction(client_name) do
    GenServer.call(__MODULE__, {:cleanup_transaction, client_name})
  end

  @impl true
  def handle_call({:get_begin_timestamp, client_name}, _from, state) do
    {:reply, Map.get(state.begin_timestamps, client_name), state}
  end

  @impl true
  def handle_call({:put_begin_timestamp, client_name}, _from, state) do
    new_timestamps = Map.put(state.begin_timestamps, client_name, System.monotonic_time(:millisecond))
    {:reply, :ok, %{state | begin_timestamps: new_timestamps}}
  end

  @impl true
  def handle_call({:get_transaction, client_name}, _from, state) do
    {:reply, Map.get(state.transactions, client_name), state}
  end

  @impl true
  def handle_call({:put_transaction, client_name, transaction}, _from, state) do
    new_transactions = Map.put(state.transactions, client_name, transaction)
    {:reply, :ok, %{state | transactions: new_transactions}}
  end

  @impl true
  def handle_call({:cleanup_transaction, client_name}, _from, state) do
    new_state = %{
      transactions: Map.delete(state.transactions, client_name),
      begin_timestamps: Map.delete(state.begin_timestamps, client_name)
    }
    {:reply, :ok, new_state}
  end
end
