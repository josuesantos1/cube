defmodule Cube.ClientStorage do
  @moduledoc """
  Stateless client storage handling GET/SET operations and transactions.
  Transaction state is stored in an Agent process.
  """

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
    Agent.start_link(
      fn -> %{transactions: %{}, begin_timestamps: %{}} end,
      name: __MODULE__
    )
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
    Agent.get(__MODULE__, fn state ->
      Map.get(state.begin_timestamps, client_name)
    end)
  end

  defp get_cached_or_snapshot_value(client_name, transaction, key) do
    case Map.get(transaction.reads, key) do
      nil ->
        begin_ts = get_begin_timestamp(client_name)
        {:ok, value} = Cube.GlobalStorage.get(key, begin_ts)
        {value, Map.put(transaction.reads, key, value)}

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
    Agent.update(__MODULE__, fn state ->
      %{
        state
        | begin_timestamps: Map.put(state.begin_timestamps, client_name, System.monotonic_time())
      }
    end)
  end

  def commit(client_name) do
    case get_transaction(client_name) do
      nil ->
        {:error, "No transaction in progress"}

      transaction ->
        conflicts =
          transaction.reads
          |> Enum.filter(fn {key, expected_value} ->
            {:ok, current_value} = Cube.GlobalStorage.get(key)
            current_value != expected_value
          end)

        case conflicts do
          [] ->
            Enum.each(transaction.writes, fn {key, value_str} ->
              parsed_value =
                cond do
                  value_str == "NIL" ->
                    %Parser.Value{type: nil, value: nil}

                  value_str == "true" ->
                    %Parser.Value{type: :boolean, value: true}

                  value_str == "false" ->
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
    Agent.get(__MODULE__, fn state -> Map.get(state.transactions, client_name) end)
  end

  defp put_transaction(client_name, transaction) do
    Agent.update(__MODULE__, fn state ->
      %{state | transactions: Map.put(state.transactions, client_name, transaction)}
    end)
  end

  defp cleanup_transaction(client_name) do
    Agent.update(__MODULE__, fn state ->
      %{
        state
        | transactions: Map.delete(state.transactions, client_name),
          begin_timestamps: Map.delete(state.begin_timestamps, client_name)
      }
    end)
  end
end
