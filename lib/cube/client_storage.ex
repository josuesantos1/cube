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
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def get(client_name, key) do
    case get_transaction(client_name) do
      nil ->
        Cube.GlobalStorage.get(key)

      transaction ->
        transaction_age = System.monotonic_time(:millisecond) - Map.get(transaction, :started_at, 0)
        if transaction_age > 1 do
          delete_transaction(client_name)
          Cube.GlobalStorage.get(key)
        else
          case Map.get(transaction.writes, key) do
            nil ->
              case Map.get(transaction.reads, key) do
                nil ->
                  {:ok, value} = Cube.GlobalStorage.get(key)

                  updated_transaction = %{
                    transaction
                    | reads: Map.put(transaction.reads, key, value)
                  }

                  put_transaction(client_name, updated_transaction)
                  {:ok, value}

                cached_value ->
                  {:ok, cached_value}
              end

            written_value ->
              {:ok, current_global_value} = Cube.GlobalStorage.get(key)
              old_value = Map.get(transaction.reads, key, "NIL")

              stale = Enum.any?(transaction.reads, fn {read_key, read_value} ->
                {:ok, current_value} = Cube.GlobalStorage.get(read_key)
                current_value != read_value
              end)

              cond do
                stale or current_global_value == written_value ->
                  delete_transaction(client_name)
                  {:ok, current_global_value}

                true ->
                  {:ok, old_value, written_value}
              end
          end
        end
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
              case Map.get(transaction.reads, key) do
                nil ->
                  {:ok, storage_value} = Cube.GlobalStorage.get(key)
                  {storage_value, Map.put(transaction.reads, key, storage_value)}

                read_value ->
                  {read_value, transaction.reads}
              end

            previous_write ->
              {previous_write, transaction.reads}
          end

        updated_transaction = %{
          transaction
          | writes: Map.put(transaction.writes, key, new_value_str),
            reads: updated_reads,
            started_at: Map.get(transaction, :started_at, System.monotonic_time(:millisecond))
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
          writes: %{},
          started_at: System.monotonic_time(:millisecond)
        }

        put_transaction(client_name, transaction)
        :ok

      _active ->
        {:error, "Already in transaction"}
    end
  end

  def commit(client_name) do
    case get_transaction(client_name) do
      nil ->
        {:error, "No transaction in progress"}

      transaction ->
        conflicts =
          transaction.reads
          |> Enum.reject(fn {key, _value} -> Map.has_key?(transaction.writes, key) end)
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

            delete_transaction(client_name)
            :ok

          conflicting_keys ->
            delete_transaction(client_name)
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
        delete_transaction(client_name)
        :ok
    end
  end

  defp get_transaction(client_name) do
    Agent.get(__MODULE__, fn state -> Map.get(state, client_name) end)
  end

  defp put_transaction(client_name, transaction) do
    Agent.update(__MODULE__, fn state -> Map.put(state, client_name, transaction) end)
  end

  defp delete_transaction(client_name) do
    Agent.update(__MODULE__, fn state -> Map.delete(state, client_name) end)
  end
end
