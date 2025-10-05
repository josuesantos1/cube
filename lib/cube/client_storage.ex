defmodule Cube.ClientStorage do
  use GenServer

  def start_link(client_name) do
    GenServer.start_link(__MODULE__, client_name, name: via_tuple(client_name))
  end

  defp via_tuple(client_name) do
    {:via, Registry, {Cube.ClientRegistry, client_name}}
  end

  @impl true
  def init(client_name) do
    {:ok, %{client_name: client_name, transaction: nil}}
  end

  def get(client_pid, key) do
    GenServer.call(client_pid, {:get, key})
  end

  def set(client_pid, key, value) do
    GenServer.call(client_pid, {:set, key, value})
  end

  def begin_transaction(client_pid) do
    GenServer.call(client_pid, :begin)
  end

  def commit(client_pid) do
    GenServer.call(client_pid, :commit)
  end

  def rollback(client_pid) do
    GenServer.call(client_pid, :rollback)
  end

  @impl true
  def handle_call(:begin, _from, state) do
    case state.transaction do
      nil ->
        transaction = %{
          reads: %{},
          writes: %{}
        }
        {:reply, :ok, %{state | transaction: transaction}}

      _active ->
        {:reply, {:error, "Already in transaction"}, state}
    end
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case state.transaction do
      nil ->
        result = Cube.GlobalStorage.get(key)
        {:reply, result, state}

      transaction ->
        case Map.get(transaction.writes, key) do
          nil ->
            case Map.get(transaction.reads, key) do
              nil ->
                {:ok, value} = Cube.GlobalStorage.get(key)
                updated_transaction = %{transaction | reads: Map.put(transaction.reads, key, value)}
                {:reply, {:ok, value}, %{state | transaction: updated_transaction}}

              cached_value ->
                {:reply, {:ok, cached_value}, state}
            end

          written_value ->
            {:reply, {:ok, written_value}, state}
        end
    end
  end

  @impl true
  def handle_call(:commit, _from, state) do
    case state.transaction do
      nil ->
        {:reply, {:error, "No transaction in progress"}, state}

      transaction ->
        conflicts =
          Enum.filter(transaction.reads, fn {key, expected_value} ->
            {:ok, current_value} = Cube.GlobalStorage.get(key)
            current_value != expected_value
          end)

        case conflicts do
          [] ->
            Enum.each(transaction.writes, fn {key, value_str} ->
              parsed_value =
                cond do
                  value_str == "NIL" -> %Parser.Value{type: :nil, value: nil}
                  value_str == "true" -> %Parser.Value{type: :boolean, value: true}
                  value_str == "false" -> %Parser.Value{type: :boolean, value: false}
                  String.match?(value_str, ~r/^\d+$/) -> %Parser.Value{type: :integer, value: String.to_integer(value_str)}
                  true -> %Parser.Value{type: :string, value: value_str}
                end

              Cube.GlobalStorage.set(key, parsed_value)
            end)

            {:reply, :ok, %{state | transaction: nil}}

          conflicting_keys ->
            conflicting_key_names = Enum.map(conflicting_keys, fn {key, _} -> key end)
            error_msg = "Atomicity failure (#{Enum.join(conflicting_key_names, ", ")})"
            {:reply, {:error, error_msg}, %{state | transaction: nil}}
        end
    end
  end

  @impl true
  def handle_call(:rollback, _from, state) do
    case state.transaction do
      nil ->
        {:reply, {:error, "No transaction in progress"}, state}

      _transaction ->
        {:reply, :ok, %{state | transaction: nil}}
    end
  end

  @impl true
  def handle_call({:set, key, value}, _from, state) do
    new_value_str = Storage.Engine.encode_value(value)

    case state.transaction do
      nil ->
        {:ok, old_value, ^new_value_str} = Cube.GlobalStorage.set(key, value)
        {:reply, {:ok, old_value, new_value_str}, state}

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
            reads: updated_reads
        }

        {:reply, {:ok, old_value, new_value_str}, %{state | transaction: updated_transaction}}
    end
  end

end
