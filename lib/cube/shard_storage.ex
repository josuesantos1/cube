defmodule Cube.ShardStorage do
  @moduledoc """
  GenServer that manages a single shard (partition) of the database.
  Each shard runs in its own process for parallel access.
  """
  use GenServer

  def start_link(shard_number) do
    shard_str = String.pad_leading(Integer.to_string(shard_number), 2, "0")
    GenServer.start_link(__MODULE__, shard_str, name: via_tuple(shard_str))
  end

  defp via_tuple(shard_str) do
    {:via, Registry, {Cube.ShardRegistry, shard_str}}
  end

  @doc """
  Gets a value from this shard.
  Returns {:ok, value} where value is the decoded string or "NIL"
  Optional timestamp parameter for MVCC snapshot isolation.
  """
  def get(shard_str, key, timestamp \\ nil) do
    GenServer.call(via_tuple(shard_str), {:get, key, timestamp})
  end

  @doc """
  Sets a value in this shard.
  Returns {:ok, old_value, new_value}
  """
  def set(shard_str, key, value) do
    GenServer.call(via_tuple(shard_str), {:set, key, value})
  end

  @impl true
  def init(shard_str) do
    shard_identifier = "shard_#{shard_str}"
    filter = Storage.Engine.load_filter(shard_identifier)

    {:ok,
     %{
       shard_str: shard_str,
       shard_identifier: shard_identifier,
       filter: filter,
       versions: %{}
     }}
  end

  @impl true
  def handle_call({:get, key, timestamp}, _from, state) do
    result =
      case timestamp do
        nil ->
          Storage.Engine.get(state.shard_identifier, key, state.filter)

        ts ->
          case Map.get(state.versions, key) do
            nil ->
              Storage.Engine.get(state.shard_identifier, key, state.filter)

            version_list ->
              case Enum.find(version_list, fn {v_ts, _value} -> v_ts <= ts end) do
                {_v_ts, value} -> {:ok, value}
                nil -> {:ok, "NIL"}
              end
          end
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:set, key, value}, _from, state) do
    {:ok, old_value, new_value_str, updated_filter} =
      Storage.Engine.set(state.shard_identifier, key, value, state.filter)

    timestamp = System.monotonic_time()
    current_versions = Map.get(state.versions, key, [])
    new_versions = [{timestamp, new_value_str} | current_versions]
    trimmed_versions = Enum.take(new_versions, 100)
    updated_versions = Map.put(state.versions, key, trimmed_versions)

    new_state = %{state | filter: updated_filter, versions: updated_versions}
    {:reply, {:ok, old_value, new_value_str}, new_state}
  end
end
