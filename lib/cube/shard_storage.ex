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
  """
  def get(shard_str, key) do
    GenServer.call(via_tuple(shard_str), {:get, key})
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
       filter: filter
     }}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    result = Storage.Engine.get(state.shard_identifier, key, state.filter)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:set, key, value}, _from, state) do
    {:ok, old_value, new_value_str, updated_filter} =
      Storage.Engine.set(state.shard_identifier, key, value, state.filter)

    new_state = %{state | filter: updated_filter}
    {:reply, {:ok, old_value, new_value_str}, new_state}
  end
end
