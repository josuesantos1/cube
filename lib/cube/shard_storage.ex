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
  Gets a value from this shard for a specific client.
  Returns {:ok, value} where value is the decoded string or "NIL"
  """
  def get(shard_str, client_name, key) do
    GenServer.call(via_tuple(shard_str), {:get, client_name, key})
  end

  @doc """
  Sets a value in this shard for a specific client.
  Returns {:ok, old_value, new_value}
  """
  def set(shard_str, client_name, key, value) do
    GenServer.call(via_tuple(shard_str), {:set, client_name, key, value})
  end

  @impl true
  def init(shard_str) do
    {:ok, %{
      shard_str: shard_str,
      filters: %{}
    }}
  end

  @impl true
  def handle_call({:get, client_name, key}, _from, state) do
    shard_identifier = "shard_#{state.shard_str}_#{client_name}"
    filter = get_or_load_filter(state.filters, client_name, shard_identifier)

    result = Storage.Engine.get(shard_identifier, key, filter)
    {:reply, result, %{state | filters: Map.put(state.filters, client_name, filter)}}
  end

  @impl true
  def handle_call({:set, client_name, key, value}, _from, state) do
    shard_identifier = "shard_#{state.shard_str}_#{client_name}"
    filter = get_or_load_filter(state.filters, client_name, shard_identifier)

    {:ok, old_value, new_value_str, updated_filter} =
      Storage.Engine.set(shard_identifier, key, value, filter)

    new_state = %{state | filters: Map.put(state.filters, client_name, updated_filter)}
    {:reply, {:ok, old_value, new_value_str}, new_state}
  end

  defp get_or_load_filter(filters, client_name, shard_identifier) do
    case Map.get(filters, client_name) do
      nil -> Storage.Engine.load_filter(shard_identifier)
      filter -> filter
    end
  end
end
