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
    filters =
      0..19
      |> Enum.map(fn shard ->
        shard_str = String.pad_leading(Integer.to_string(shard), 2, "0")
        {shard_str, load_filter_from_file(client_name, shard_str)}
      end)
      |> Map.new()

    {:ok, %{client_name: client_name, filters: filters}}
  end

  def get(client_pid, key) do
    GenServer.call(client_pid, {:get, key})
  end

  def set(client_pid, key, value) do
    GenServer.call(client_pid, {:set, key, value})
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    {command, shard} = Encoder.encode_get(key)
    key_prefix = Encoder.extract_key_prefix(command)

    result =
      if bloom_contains?(state, shard, key_prefix) do
        case Persistence.read_line_by_prefix(shard_file(state.client_name, shard), command) do
          nil -> {:ok, "NIL"}
          line -> {:ok, Encoder.decode(line)}
        end
      else
        {:ok, "NIL"}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:set, key, value}, _from, state) do
    {command, shard} = Encoder.encode_set(key, value)
    key_prefix = Encoder.extract_key_prefix(command)

    result =
      if bloom_contains?(state, shard, key_prefix) do
        old_value =
          case Persistence.read_line_by_prefix(shard_file(state.client_name, shard), command) do
            nil -> "NIL"
            line -> Encoder.decode(line)
          end

        {:already_exists, old_value}
      else
        updated_filter = Filter.add(Map.get(state.filters, shard), key_prefix)
        updated_filters = Map.put(state.filters, shard, updated_filter)
        Persistence.write(shard_file(state.client_name, shard), command)
        {:ok, nil}
      end

    new_state =
      if match?({:ok, _}, result) do
        updated_filter = Filter.add(Map.get(state.filters, shard), key_prefix)
        %{state | filters: Map.put(state.filters, shard, updated_filter)}
      else
        state
      end

    {:reply, result, new_state}
  end

  defp bloom_contains?(state, shard, key) do
    filter = Map.get(state.filters, shard)
    Filter.contains?(filter, key)
  end

  defp load_filter_from_file(client_name, shard) do
    filter = Filter.new()
    shard_file = shard_file(client_name, shard)

    shard_file
    |> Persistence.stream_lines()
    |> Enum.reduce(filter, fn line, acc ->
      key_encoded = Encoder.extract_key_prefix(String.trim(line))
      Filter.add(acc, key_encoded)
    end)
  end

  defp shard_file(client_name, shard) do
    "S#{shard}_#{client_name}"
  end
end
