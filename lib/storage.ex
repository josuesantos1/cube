defmodule Storage do
  @behaviour Storage.Behaviour
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def init(_) do
    filters =
      0..19
      |> Enum.map(fn shard ->
        shard_str = String.pad_leading(Integer.to_string(shard), 2, "0")
        {shard_str, load_filter_from_file(shard_str)}
      end)
      |> Map.new()

    {:ok, %{filters: filters}}
  end

  @impl Storage.Behaviour
  def get(key) do
    {command, shard} = Encoder.encode_get(key)
    key_prefix = Encoder.extract_key_prefix(command)

    if bloom_contains?(shard, key_prefix) do
      case Persistence.read_line_by_prefix(shard, command) do
        nil -> {:ok, "NIL"}
        line -> {:ok, Encoder.decode(line)}
      end
    else
      {:ok, "NIL"}
    end
  end

  @impl Storage.Behaviour
  def set(key, value) do
    {command, shard} = Encoder.encode_set(key, value)
    key_prefix = Encoder.extract_key_prefix(command)

    if bloom_contains?(shard, key_prefix) do
      old_value =
        case Persistence.read_line_by_prefix(shard, command) do
          nil -> "NIL"
          line -> Encoder.decode(line)
        end

      {:already_exists, old_value}
    else
      GenServer.call(__MODULE__, {:add_to_filter, shard, key_prefix})
      Persistence.write(shard, command)
      {:ok, nil}
    end
  end

  defp bloom_contains?(shard, key) do
    GenServer.call(__MODULE__, {:contains?, shard, key})
  end

  defp load_filter_from_file(shard) do
    filter = Filter.new()

    shard
    |> Persistence.stream_lines()
    |> Enum.reduce(filter, fn line, acc ->
      key_encoded = Encoder.extract_key_prefix(String.trim(line))
      Filter.add(acc, key_encoded)
    end)
  end

  def handle_call({:contains?, shard, key}, _from, state) do
    filter = Map.get(state.filters, shard)
    result = Filter.contains?(filter, key)
    {:reply, result, state}
  end

  def handle_call({:add_to_filter, shard, key}, _from, state) do
    filter = Map.get(state.filters, shard)
    updated_filter = Filter.add(filter, key)
    updated_filters = Map.put(state.filters, shard, updated_filter)
    {:reply, :ok, %{state | filters: updated_filters}}
  end
end
