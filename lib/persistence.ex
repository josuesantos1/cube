defmodule Persistence do
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_) do
    {:ok, %{cache: %{}}}
  end

  @impl true
  def handle_call({:update_or_append, shard, command, key_prefix}, _from, state) do
    result = do_update_or_append(shard, command, key_prefix)

    cache_key = {shard, key_prefix}
    new_cache = Map.put(state.cache, cache_key, String.trim(command))

    {:reply, result, %{state | cache: new_cache}}
  end

  @impl true
  def handle_call({:read_line_by_prefix, shard, prefix}, _from, state) do
    cache_key = {shard, prefix}

    result =
      case Map.get(state.cache, cache_key) do
        nil ->
          value = do_read_line_by_prefix(shard, prefix)
          new_cache = if value, do: Map.put(state.cache, cache_key, value), else: state.cache
          {value, new_cache}

        cached_value ->
          {cached_value, state.cache}
      end

    {value, new_cache} = result
    {:reply, value, %{state | cache: new_cache}}
  end

  @doc false
  def write(shard, command) do
    file_path = build_path(shard)
    File.write(file_path, command, [:append])
  end

  def update_or_append(shard, command, key_prefix) do
    GenServer.call(__MODULE__, {:update_or_append, shard, command, key_prefix}, 30_000)
  end

  defp do_update_or_append(shard, command, _key_prefix) do
    file_path = build_path(shard)
    File.write(file_path, String.trim(command) <> "\n", [:append, :sync])
  end

  def read_line_by_prefix(shard, prefix) do
    GenServer.call(__MODULE__, {:read_line_by_prefix, shard, prefix}, 30_000)
  end

  defp do_read_line_by_prefix(shard, prefix) do
    file_path = build_path(shard)

    if File.exists?(file_path) do
      result = file_path
      |> File.stream!([], :line)
      |> Enum.reverse()
      |> Enum.find(fn line -> String.starts_with?(line, prefix) end)

      case result do
        nil -> nil
        line -> String.trim(line)
      end
    else
      nil
    end
  end

  def stream_lines(shard) do
    file_path = build_path(shard)

    if File.exists?(file_path) do
      File.stream!(file_path)
    else
      []
    end
  end

  def exists?(shard) do
    file_path = build_path(shard)
    File.exists?(file_path)
  end

  defp build_path(shard) do
    data_dir = System.get_env("DATA_DIR", ".")
    Path.join(data_dir, shard <> "_data.txt")
  end
end
