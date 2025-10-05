defmodule Storage do
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

  def exec(%Parser.Query{type: :get, command: command, shard: shard}) do
    IO.puts("Executing GET query: #{command}")
    {:ok, get(command, shard)}
  end

  def exec(%Parser.Query{type: :set, command: command, shard: shard}) do
    IO.puts("Executing SET command: #{command}")

    key_encoded = extract_key_from_command(command)

    if bloom_contains?(shard, key_encoded) do
      old_value = get_from_file(command, shard)
      # TODO: Update the value in the file (not implemented)
      {:already_exists, old_value}
    else
      GenServer.call(__MODULE__, {:add_to_filter, shard, key_encoded})
      File.write(shard <> "_data.txt", command, [:append])
      {:ok, command}
    end
  end

  defp get(command, shard) do
    IO.puts("Retrieving data for command: #{command} in shard: #{shard}")

    key_encoded = extract_key_from_command(command)

    if bloom_contains?(shard, key_encoded) do
      get_from_file(command, shard)
    else
      "NIL"
    end
  end

  defp get_from_file(command, shard) do
    file_path = shard <> "_data.txt"

    if File.exists?(file_path) do
      file_path
      |> File.stream!()
      |> Enum.find(fn line ->
        String.starts_with?(line, command)
      end)
      |> case do
        nil -> "NIL"
        line ->
          line
          |> String.trim()
          |> Parser.Data.decoding()
      end
    else
      "NIL"
    end
  end

  defp bloom_contains?(shard, key) do
    GenServer.call(__MODULE__, {:contains?, shard, key})
  end

  defp extract_key_from_command(command) do
    key_length = String.slice(command, 0, 3) |> String.to_integer(16)
    String.slice(command, 0, 3 + key_length)
  end

  defp load_filter_from_file(shard) do
    file_path = shard <> "_data.txt"
    filter = Filter.new()

    if File.exists?(file_path) do
      file_path
      |> File.stream!()
      |> Enum.reduce(filter, fn line, acc ->
        key_encoded = extract_key_from_command(String.trim(line))
        Filter.add(acc, key_encoded)
      end)
    else
      filter
    end
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
