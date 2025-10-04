defmodule Storage do
  def exec(%Parser.Query{type: :get, command: command, shard: shard}) do
    IO.puts("Executing query: #{command}")
    get(command, shard)
  end

 # mudar para receber query
 # mudar parser query para adicionar um field shard
  def exec(%Parser.Query{type: :set, command: command, shard: shard}) do
    IO.puts("Executing command: #{command}")
    # adicionar value
    File.write(shard <> "_data.txt", command, [:append])
    :ok
  end

  defp get(command, shard) do
    IO.puts("Retrieving data for command: #{command} in shard: #{shard}")

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
          |> Parser.Data.decoding_string()
      end
    else
      "NIL"
    end
  end
end
