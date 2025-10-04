defmodule Storage do
  def exec(%Parser.Query{type: :get, command: command, shard: shard}) do
    IO.puts("Executing query: #{command}")
  end

 # mudar para receber query
 # mudar parser query para adicionar um field shard
  def exec(%Parser.Query{type: :set, command: command, shard: shard}) do
    IO.puts("Executing command: #{command}")
    # adicionar value
    File.write(shard <> "_data.txt", command, [:append])
    :ok
  end

  def get(id) do
    IO.puts("Retrieving data for ID: #{id}")
    {:ok, %{id: id, data: "Sample Data"}}
  end
end
