defmodule Storage do
  def set(data) do
    IO.puts("Storing data: #{inspect(data)}")

    File.write("data.txt", "#{inspect(data)}\n", [:append])
    :ok
  end

  def get(id) do
    IO.puts("Retrieving data for ID: #{id}")
    {:ok, %{id: id, data: "Sample Data"}}
  end

  defp shard(id) do
    rem(id, 20)
  end
end
