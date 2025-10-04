defmodule Storage do
  def set(data) do
    IO.puts("Storing data: #{inspect(data)}")

    id = :erlang.phash2(data, 20)
    filename = get_shard(id)
    |> Integer.to_string()
    |> String.pad_leading(2, "0")
    |> Kernel.<>("_data.txt")

    File.write(filename, data, [:append])
    :ok
  end

  def get(id) do
    IO.puts("Retrieving data for ID: #{id}")
    {:ok, %{id: id, data: "Sample Data"}}
  end

  defp get_shard(id) do
    rem(id, 20)
  end
end
