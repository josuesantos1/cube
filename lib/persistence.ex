defmodule Persistence do
  def write(shard, command) do
    file_path = build_path(shard)
    File.write(file_path, command, [:append])
  end

  def read_line_by_prefix(shard, prefix) do
    file_path = build_path(shard)

    if File.exists?(file_path) do
      file_path
      |> File.stream!()
      |> Enum.to_list()
      |> Enum.reverse()
      |> Enum.find(fn line ->
        String.starts_with?(line, prefix)
      end)
      |> case do
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
    shard <> "_data.txt"
  end
end
