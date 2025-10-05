defmodule Persistence do
  def write(shard, command) do
    file_path = build_path(shard)
    case File.write(file_path, command, [:append]) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def update_or_append(shard, command, key_prefix) do
    file_path = build_path(shard)

    if File.exists?(file_path) do
      lines =
        file_path
        |> File.stream!()
        |> Enum.map(&String.trim/1)
        |> Enum.to_list()

      {updated_lines, found} =
        Enum.map_reduce(lines, false, fn line, found ->
          if String.starts_with?(line, key_prefix) and not found do
            {String.trim(command), true}
          else
            {line, found}
          end
        end)

      final_lines =
        if found do
          updated_lines
        else
          updated_lines ++ [String.trim(command)]
        end

      content = Enum.join(final_lines, "\n") <> "\n"
      File.write(file_path, content)
    else
      File.write(file_path, command)
    end
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
