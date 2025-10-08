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

    result =
      if File.exists?(file_path) do
        found = file_exists_with_prefix?(file_path, key_prefix)

        if found do
          update_line_in_place(file_path, key_prefix, command)
        else
          File.write(file_path, String.trim(command) <> "\n", [:append])
        end
      else
        File.write(file_path, command)
      end

    case result do
      :ok -> :ok
      error -> error
    end
  end

  defp file_exists_with_prefix?(file_path, key_prefix) do
    file_path
    |> File.stream!([], :line)
    |> Enum.any?(fn line -> String.starts_with?(line, key_prefix) end)
  end

  defp update_line_in_place(file_path, key_prefix, command) do
    temp_path = file_path <> ".tmp"

    file_path
    |> File.stream!([], :line)
    |> Stream.transform(false, fn line, found ->
      if String.starts_with?(line, key_prefix) and not found do
        {[String.trim(command) <> "\n"], true}
      else
        {[line], found}
      end
    end)
    |> Stream.into(File.stream!(temp_path))
    |> Stream.run()

    File.rename(temp_path, file_path)
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
