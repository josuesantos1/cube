defmodule WAL do
  @moduledoc """
  Write-Ahead Log for durability guarantees.

  Logs all SET operations before they are applied to ensure that
  data can be recovered after crashes.
  """

  @doc """
  Writes a SET operation to the WAL before it's applied.

  Format: Each line contains the encoded command that will be persisted.
  Uses fsync to ensure durability.

  ## Parameters
  - shard_identifier: The shard identifier (e.g., "shard_00")
  - command: The encoded command to log

  ## Returns
  - :ok on success
  - {:error, reason} on failure
  """
  def log(shard_identifier, command) do
    file_path = build_wal_path(shard_identifier)

    with :ok <- File.write(file_path, command, [:append]),
         {:ok, io_device} <- File.open(file_path, [:read]),
         :ok <- :file.sync(io_device),
         :ok <- File.close(io_device) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Replays all operations from the WAL on startup.

  Reads the WAL file and returns all commands to be replayed.

  ## Parameters
  - shard_identifier: The shard identifier

  ## Returns
  - List of commands to replay
  """
  def replay(shard_identifier) do
    file_path = build_wal_path(shard_identifier)

    if File.exists?(file_path) do
      file_path
      |> File.stream!()
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.to_list()
    else
      []
    end
  end

  @doc """
  Clears the WAL file for a shard.

  This should be called after a successful snapshot/checkpoint.
  """
  def clear(shard_identifier) do
    file_path = build_wal_path(shard_identifier)

    if File.exists?(file_path) do
      File.rm(file_path)
    else
      :ok
    end
  end

  defp build_wal_path(shard_identifier) do
    data_dir = System.get_env("DATA_DIR", ".")
    Path.join(data_dir, "wal_#{shard_identifier}.log")
  end
end
