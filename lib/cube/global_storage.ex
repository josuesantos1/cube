defmodule Cube.GlobalStorage do
  @moduledoc """
  Facade for accessing sharded storage.
  Delegates to individual ShardStorage processes for parallel access.
  """

  @doc """
  Gets a value from the appropriate shard.
  Returns {:ok, value} where value is the decoded string or "NIL"
  Optional timestamp parameter for MVCC snapshot isolation.
  """
  def get(key, timestamp \\ nil) do
    shard_str = get_shard(key)
    Cube.ShardStorage.get(shard_str, key, timestamp)
  end

  @doc """
  Sets a value in the appropriate shard.
  Returns {:ok, old_value, new_value}
  """
  def set(key, value) do
    shard_str = get_shard(key)
    Cube.ShardStorage.set(shard_str, key, value)
  end

  defp get_shard(key) do
    {_command, shard} = Encoder.encode_get(key)
    shard
  end
end
