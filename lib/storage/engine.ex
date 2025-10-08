defmodule Storage.Engine do
  @moduledoc """
  Core storage engine logic shared between different storage implementations.

  Handles:
  - GET operations with Bloom filter optimization
  - SET operations with persistence and filter updates
  - Value encoding/decoding
  - Filter management
  """

  defp read_value(shard_identifier, key_prefix, filter) do
    if Filter.contains?(filter, key_prefix) do
      case Persistence.read_line_by_prefix(shard_identifier, key_prefix) do
        nil -> "NIL"
        line -> Encoder.decode(line)
      end
    else
      "NIL"
    end
  end

  @doc """
  Retrieves a value from storage.

  Uses Bloom filter to optimize lookups - if filter says key doesn't exist,
  we can skip disk I/O.

  ## Parameters
  - shard_identifier: The shard file identifier (e.g., "00_Alice")
  - key: The key to retrieve
  - filter: The Bloom filter for this shard

  ## Returns
  - {:ok, value} where value is the decoded string or "NIL"
  """
  def get(shard_identifier, key, filter) do
    {command, _shard} = Encoder.encode_get(key)
    key_prefix = Encoder.extract_key_prefix(command)
    {:ok, read_value(shard_identifier, key_prefix, filter)}
  end

  @doc """
  Sets a value in storage.

  ## Parameters
  - shard_identifier: The shard file identifier (e.g., "00_Alice")
  - key: The key to set
  - value: The Parser.Value struct with type and value
  - filter: The Bloom filter for this shard

  ## Returns
  - {:ok, old_value, new_value, updated_filter}
  """
  def set(shard_identifier, key, value, filter) do
    {command, _shard} = Encoder.encode_set(key, value)
    key_prefix = Encoder.extract_key_prefix(command)
    new_value_str = encode_value(value)

    old_value = read_value(shard_identifier, key_prefix, filter)

    WAL.log(shard_identifier, command)

    Persistence.update_or_append(shard_identifier, command, key_prefix)
    updated_filter = Filter.add(filter, key_prefix)

    {:ok, old_value, new_value_str, updated_filter}
  end

  @doc """
  Encodes a Parser.Value into a string representation.

  ## Examples
      iex> Storage.Engine.encode_value(%Parser.Value{type: :string, value: "test"})
      "test"

      iex> Storage.Engine.encode_value(%Parser.Value{type: :integer, value: 42})
      "42"

      iex> Storage.Engine.encode_value(%Parser.Value{type: :boolean, value: true})
      "TRUE"
  """
  def encode_value(%Parser.Value{type: type, value: value}) do
    case type do
      :string -> value
      :integer -> Integer.to_string(value)
      :boolean -> if value, do: "TRUE", else: "FALSE"
      nil -> "NIL"
    end
  end

  @doc """
  Loads a Bloom filter from a shard file.

  Reads all lines from the shard file and populates a Bloom filter
  with the key prefixes. Also replays WAL if present.

  ## Parameters
  - shard_identifier: The shard file identifier (e.g., "shard_00")

  ## Returns
  - A populated Bloom filter
  """
  def load_filter(shard_identifier) do
    replay_wal(shard_identifier)

    filter = Filter.new()

    shard_identifier
    |> Persistence.stream_lines()
    |> Enum.reduce(filter, fn line, acc ->
      key_encoded = Encoder.extract_key_prefix(String.trim(line))
      Filter.add(acc, key_encoded)
    end)
  end

  defp replay_wal(shard_identifier) do
    shard_identifier
    |> WAL.replay()
    |> Enum.each(fn command ->
      key_prefix = Encoder.extract_key_prefix(command)
      Persistence.update_or_append(shard_identifier, command, key_prefix)
    end)

    WAL.clear(shard_identifier)
  end
end
