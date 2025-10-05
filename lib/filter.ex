defmodule Filter do
  defstruct [:size, :counters, :hash_count]

  def new(size \\ 10000, hash_count \\ 3) do
    %__MODULE__{
      size: size,
      counters: :atomics.new(size, signed: false),
      hash_count: hash_count
    }
  end

  def add(filter, key) do
    key
    |> get_hash_positions(filter.size, filter.hash_count)
    |> Enum.each(fn pos ->
      :atomics.add(filter.counters, pos + 1, 1)
    end)
    filter
  end

  def remove(filter, key) do
    key
    |> get_hash_positions(filter.size, filter.hash_count)
    |> Enum.each(fn pos ->
      current = :atomics.get(filter.counters, pos + 1)
      if current > 0 do
        :atomics.sub(filter.counters, pos + 1, 1)
      end
    end)
    filter
  end

  def contains?(filter, key) do
    key
    |> get_hash_positions(filter.size, filter.hash_count)
    |> Enum.all?(fn pos ->
      :atomics.get(filter.counters, pos + 1) > 0
    end)
  end

  defp get_hash_positions(key, size, hash_count) do
    base_hash = :erlang.phash2(key)

    0..(hash_count - 1)
    |> Enum.map(fn i ->
      rem(:erlang.phash2({base_hash, i}), size)
    end)
  end
end
