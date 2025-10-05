defmodule Cube.ShardSupervisor do
  @moduledoc """
  Supervisor that manages 20 shard processes (0-19).
  Each shard runs in parallel for concurrent access.
  """
  use Supervisor

  @shard_count 20

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children =
      0..(@shard_count - 1)
      |> Enum.map(fn shard_number ->
        %{
          id: {Cube.ShardStorage, shard_number},
          start: {Cube.ShardStorage, :start_link, [shard_number]},
          restart: :permanent,
          type: :worker
        }
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Returns the total number of shards in the system.
  """
  def shard_count, do: @shard_count
end
