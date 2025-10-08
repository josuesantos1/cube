defmodule Cube.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    port = String.to_integer(System.get_env("PORT") || "4000")

    children = [
      {Registry, keys: :unique, name: Cube.ShardRegistry},
      Persistence,
      Cube.ShardSupervisor,
      Cube.ClientStorage,
      {Bandit, plug: Cube.Router, scheme: :http, port: port}
    ]

    opts = [strategy: :one_for_one, name: Cube.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
