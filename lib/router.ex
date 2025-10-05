defmodule Cube.Router do
  use Plug.Router

  plug :match
  plug :dispatch

  get "/" do
    send_resp(conn, 200, "Hello")
  end

  post "/" do
    {:ok, body, conn} = Plug.Conn.read_body(conn)

    case Parser.Parser.parse(body) do
      {:ok, %{command: :get, key: key}} ->
        case Storage.get(key) do
          {:ok, value} -> send_resp(conn, 200, "OK: #{value}")
          {:error, reason} -> send_resp(conn, 400, "ERR: #{reason}")
        end

      {:ok, %{command: :set, key: key, value: value}} ->
        case Storage.set(key, value) do
          {:ok, _} -> send_resp(conn, 200, "OK: SET")
          {:already_exists, old_value} -> send_resp(conn, 200, "OK: ALREADY EXISTS (#{old_value})")
          {:error, reason} -> send_resp(conn, 400, "ERR: #{reason}")
        end

      {:error, reason} ->
        send_resp(conn, 400, "ERR: #{reason}")
    end
  end

  match _ do
    send_resp(conn, 404, "Not found")
  end
end
