defmodule Cube.Router do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/" do
    send_resp(conn, 200, "Hello")
  end

  post "/" do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    client_name = get_req_header(conn, "x-client-name") |> List.first()

    unless client_name do
      send_resp(conn, 400, "ERR X-Client-Name header required")
    else
      case Parser.Parser.parse(body) do
        {:ok, %{command: :get, key: key}} ->
          case Cube.ClientStorage.get(client_name, key) do
            {:ok, old_value, new_value} -> send_resp(conn, 200, "#{old_value} #{new_value}")
            {:ok, value} -> send_resp(conn, 200, value)
            {:error, reason} -> send_resp(conn, 400, "ERR #{reason}")
          end

        {:ok, %{command: :set, key: key, value: value}} ->
          {:ok, old_value, new_value} = Cube.ClientStorage.set(client_name, key, value)
          send_resp(conn, 200, "#{old_value} #{new_value}")

        {:ok, %{command: :begin}} ->
          case Cube.ClientStorage.begin_transaction(client_name) do
            :ok -> send_resp(conn, 200, "OK")
            {:error, reason} -> send_resp(conn, 400, "ERR #{reason}")
          end

        {:ok, %{command: :commit}} ->
          case Cube.ClientStorage.commit(client_name) do
            :ok -> send_resp(conn, 200, "OK")
            {:error, reason} -> send_resp(conn, 400, "ERR #{reason}")
          end

        {:ok, %{command: :rollback}} ->
          case Cube.ClientStorage.rollback(client_name) do
            :ok -> send_resp(conn, 200, "OK")
            {:error, reason} -> send_resp(conn, 400, "ERR #{reason}")
          end

        {:error, reason} ->
          send_resp(conn, 400, "ERR #{reason}")
      end
    end
  end

  match _ do
    send_resp(conn, 404, "Not found")
  end
end
