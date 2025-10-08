defmodule Cube.Router do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/" do
    send_resp(conn, 200, "Hello")
  end

  defp handle_result(conn, result) do
    case result do
      :ok -> send_resp(conn, 200, "OK")
      {:ok, value} -> send_resp(conn, 200, value)
      {:error, reason} -> send_resp(conn, 400, "ERR \"#{reason}\"")
    end
  end

  post "/" do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    client_name = get_req_header(conn, "x-client-name") |> List.first()

    unless client_name do
      send_resp(conn, 400, "ERR \"X-Client-Name header required\"")
    else
      case Parser.Parser.parse(body) do
        {:ok, %{command: :get, key: key}} ->
          handle_result(conn, Cube.ClientStorage.get(client_name, key))

        {:ok, %{command: :set, key: key, value: value}} ->
          {:ok, old_value, new_value} = Cube.ClientStorage.set(client_name, key, value)
          send_resp(conn, 200, "#{old_value} #{new_value}")

        {:ok, %{command: :begin}} ->
          handle_result(conn, Cube.ClientStorage.begin_transaction(client_name))

        {:ok, %{command: :commit}} ->
          handle_result(conn, Cube.ClientStorage.commit(client_name))

        {:ok, %{command: :rollback}} ->
          handle_result(conn, Cube.ClientStorage.rollback(client_name))

        {:error, reason} ->
          send_resp(conn, 400, "ERR \"#{reason}\"")
      end
    end
  end

  match _ do
    send_resp(conn, 404, "Not found")
  end
end
