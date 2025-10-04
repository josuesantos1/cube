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
      {:ok, parsed} ->
        parsed
        |> Parser.Data.encoding()
        |> Storage.set()
        send_resp(conn, 200, "OK: #{inspect(parsed)}")
      {:error, reason} ->
        send_resp(conn, 400, "ERR: #{reason}")
    end
  end

  match _ do
    send_resp(conn, 404, "Not found")
  end
end
