defmodule Cube.Router do
  use Plug.Router

  plug :match
  plug :dispatch

  get "/" do
    send_resp(conn, 200, "Hello")
  end

  post "/" do
    send_resp(conn, 200, "Data stored")
  end

  match _ do
    send_resp(conn, 404, "Not found")
  end
end
