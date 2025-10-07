defmodule EdgeCasesTest do
  use ExUnit.Case, async: true
  import Plug.Test
  import Plug.Conn

  @opts Cube.Router.init([])

  test "Edge case test the challenger - BENGIN" do
    client_bob = "bob"
    client_alan = "alan"

    conn1 =
      conn(:post, "/", "GET teste")
      |> put_req_header("x-client-name", client_alan)
      |> Cube.Router.call(@opts)

    assert conn1.status == 200
    assert conn1.resp_body == "NIL"

    conn2 =
      conn(:post, "/", "GET teste")
      |> put_req_header("x-client-name", client_bob)
      |> Cube.Router.call(@opts)

    assert conn2.status == 200
    assert conn2.resp_body == "NIL"

    conn3 =
      conn(:post, "/", "BEGIN")
      |> put_req_header("x-client-name", client_alan)
      |> Cube.Router.call(@opts)

    assert conn3.status == 200
    assert conn3.resp_body == "OK"

    conn4 =
      conn(:post, "/", "SET teste 1")
      |> put_req_header("x-client-name", client_alan)
      |> Cube.Router.call(@opts)

    assert conn4.status == 200
    assert conn4.resp_body == "NIL 1"

    conn5 =
      conn(:post, "/", "GET teste")
      |> put_req_header("x-client-name", client_bob)
      |> Cube.Router.call(@opts)

    assert conn5.status == 200
    assert conn5.resp_body == "NIL"

    conn6 =
      conn(:post, "/", "GET teste")
      |> put_req_header("x-client-name", client_alan)
      |> Cube.Router.call(@opts)

    assert conn6.status == 200
    assert conn6.resp_body == "NIL 1"
  end

  test "Edge case test the challenger - COMMIT" do
    client_bob = "bob"
    client_alan = "alan"

    conn1 =
      conn(:post, "/", "GET teste")
      |> put_req_header("x-client-name", client_alan)
      |> Cube.Router.call(@opts)

    assert conn1.status == 200
    assert conn1.resp_body == "NIL"

    conn2 =
      conn(:post, "/", "GET teste")
      |> put_req_header("x-client-name", client_bob)
      |> Cube.Router.call(@opts)

    assert conn2.status == 200
    assert conn2.resp_body == "NIL"
  end
end
