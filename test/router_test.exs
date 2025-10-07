defmodule Cube.RouterTest do
  use ExUnit.Case, async: true
  import Plug.Test
  import Plug.Conn

  @opts Cube.Router.init([])

  describe "GET /" do
    test "returns hello message" do
      conn = conn(:get, "/")
      conn = Cube.Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 200
      assert conn.resp_body == "Hello"
    end
  end

  describe "unknown routes" do
    test "returns 404 for unknown GET routes" do
      conn = conn(:get, "/unknown")
      conn = Cube.Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 404
      assert conn.resp_body == "Not found"
    end

    test "returns 404 for unknown POST routes" do
      conn = conn(:post, "/unknown")
      conn = Cube.Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 404
      assert conn.resp_body == "Not found"
    end
  end

  describe "POST / - missing header" do
    test "returns 400 when X-Client-Name header is missing" do
      conn = conn(:post, "/", "GET key")
      conn = Cube.Router.call(conn, @opts)

      assert conn.status == 400
      assert conn.resp_body == "ERR X-Client-Name header required"
    end
  end

  describe "POST / - GET command" do
    test "returns NIL for non-existent key" do
      client_name = "router_get_nil_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "GET nonexistent")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "NIL"
    end

    test "returns value for existing key" do
      client_name = "router_get_existing_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "SET mykey \"myvalue\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET mykey")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "myvalue"
    end

    test "returns value for existing key with underscore" do
      client_name = "router_get_underscore_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "SET my_key \"my_value\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET my_key")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "my_value"
    end
  end

  describe "POST / - SET command" do
    test "returns NIL and new value for new key" do
      client_name = "router_set_new_#{:rand.uniform(100_000)}"
      key = "newkey_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "SET #{key} \"newvalue\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "NIL newvalue"
    end

    test "returns old and new values for existing key" do
      client_name = "router_set_update_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "SET key \"original\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "SET key \"updated\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "original updated"
    end

    test "handles integer values" do
      client_name = "router_set_int_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "SET age 42")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "NIL 42"
    end

    test "handles boolean values" do
      client_name = "router_set_bool_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "SET active true")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "NIL true"
    end

    test "handles nil values" do
      client_name = "router_set_nil_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "SET empty nil")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "NIL NIL"
    end

    test "handles quoted strings with spaces" do
      client_name = "router_set_spaces_#{:rand.uniform(100_000)}"
      key = "message_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "SET #{key} \"Hello World\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "NIL Hello World"
    end

    test "handles escaped quotes" do
      client_name = "router_set_quotes_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", ~s(SET text "He said \\"Hello\\""))
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body =~ "He said"
    end
  end

  describe "POST / - BEGIN command" do
    test "starts new transaction" do
      client_name = "router_begin_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "BEGIN")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "OK"
    end

    test "fails if already in transaction" do
      client_name = "router_begin_twice_#{:rand.uniform(100_000)}"

      _conn =
        conn(:post, "/", "BEGIN")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "BEGIN")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 400
      assert conn.resp_body == "ERR Already in transaction"
    end
  end

  describe "POST / - COMMIT command" do
    test "commits transaction successfully" do
      client_name = "router_commit_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "BEGIN")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      _conn2 =
        conn(:post, "/", "SET key \"value\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "COMMIT")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "OK"
    end

    test "fails when not in transaction" do
      client_name = "router_commit_no_tx_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "COMMIT")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 400
      assert conn.resp_body == "ERR No transaction in progress"
    end

    test "persists writes after commit" do
      client_name = "router_commit_persist_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "BEGIN")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      _conn2 =
        conn(:post, "/", "SET data \"committed\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      _conn3 =
        conn(:post, "/", "COMMIT")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET data")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "committed"
    end
  end

  describe "POST / - ROLLBACK command" do
    test "rolls back transaction successfully" do
      client_name = "router_rollback_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "BEGIN")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "ROLLBACK")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "OK"
    end

    test "fails when not in transaction" do
      client_name = "router_rollback_no_tx_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "ROLLBACK")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 400
      assert conn.resp_body == "ERR No transaction in progress"
    end

    test "discards writes after rollback" do
      client_name = "router_rollback_discard_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "SET key \"original\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      _conn2 =
        conn(:post, "/", "BEGIN")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      _conn3 =
        conn(:post, "/", "SET key \"modified\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      _conn4 =
        conn(:post, "/", "ROLLBACK")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET key")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "original"
    end
  end

  describe "POST / - error handling" do
    test "returns 400 for invalid command" do
      client_name = "router_invalid_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "INVALID COMMAND")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 400
      assert String.starts_with?(conn.resp_body, "ERR")
    end

    test "returns 400 for malformed GET" do
      client_name = "router_malformed_get_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "GET")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 400
      assert String.starts_with?(conn.resp_body, "ERR")
    end

    test "returns 400 for malformed SET" do
      client_name = "router_malformed_set_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "SET key")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 400
      assert String.starts_with?(conn.resp_body, "ERR")
    end

    test "handles empty body" do
      client_name = "router_empty_body_#{:rand.uniform(100_000)}"

      conn =
        conn(:post, "/", "")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 400
    end
  end

  describe "POST / - shared data across clients" do
    test "different clients share the same data" do
      client1 = "router_client1_#{:rand.uniform(100_000)}"
      client2 = "router_client2_#{:rand.uniform(100_000)}"
      key = "shared_#{:rand.uniform(100_000)}"

      _conn_setup =
        conn(:post, "/", "SET #{key} \"shared_value\"")
        |> put_req_header("x-client-name", client1)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET #{key}")
        |> put_req_header("x-client-name", client2)
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "shared_value"
    end

    test "transactions are isolated between clients but data is shared" do
      client1 = "router_tx_client1_#{:rand.uniform(100_000)}"
      client2 = "router_tx_client2_#{:rand.uniform(100_000)}"
      key = "tx_key_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "BEGIN")
        |> put_req_header("x-client-name", client1)
        |> Cube.Router.call(@opts)

      _conn2 =
        conn(:post, "/", "SET #{key} \"client1_tx\"")
        |> put_req_header("x-client-name", client1)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET #{key}")
        |> put_req_header("x-client-name", client2)
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "NIL"
    end
  end

  describe "POST / - special characters" do
    test "handles newlines in values" do
      client_name = "router_newline_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "SET text \"Line1\\nLine2\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET text")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert String.contains?(conn.resp_body, "\n")
    end

    test "handles tabs in values" do
      client_name = "router_tab_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "SET text \"Col1\\tCol2\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET text")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert String.contains?(conn.resp_body, "\t")
    end

    test "handles emoji in values" do
      client_name = "router_emoji_#{:rand.uniform(100_000)}"

      _conn1 =
        conn(:post, "/", "SET message \"Hello 🎉\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET message")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "Hello 🎉"
    end
  end
end
