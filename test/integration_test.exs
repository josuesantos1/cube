defmodule Cube.IntegrationTest do
  use ExUnit.Case, async: false
  use Plug.Test

  @opts Cube.Router.init([])

  setup do
    File.ls!()
    |> Enum.filter(&String.ends_with?(&1, "_data.txt"))
    |> Enum.each(&File.rm/1)

    :ok
  end

  describe "HTTP POST with X-Client-Name header" do
    test "requires X-Client-Name header" do
      conn =
        conn(:post, "/", "GET test")
        |> Cube.Router.call(@opts)

      assert conn.status == 400
      assert conn.resp_body == "ERR X-Client-Name header required"
    end

    test "accepts request with X-Client-Name header" do
      conn =
        conn(:post, "/", "GET test")
        |> put_req_header("x-client-name", "Alice")
        |> Cube.Router.call(@opts)

      assert conn.status == 200
    end
  end

  describe "GET command via HTTP" do
    test "returns NIL for non-existent key" do
      conn =
        conn(:post, "/", "GET nonexistent")
        |> put_req_header("x-client-name", "TestClient")
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "NIL"
    end

    test "returns value for existing key" do
      client_name = "GetTestClient"

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
  end

  describe "SET command via HTTP" do
    test "returns TRUE for new key" do
      conn =
        conn(:post, "/", "SET newkey \"newvalue\"")
        |> put_req_header("x-client-name", "SetTestClient")
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "TRUE"
    end

    test "returns FALSE with old value for existing key" do
      client_name = "SetExistingClient"

      conn(:post, "/", "SET dupkey \"value1\"")
      |> put_req_header("x-client-name", client_name)
      |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "SET dupkey \"value2\"")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.status == 200
      assert conn.resp_body == "FALSE value1"
    end

    test "sets integer value" do
      client_name = "IntClient"

      conn(:post, "/", "SET age 25")
      |> put_req_header("x-client-name", client_name)
      |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET age")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "25"
    end

    test "sets boolean value" do
      client_name = "BoolClient"

      conn(:post, "/", "SET active true")
      |> put_req_header("x-client-name", client_name)
      |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET active")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "true"
    end
  end

  describe "multi-user isolation" do
    test "Alice and Bob have isolated data stores" do
      conn(:post, "/", "SET secret \"Alice's data\"")
      |> put_req_header("x-client-name", "Alice")
      |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET secret")
        |> put_req_header("x-client-name", "Bob")
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "NIL"

      conn =
        conn(:post, "/", "GET secret")
        |> put_req_header("x-client-name", "Alice")
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "Alice's data"
    end

    test "multiple clients can use same key name" do
      conn(:post, "/", "SET name \"Alice\"")
      |> put_req_header("x-client-name", "ClientA")
      |> Cube.Router.call(@opts)

      conn(:post, "/", "SET name \"Bob\"")
      |> put_req_header("x-client-name", "ClientB")
      |> Cube.Router.call(@opts)

      conn_a =
        conn(:post, "/", "GET name")
        |> put_req_header("x-client-name", "ClientA")
        |> Cube.Router.call(@opts)

      assert conn_a.resp_body == "Alice"

      conn_b =
        conn(:post, "/", "GET name")
        |> put_req_header("x-client-name", "ClientB")
        |> Cube.Router.call(@opts)

      assert conn_b.resp_body == "Bob"
    end

    test "concurrent requests from different clients" do
      tasks =
        Enum.map(1..10, fn i ->
          Task.async(fn ->
            conn =
              conn(:post, "/", "SET key#{i} #{i}")
              |> put_req_header("x-client-name", "Client#{i}")
              |> Cube.Router.call(@opts)

            conn.status
          end)
        end)

      results = Task.await_many(tasks)

      assert Enum.all?(results, &(&1 == 200))
    end
  end

  describe "error handling" do
    test "returns error for invalid command" do
      conn =
        conn(:post, "/", "INVALID command")
        |> put_req_header("x-client-name", "ErrorClient")
        |> Cube.Router.call(@opts)

      assert conn.status == 400
      assert String.starts_with?(conn.resp_body, "ERR")
    end

    test "returns error for malformed SET" do
      conn =
        conn(:post, "/", "SET")
        |> put_req_header("x-client-name", "ErrorClient")
        |> Cube.Router.call(@opts)

      assert conn.status == 400
      assert String.starts_with?(conn.resp_body, "ERR")
    end

    test "returns error for unclosed string" do
      conn =
        conn(:post, "/", "SET key \"unclosed")
        |> put_req_header("x-client-name", "ErrorClient")
        |> Cube.Router.call(@opts)

      assert conn.status == 400
      assert String.contains?(conn.resp_body, "unclosed string")
    end
  end

  describe "special characters and encoding" do
    test "handles Unicode in values" do
      client_name = "UnicodeClient"

      conn(:post, "/", "SET greeting \"你好世界\"")
      |> put_req_header("x-client-name", client_name)
      |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET greeting")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "你好世界"
    end

    test "handles escaped quotes" do
      client_name = "QuoteClient"

      conn(:post, "/", "SET quote \"He said \\\"Hello\\\"\"")
      |> put_req_header("x-client-name", client_name)
      |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET quote")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "He said \"Hello\""
    end

    test "handles newlines in values" do
      client_name = "NewlineClient"

      conn(:post, "/", "SET text \"Line1\\nLine2\"")
      |> put_req_header("x-client-name", client_name)
      |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET text")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "Line1\nLine2"
    end
  end

  describe "persistence across requests" do
    test "data persists across multiple requests" do
      client_name = "PersistentClient"

      conn(:post, "/", "SET counter 1")
      |> put_req_header("x-client-name", client_name)
      |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET counter")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "1"

      conn(:post, "/", "SET counter 2")
      |> put_req_header("x-client-name", client_name)
      |> Cube.Router.call(@opts)

      conn =
        conn(:post, "/", "GET counter")
        |> put_req_header("x-client-name", client_name)
        |> Cube.Router.call(@opts)

      assert conn.resp_body == "2"
    end
  end
end
