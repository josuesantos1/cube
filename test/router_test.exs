defmodule Cube.RouterTest do
  use ExUnit.Case, async: true
  import Plug.Test

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
end
