defmodule ParserTest do
  use ExUnit.Case

  describe "parse/1" do
    test "parses SET command with string value" do
      assert {:ok, %{command: :set, key: "nome", value: %Parser.Value{type: :string, value: "João"}}} =
               Parser.Parser.parse("SET nome \"João\"")
    end

    test "parses SET command with integer value" do
      assert {:ok, %{command: :set, key: "idade", value: %Parser.Value{type: :integer, value: 25}}} =
               Parser.Parser.parse("SET idade 25")
    end

    test "parses SET command with negative integer" do
      assert {:ok, %{command: :set, key: "temperatura", value: %Parser.Value{type: :integer, value: -10}}} =
               Parser.Parser.parse("SET temperatura -10")
    end

    test "parses SET command with boolean true" do
      assert {:ok, %{command: :set, key: "ativo", value: %Parser.Value{type: :boolean, value: true}}} =
               Parser.Parser.parse("SET ativo true")
    end

    test "parses SET command with boolean false" do
      assert {:ok, %{command: :set, key: "inativo", value: %Parser.Value{type: :boolean, value: false}}} =
               Parser.Parser.parse("SET inativo false")
    end

    test "parses SET command with nil value" do
      assert {:ok, %{command: :set, key: "vazio", value: %Parser.Value{type: :nil, value: nil}}} =
               Parser.Parser.parse("SET vazio nil")
    end

    test "parses SET command with escaped quotes in string" do
      assert {:ok, %{command: :set, key: "texto", value: %Parser.Value{type: :string, value: "Disse \"olá\""}}} =
               Parser.Parser.parse("SET texto \"Disse \\\"olá\\\"\"")
    end

    test "parses SET command with escaped backslash" do
      assert {:ok, %{command: :set, key: "caminho", value: %Parser.Value{type: :string, value: "C:\\Users"}}} =
               Parser.Parser.parse("SET caminho \"C:\\\\Users\"")
    end

    test "parses SET command with newline escape" do
      assert {:ok, %{command: :set, key: "multilinhas", value: %Parser.Value{type: :string, value: "linha1\nlinha2"}}} =
               Parser.Parser.parse("SET multilinhas \"linha1\\nlinha2\"")
    end

    test "parses SET command with tab escape" do
      assert {:ok, %{command: :set, key: "tabulado", value: %Parser.Value{type: :string, value: "col1\tcol2"}}} =
               Parser.Parser.parse("SET tabulado \"col1\\tcol2\"")
    end

    test "handles extra whitespace" do
      assert {:ok, %{command: :set, key: "chave", value: %Parser.Value{type: :string, value: "valor"}}} =
               Parser.Parser.parse("  SET   chave   \"valor\"  ")
    end

    test "returns error for invalid key" do
      assert {:error, "invalid key - must be simple string (e.g. ABC, my_key)"} =
               Parser.Parser.parse("SET 123invalid \"valor\"")
    end

    test "returns error for unclosed string" do
      assert {:error, "unclosed string - missing closing quote"} =
               Parser.Parser.parse("SET chave \"valor sem fechar")
    end

    test "returns error for invalid value" do
      assert {:error, "invalid value - expected integer, string, boolean or nil"} =
               Parser.Parser.parse("SET chave invalid_value")
    end

    test "returns error for unknown command" do
      assert {:error, "unknown command"} =
               Parser.Parser.parse("INVALID chave")
    end
  end

  describe "GET command" do
    test "parses GET command with simple key" do
      assert {:ok, %{command: :get, key: "nome"}} =
               Parser.Parser.parse("GET nome")
    end

    test "parses GET command with underscore in key" do
      assert {:ok, %{command: :get, key: "user_name"}} =
               Parser.Parser.parse("GET user_name")
    end

    test "parses GET command with numbers in key" do
      assert {:ok, %{command: :get, key: "key123"}} =
               Parser.Parser.parse("GET key123")
    end

    test "handles extra whitespace in GET" do
      assert {:ok, %{command: :get, key: "chave"}} =
               Parser.Parser.parse("  GET   chave  ")
    end

    test "returns error for GET with invalid key" do
      assert {:error, "invalid key - must be simple string (e.g. ABC, my_key)"} =
               Parser.Parser.parse("GET 123invalid")
    end

    test "returns error for GET without key" do
      assert {:error, _} =
               Parser.Parser.parse("GET")
    end
  end

  describe "Transaction commands" do
    test "parses BEGIN command" do
      assert {:ok, %{command: :begin}} = Parser.Parser.parse("BEGIN")
    end

    test "parses BEGIN with extra whitespace" do
      assert {:ok, %{command: :begin}} = Parser.Parser.parse("  BEGIN  ")
    end

    test "rejects BEGIN with arguments" do
      assert {:error, "BEGIN command does not accept arguments"} =
               Parser.Parser.parse("BEGIN extra")
    end

    test "parses COMMIT command" do
      assert {:ok, %{command: :commit}} = Parser.Parser.parse("COMMIT")
    end

    test "parses COMMIT with extra whitespace" do
      assert {:ok, %{command: :commit}} = Parser.Parser.parse("  COMMIT  ")
    end

    test "rejects COMMIT with arguments" do
      assert {:error, "COMMIT command does not accept arguments"} =
               Parser.Parser.parse("COMMIT extra")
    end

    test "parses ROLLBACK command" do
      assert {:ok, %{command: :rollback}} = Parser.Parser.parse("ROLLBACK")
    end

    test "parses ROLLBACK with extra whitespace" do
      assert {:ok, %{command: :rollback}} = Parser.Parser.parse("  ROLLBACK  ")
    end

    test "rejects ROLLBACK with arguments" do
      assert {:error, "ROLLBACK command does not accept arguments"} =
               Parser.Parser.parse("ROLLBACK extra")
    end
  end
end
