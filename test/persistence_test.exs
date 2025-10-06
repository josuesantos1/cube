defmodule PersistenceTest do
  use ExUnit.Case, async: false

  setup do
    shard = "test_persist_#{:rand.uniform(1_000_000)}"
    on_exit(fn -> cleanup(shard) end)
    {:ok, shard: shard}
  end

  defp cleanup(shard) do
    file_path = shard <> "_data.txt"
    if File.exists?(file_path), do: File.rm!(file_path)
  end

  describe "write/2" do
    test "creates new file and writes command", %{shard: shard} do
      command = "SET key \"value\"\n"
      assert :ok = Persistence.write(shard, command)
      assert File.exists?(shard <> "_data.txt")
      assert File.read!(shard <> "_data.txt") == command
    end

    test "appends to existing file", %{shard: shard} do
      Persistence.write(shard, "first\n")
      Persistence.write(shard, "second\n")
      content = File.read!(shard <> "_data.txt")
      assert content == "first\nsecond\n"
    end

    test "handles multiple appends", %{shard: shard} do
      commands = ["cmd1\n", "cmd2\n", "cmd3\n"]
      Enum.each(commands, &Persistence.write(shard, &1))
      content = File.read!(shard <> "_data.txt")
      assert content == "cmd1\ncmd2\ncmd3\n"
    end

    test "handles empty command", %{shard: shard} do
      assert :ok = Persistence.write(shard, "")
      assert File.read!(shard <> "_data.txt") == ""
    end
  end

  describe "update_or_append/3" do
    test "creates new file if not exists", %{shard: shard} do
      command = "SET key \"value\""
      key_prefix = "SET key"
      assert :ok = Persistence.update_or_append(shard, command, key_prefix)
      assert File.exists?(shard <> "_data.txt")
    end

    test "appends new key if not found", %{shard: shard} do
      Persistence.update_or_append(shard, "SET key1 \"val1\"", "SET key1")
      Persistence.update_or_append(shard, "SET key2 \"val2\"", "SET key2")

      content = File.read!(shard <> "_data.txt")
      assert String.contains?(content, "SET key1 \"val1\"")
      assert String.contains?(content, "SET key2 \"val2\"")
    end

    test "updates existing key", %{shard: shard} do
      Persistence.update_or_append(shard, "SET name \"Alice\"", "SET name")
      Persistence.update_or_append(shard, "SET name \"Bob\"", "SET name")

      content = File.read!(shard <> "_data.txt")
      refute String.contains?(content, "Alice")
      assert String.contains?(content, "Bob")
    end

    test "updates only first occurrence", %{shard: shard} do
      File.write!(shard <> "_data.txt", "SET key \"v1\"\nSET key \"v2\"\n")
      Persistence.update_or_append(shard, "SET key \"v3\"", "SET key")

      lines = File.read!(shard <> "_data.txt") |> String.split("\n", trim: true)
      assert Enum.count(lines, &String.contains?(&1, "v3")) == 1
      assert Enum.count(lines, &String.contains?(&1, "v2")) == 1
    end

    test "preserves other keys when updating", %{shard: shard} do
      Persistence.update_or_append(shard, "SET key1 \"val1\"", "SET key1")
      Persistence.update_or_append(shard, "SET key2 \"val2\"", "SET key2")
      Persistence.update_or_append(shard, "SET key1 \"updated\"", "SET key1")

      content = File.read!(shard <> "_data.txt")
      assert String.contains?(content, "SET key1 \"updated\"")
      assert String.contains?(content, "SET key2 \"val2\"")
      refute String.contains?(content, "val1")
    end

    test "handles prefix matching correctly", %{shard: shard} do
      Persistence.update_or_append(shard, "SET key \"val\"", "SET key ")
      Persistence.update_or_append(shard, "SET key2 \"val2\"", "SET key2")

      content = File.read!(shard <> "_data.txt")
      lines = String.split(content, "\n", trim: true)
      assert length(lines) == 2
    end

    test "trims command before writing", %{shard: shard} do
      command = "  SET key \"value\"  "
      Persistence.update_or_append(shard, command, "SET key")

      content = File.read!(shard <> "_data.txt")
      assert String.trim(content) == "SET key \"value\""
    end

    test "adds newline at end of file when updating", %{shard: shard} do
      File.write!(shard <> "_data.txt", "SET old \"value\"\n")
      Persistence.update_or_append(shard, "SET key \"val\"", "SET key")
      content = File.read!(shard <> "_data.txt")
      assert String.ends_with?(content, "\n")
    end
  end

  describe "read_line_by_prefix/2" do
    test "returns nil for non-existent file", %{shard: shard} do
      assert nil == Persistence.read_line_by_prefix(shard, "SET key")
    end

    test "returns nil when prefix not found", %{shard: shard} do
      File.write!(shard <> "_data.txt", "SET key1 \"val1\"\n")
      assert nil == Persistence.read_line_by_prefix(shard, "SET key2")
    end

    test "returns matching line", %{shard: shard} do
      File.write!(shard <> "_data.txt", "SET key \"value\"\n")
      result = Persistence.read_line_by_prefix(shard, "SET key")
      assert result == "SET key \"value\""
    end

    test "returns last matching line when multiple exist", %{shard: shard} do
      content = "SET key \"v1\"\nSET key \"v2\"\nSET key \"v3\"\n"
      File.write!(shard <> "_data.txt", content)
      result = Persistence.read_line_by_prefix(shard, "SET key")
      assert result == "SET key \"v3\""
    end

    test "trims whitespace from result", %{shard: shard} do
      File.write!(shard <> "_data.txt", "SET key \"val\"  \n")
      result = Persistence.read_line_by_prefix(shard, "SET key")
      assert result == "SET key \"val\""
    end

    test "matches prefix correctly", %{shard: shard} do
      content = "SET key1 \"v1\"\nSET key2 \"v2\"\n"
      File.write!(shard <> "_data.txt", content)

      assert Persistence.read_line_by_prefix(shard, "SET key1") == "SET key1 \"v1\""
      assert Persistence.read_line_by_prefix(shard, "SET key2") == "SET key2 \"v2\""
    end

    test "handles empty lines in file", %{shard: shard} do
      content = "SET key1 \"v1\"\n\n\nSET key2 \"v2\"\n"
      File.write!(shard <> "_data.txt", content)
      result = Persistence.read_line_by_prefix(shard, "SET key2")
      assert result == "SET key2 \"v2\""
    end
  end

  describe "stream_lines/1" do
    test "returns empty list for non-existent file", %{shard: shard} do
      stream = Persistence.stream_lines(shard)
      assert Enum.to_list(stream) == []
    end

    test "returns stream of lines", %{shard: shard} do
      content = "line1\nline2\nline3\n"
      File.write!(shard <> "_data.txt", content)

      lines = Persistence.stream_lines(shard) |> Enum.to_list()
      assert length(lines) == 3
      assert Enum.at(lines, 0) == "line1\n"
      assert Enum.at(lines, 1) == "line2\n"
      assert Enum.at(lines, 2) == "line3\n"
    end

    test "can be processed lazily", %{shard: shard} do
      content = Enum.map(1..100, &"line#{&1}\n") |> Enum.join()
      File.write!(shard <> "_data.txt", content)

      first_5 = Persistence.stream_lines(shard) |> Enum.take(5)
      assert length(first_5) == 5
      assert Enum.at(first_5, 0) == "line1\n"
    end

    test "handles empty file", %{shard: shard} do
      File.write!(shard <> "_data.txt", "")
      lines = Persistence.stream_lines(shard) |> Enum.to_list()
      assert lines == []
    end
  end

  describe "exists?/1" do
    test "returns false for non-existent file", %{shard: shard} do
      refute Persistence.exists?(shard)
    end

    test "returns true for existing file", %{shard: shard} do
      File.write!(shard <> "_data.txt", "content")
      assert Persistence.exists?(shard)
    end

    test "returns true for empty file", %{shard: shard} do
      File.write!(shard <> "_data.txt", "")
      assert Persistence.exists?(shard)
    end
  end

  describe "build_path/1 (via other functions)" do
    test "creates correct file path format", %{shard: shard} do
      Persistence.write(shard, "test")
      assert File.exists?(shard <> "_data.txt")
    end

    test "uses shard name in path", %{shard: _shard} do
      custom_shard = "custom_test_123"
      Persistence.write(custom_shard, "data")
      on_exit(fn -> File.rm(custom_shard <> "_data.txt") end)
      assert File.exists?("custom_test_123_data.txt")
    end
  end

  describe "integration scenarios" do
    test "write then read workflow", %{shard: shard} do
      command = "SET mykey \"myvalue\"\n"
      Persistence.write(shard, command)
      result = Persistence.read_line_by_prefix(shard, "SET mykey")
      assert String.trim(result) == String.trim(command)
    end

    test "update workflow maintains consistency", %{shard: shard} do
      Persistence.update_or_append(shard, "SET count 1", "SET count")
      Persistence.update_or_append(shard, "SET count 2", "SET count")
      Persistence.update_or_append(shard, "SET count 3", "SET count")

      result = Persistence.read_line_by_prefix(shard, "SET count")
      assert result == "SET count 3"

      lines = File.read!(shard <> "_data.txt") |> String.split("\n", trim: true)
      assert length(lines) == 1
    end

    test "multiple keys workflow", %{shard: shard} do
      Persistence.update_or_append(shard, "SET user \"Alice\"", "SET user")
      Persistence.update_or_append(shard, "SET age 30", "SET age")
      Persistence.update_or_append(shard, "SET city \"NYC\"", "SET city")

      assert Persistence.read_line_by_prefix(shard, "SET user") == "SET user \"Alice\""
      assert Persistence.read_line_by_prefix(shard, "SET age") == "SET age 30"
      assert Persistence.read_line_by_prefix(shard, "SET city") == "SET city \"NYC\""
    end

    test "stream and update workflow", %{shard: shard} do
      Persistence.write(shard, "line1\n")
      Persistence.write(shard, "line2\n")

      lines = Persistence.stream_lines(shard) |> Enum.map(&String.trim/1)
      assert lines == ["line1", "line2"]

      Persistence.update_or_append(shard, "line1_updated", "line1")
      updated = Persistence.read_line_by_prefix(shard, "line1")
      assert updated == "line1_updated"
    end

    test "handles rapid updates", %{shard: shard} do
      for i <- 1..50 do
        Persistence.update_or_append(shard, "SET counter #{i}", "SET counter")
      end

      result = Persistence.read_line_by_prefix(shard, "SET counter")
      assert result == "SET counter 50"

      lines = File.read!(shard <> "_data.txt") |> String.split("\n", trim: true)
      assert length(lines) == 1
    end
  end

  describe "edge cases" do
    test "handles very long commands", %{shard: shard} do
      long_value = String.duplicate("x", 10_000)
      command = "SET key \"#{long_value}\""
      Persistence.update_or_append(shard, command, "SET key")
      result = Persistence.read_line_by_prefix(shard, "SET key")
      assert String.contains?(result, long_value)
    end

    test "handles special characters in commands", %{shard: shard} do
      command = "SET key \"Hello\\nWorld\\t!\""
      Persistence.update_or_append(shard, command, "SET key")
      result = Persistence.read_line_by_prefix(shard, "SET key")
      assert result == command
    end

    test "handles Unicode in commands", %{shard: shard} do
      command = "SET value \"测试 🎉\""
      Persistence.update_or_append(shard, command, "SET value")
      result = Persistence.read_line_by_prefix(shard, "SET value")
      assert result == command
    end

    test "handles empty prefix search", %{shard: shard} do
      File.write!(shard <> "_data.txt", "first\nsecond\n")
      result = Persistence.read_line_by_prefix(shard, "")
      assert result == "second"
    end

    test "handles shard names with special characters", %{shard: _shard} do
      special_shard = "shard_01_client@123"
      Persistence.write(special_shard, "data\n")
      on_exit(fn -> File.rm(special_shard <> "_data.txt") end)
      assert Persistence.exists?(special_shard)
    end
  end
end
