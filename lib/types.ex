defmodule Types do
  types = %{
    "0" => :string,
    "1" => :integer,
    "2" => :float,
    "3" => :boolean,
    "4" => :nil,
  }

  def get_type(code) do
    Map.get(unquote(Macro.escape(types)), code, :unknown)
  end
end
