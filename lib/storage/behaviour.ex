defmodule Storage.Behaviour do
  @callback get(key :: String.t()) :: {:ok, any()} | {:error, term()}
  @callback set(key :: String.t(), value :: Parser.Value.t()) :: {:ok, any()} | {:already_exists, any()}
end
