defmodule McpGatewayTest do
  use ExUnit.Case
  doctest McpGateway

  test "greets the world" do
    assert McpGateway.hello() == :world
  end
end
