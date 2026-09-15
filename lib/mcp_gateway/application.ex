#  simple

defmodule McpGateway.Application do
  @moduledoc false
  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    port = Application.get_env(:mcp_gateway, :http_port, 4000)

    children = [
      {Plug.Cowboy,
       scheme: :http,
       plug: McpGateway.Router,
       options: [port: port]}
    ]

    Logger.info("MCP Gateway starting on port #{port}")

    opts = [strategy: :one_for_one, name: McpGateway.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
