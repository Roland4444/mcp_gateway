defmodule McpGateway.Router do
  use Plug.Router

  require Logger

  plug Plug.Parsers,
    parsers: [:json],
    pass: ["application/json"],
    json_decoder: Jason

  plug :match
  plug :dispatch

  post "/mcp2" do
    Logger.info("MCP POST: #{inspect(conn.body_params)}")

    case McpGateway.RpcClient.call(conn.body_params) do
      {:ok, response} ->
        body = Jason.encode!(response)

        conn
        |> put_resp_header("content-type", "application/json")
        |> delete_resp_header("cache-control")
        |> send_resp(200, body)

      {:error, reason} ->
        Logger.error("RPC error: #{inspect(reason)}")

        error_body =
          Jason.encode!(%{
            jsonrpc: "2.0",
            id: conn.body_params["id"],
            error: %{code: -32000, message: "RPC error: #{inspect(reason)}"}
          })

        conn
        |> put_resp_header("content-type", "application/json")
        |> delete_resp_header("cache-control")
        |> send_resp(500, error_body)
    end
  end

  get "/health" do
    send_resp(conn, 200, "OK")
  end

  match _ do
    send_resp(conn, 404, "Not Found")
  end
end
