defmodule McpGateway.Router do
  use Plug.Router

  require Logger

  plug Plug.Parsers,
    parsers: [:json],
    pass: ["application/json"],
    json_decoder: Jason

  plug :match
  plug :dispatch

  # ------------------------------------------------------------------
  # POST /mcp — единственный MCP-эндпоинт
  # ------------------------------------------------------------------
  post "/mcp" do
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

  # ------------------------------------------------------------------
  # GET /blob/:token — скачивание файлов
  # ------------------------------------------------------------------
  get "/blob/:token" do
    token = conn.path_params["token"]
    Logger.info("Blob request: #{token}")

    case McpGateway.RpcClient.call(%{
           "jsonrpc" => "2.0",
           "id" => System.unique_integer([:positive]),
           "method" => "read_file_blob",
           "params" => %{"token" => token}
         }) do
      {:ok, %{"result" => %{"data" => b64, "path" => path}}} ->
        case Base.decode64(b64) do
          {:ok, bytes} ->
            filename = Path.basename(path)

            conn
            |> put_resp_header("content-type", "application/octet-stream")
            |> put_resp_header("content-disposition",
                 ~s(attachment; filename="#{filename}"))
            |> put_resp_header("content-length", to_string(byte_size(bytes)))
            |> put_resp_header("x-accel-buffering", "no")
            |> send_resp(200, bytes)

          :error ->
            send_resp(conn, 500, "Invalid base64 from worker")
        end

      {:ok, %{"error" => err}} ->
        conn
        |> put_resp_header("content-type", "application/json")
        |> send_resp(404, Jason.encode!(err))

      {:error, reason} ->
        Logger.error("Blob RPC error: #{inspect(reason)}")
        send_resp(conn, 502, "RPC error: #{inspect(reason)}")
    end
  end

  # ------------------------------------------------------------------
  # Healthcheck
  # ------------------------------------------------------------------
  get "/health" do
    send_resp(conn, 200, "OK")
  end

  # ------------------------------------------------------------------
  # Fallback
  # ------------------------------------------------------------------
  match _ do
    send_resp(conn, 404, "Not Found")
  end
end
