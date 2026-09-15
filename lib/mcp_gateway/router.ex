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
  # POST /mcp — основной обработчик JSON-RPC
  # ------------------------------------------------------------------
post "/mcp" do
  Logger.info("MCP POST: #{inspect(conn.body_params)}")

  case McpGateway.RpcClient.call(conn.body_params) do
    {:ok, response} ->
      body = Jason.encode!(response)

      conn
      |> put_resp_header("content-type", "application/json")   # без charset
      |> delete_resp_header("cache-control")                    # убираем кэш
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
          |> put_resp_content_type("application/octet-stream")
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
      |> put_resp_content_type("application/json")
      |> send_resp(404, Jason.encode!(err))

    {:error, reason} ->
      Logger.error("Blob RPC error: #{inspect(reason)}")
      send_resp(conn, 502, "RPC error: #{inspect(reason)}")
  end
end


  # ------------------------------------------------------------------
  # GET /mcp — SSE-эндпоинт для долгоживущих соединений
  # ------------------------------------------------------------------
  get "/mcp" do
    Logger.info("MCP GET (SSE stream opened)")

    conn
    |> put_resp_content_type("text/event-stream")
    |> put_resp_header("cache-control", "no-cache")
    |> put_resp_header("connection", "keep-alive")
    |> put_resp_header("x-accel-buffering", "no")
    |> send_chunked(200)
    |> sse_heartbeat_loop()
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

  # ==================================================================
  # Приватные функции
  # ==================================================================

  # Отправляет одно SSE-событие и закрывает поток.
  defp send_sse_event(conn, json_body) do
    case chunk(conn, "data: #{json_body}\n\n") do
      {:ok, conn} ->
        # Закрываем поток после отправки одного события
        # (Streamable HTTP допускает завершение после ответа)
        conn

      {:error, _reason} ->
        conn
    end
  end

  # Держит GET-соединение открытым, шлёт heartbeat каждые 15 секунд.
  # Это нужно, чтобы Context Forge видел активный SSE-канал.
  defp sse_heartbeat_loop(conn) do
    receive do
      :stop -> conn
    after
      15_000 ->
        case chunk(conn, ": heartbeat\n\n") do
          {:ok, conn} -> sse_heartbeat_loop(conn)
          {:error, _reason} -> conn
        end
    end
  end
end
