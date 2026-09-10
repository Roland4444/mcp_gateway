defmodule McpGateway.RpcClient do
  @moduledoc """
  RPC-клиент для MCP-шлюза.

  Публикует JSON-RPC запрос в очередь RabbitMQ `mcp_requests`
  и ждёт ответ в эксклюзивной очереди ответа. Ответ сопоставляется
  с исходным запросом по `correlation_id`.

  Использует AMQPS (TLS) для подключения к RabbitMQ.
  """

  require Logger

  @default_timeout 30_000

  @doc """
  Отправляет запрос `payload` (map) в RabbitMQ и ждёт ответ.

  Возвращает:
    * `{:ok, response_map}` при успехе
    * `{:error, reason}` при ошибке соединения, таймауте или невалидном JSON
  """
  def call(payload) do
    amqp_url = Application.get_env(:mcp_gateway, :amqp_url)
    queue = Application.get_env(:mcp_gateway, :request_queue)
    timeout = Application.get_env(:mcp_gateway, :rpc_timeout_ms, @default_timeout)

    unless amqp_url do
      raise "Не задан :amqp_url в config/config.exs"
    end

    unless queue do
      raise "Не задан :request_queue в config/config.exs"
    end

    case AMQP.Connection.open(amqp_url, ssl_options: ssl_opts()) do
      {:ok, conn} ->
        try do
          {:ok, chan} = AMQP.Channel.open(conn)
          do_rpc(chan, queue, payload, timeout)
        after
          AMQP.Connection.close(conn)
        end

      {:error, reason} ->
        Logger.error("RabbitMQ connection failed: #{inspect(reason)}")
        {:error, {:connection_failed, reason}}
    end
  end

  # ------------------------------------------------------------------
  # Внутренние функции
  # ------------------------------------------------------------------

  defp do_rpc(chan, queue, payload, timeout) do
    # Эксклюзивная временная очередь для ответа.
    # Она удалится автоматически при закрытии соединения.
    {:ok, %{queue: reply_queue}} = AMQP.Queue.declare(chan, "", exclusive: true)

    # Уникальный ID для сопоставления запроса и ответа.
    correlation_id = generate_correlation_id()

    payload_json = Jason.encode!(payload)

    Logger.debug("Publishing to #{queue} (corr_id=#{correlation_id})")

    AMQP.Basic.publish(chan, "", queue, payload_json,
      reply_to: reply_queue,
      correlation_id: correlation_id,
      content_type: "application/json",
      delivery_mode: 2
    )

    # Слушаем очередь ответа.
    AMQP.Basic.consume(chan, reply_queue, nil, no_ack: true)

    receive do
      {:basic_deliver, body, %{correlation_id: ^correlation_id}} ->
        Logger.debug("Got reply (corr_id=#{correlation_id})")

        case Jason.decode(body) do
          {:ok, decoded} ->
            {:ok, decoded}

          {:error, err} ->
            Logger.error("Invalid JSON in reply: #{inspect(err)}; body=#{body}")
            {:error, {:invalid_json, err, body}}
        end

      # Ответ с другим correlation_id — игнорируем, ждём свой.
      {:basic_deliver, _body, _meta} ->
        do_rpc_wait(chan, timeout, correlation_id)
    after
      timeout ->
        Logger.error("RPC timeout after #{timeout} ms (corr_id=#{correlation_id})")
        {:error, :timeout}
    end
  end

  # Продолжение ожидания — на случай, если пришёл чужой ответ.
  defp do_rpc_wait(_chan, timeout, correlation_id) do
    receive do
      {:basic_deliver, body, %{correlation_id: ^correlation_id}} ->
        case Jason.decode(body) do
          {:ok, decoded} -> {:ok, decoded}
          {:error, err} -> {:error, {:invalid_json, err, body}}
        end

      {:basic_deliver, _body, _meta} ->
        do_rpc_wait(nil, timeout, correlation_id)
    after
      timeout ->
        {:error, :timeout}
    end
  end

  # 16 случайных байт → hex (32 символа) — почти гарантированно уникально.
  defp generate_correlation_id do
    :crypto.strong_rand_bytes(16)
    |> Base.encode16(case: :lower)
  end

  # TLS-опции для AMQPS.
  #
  # verify: :verify_none — отключает проверку сертификата (аналог `:insecure t`
  # в Lisp/Dexador). Для production замените на :verify_peer с указанием
  # cacertfile: "/etc/ssl/certs/ca-certificates.crt" и
  # server_name_indication: 'romach.space'.
  defp ssl_opts do
    [
      verify: :verify_none,
      versions: [:"tlsv1.2", :"tlsv1.3"]
    ]
  end
end
