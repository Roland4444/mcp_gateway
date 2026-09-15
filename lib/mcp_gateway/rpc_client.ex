defmodule McpGateway.RpcClient do
  @moduledoc "Публикует MCP-запрос в RabbitMQ и ждёт ответ."

  require Logger

  def call(payload) do
    amqp_url = Application.get_env(:mcp_gateway, :amqp_url)
    queue = Application.get_env(:mcp_gateway, :request_queue)
    timeout = Application.get_env(:mcp_gateway, :rpc_timeout_ms, 120_000)

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

  defp do_rpc(chan, queue, payload, timeout) do
    {:ok, %{queue: reply_queue}} = AMQP.Queue.declare(chan, "", exclusive: true)

    correlation_id = :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
    payload_json = Jason.encode!(payload)

    Logger.debug("Publishing to #{queue} (corr_id=#{correlation_id})")

    AMQP.Basic.publish(chan, "", queue, payload_json,
      reply_to: reply_queue,
      correlation_id: correlation_id,
      content_type: "application/json",
      delivery_mode: 2
    )

    AMQP.Basic.consume(chan, reply_queue, nil, no_ack: true)

    receive do
      {:basic_deliver, body, %{correlation_id: ^correlation_id}} ->
        Logger.debug("Got reply (corr_id=#{correlation_id})")

        case Jason.decode(body) do
          {:ok, decoded} -> {:ok, decoded}
          {:error, err} -> {:error, {:invalid_json, err, body}}
        end

      {:basic_deliver, _body, _meta} ->
        do_rpc_wait(chan, timeout, correlation_id)
    after
      timeout ->
        Logger.error("RPC timeout after #{timeout} ms (corr_id=#{correlation_id})")
        {:error, :timeout}
    end
  end

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

  defp ssl_opts do
    [
      verify: :verify_none,
      versions: [:"tlsv1.2", :"tlsv1.3"]
    ]
  end
end
