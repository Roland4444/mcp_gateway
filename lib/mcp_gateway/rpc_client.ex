defmodule McpGateway.RpcClient do
  @moduledoc "CREATE MCP req in Rabbit and wait answer"
  require Logger

  def call(payload) do
    config = Application.get_env(:mcp_gateway, __MODULE__, [])
    amqp_url =  Keyword.fetch!(config, :amqp_url)
    queue = Keyword.fetch!(config, :request_queue)
    timeout = Keyword.get(config, :rpc_timeout_ms, 30_000)

    case AMQP.Connection.open(amqp_url, ssl_options: ssl_opts()) do
      {:ok, conn}  ->
        try do
          {:ok, chan} = AMQP.Channel.open(conn)
          do_rpc(chan, queue, payload, timeout)
        after
          AMQP.Connection.close(conn)
        end

        {:error, reason} -> {:error, {:connection_failed, reason}}
    end
  end

  defp do_rpc(chan, queue, payload, timeout) do

    {:ok, %{queue: reply_queue}} = AMQP.Queue.declare(chan, "", exclusive: true)
    connection_id =  :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
    payload_json = Jason.encode!(payload)

     AMQP.Basic.publish(chan, "", queue, payload_json,
      reply_to: reply_queue,
      correlation_id: correlation_id,
      content_type: "application/json",
      delivery_mode: 2
    )

    AMQP.Basic.consume(chan, reply_queue, nil, no_ack: true)

     receive do
      {:basic_deliver, body, %{correlation_id: ^correlation_id}} ->
        case Jason.decode(body) do
          {:ok, decoded} -> {:ok, decoded}
          {:error, err} -> {:error, {:invalid_json, err, body}}
        end
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


