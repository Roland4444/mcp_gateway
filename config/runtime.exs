import Config

config_file = System.get_env("MCP_GATEWAY_CONFIG") || "mcp_gateway.json"

unless File.exists?(config_file) do
  raise """
  Файл конфигурации не найден: #{config_file}

  Создайте его или укажите путь через переменную окружения MCP_GATEWAY_CONFIG.
  """
end

settings =
  config_file
  |> File.read!()
  |> Jason.decode!()
  |> Enum.map(fn {k, v} -> {String.to_atom(k), v} end)

config :mcp_gateway, settings
