defmodule McpGateway.MixProject do
  use Mix.Project

  def project do
    [
      app: :mcp_gateway,
      version: "0.1.0",
      elixir: ">= 1.15.0",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {McpGateway.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
      {:plug_cowboy, "~> 2.7"},
      {:jason, "~> 1.4"},
      {:amqp, "~> 3.3"},
      {:rabbit_common, "~> 3.13.4", override: true}
    ]
  end
end
