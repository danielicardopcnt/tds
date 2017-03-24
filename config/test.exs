use Mix.Config

config :logger, level: :info

config :mssql,
  opts: [
    hostname: "mssql.local",
    username: "mssql",
    password: "mssql",
    database: "tds_test",
  ]
