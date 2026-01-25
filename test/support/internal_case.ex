defmodule Bolt.Sips.InternalCase do
  use ExUnit.CaseTemplate

  alias Bolt.Sips.Internals.BoltProtocol
  alias Bolt.Sips.Internals.BoltProtocolHelper

  setup do
    uri = neo4j_uri()
    port_opts = [active: false, mode: :binary, packet: :raw]
    {:ok, port} = :gen_tcp.connect(uri.host, uri.port, port_opts)
    {:ok, bolt_version} = BoltProtocol.handshake(:gen_tcp, port)
    {:ok, _} = init(:gen_tcp, port, bolt_version, uri.userinfo)

    on_exit(fn ->
      :gen_tcp.close(port)
    end)

    {:ok, port: port, is_bolt_v2: bolt_version >= 2, bolt_version: bolt_version}
  end

  defp neo4j_uri do
    "bolt://localhost:7687"
    |> URI.merge(System.get_env("NEO4J_TEST_URL") || "")
    |> URI.parse()
    |> Map.update!(:host, &String.to_charlist/1)
    |> Map.update!(:userinfo, fn
      nil ->
        {}

      userinfo ->
        userinfo
        |> String.split(":")
        |> List.to_tuple()
    end)
  end

  # v5.1+ uses HELLO (without auth) + LOGON (with auth)
  defp init(transport, port, {major, minor} = bolt_version, auth)
       when major >= 5 and minor >= 1 do
    with {:ok, _} <- BoltProtocol.hello(transport, port, bolt_version, {}),
         :ok <- do_logon(transport, port, bolt_version, auth) do
      {:ok, %{}}
    end
  end

  # v4+ and v3 use HELLO with auth
  defp init(transport, port, bolt_version, auth) when is_tuple(bolt_version) or bolt_version >= 3 do
    BoltProtocol.hello(transport, port, bolt_version, auth)
  end

  # v1-v2 use INIT
  defp init(transport, port, bolt_version, auth) do
    BoltProtocol.init(transport, port, bolt_version, auth)
  end

  defp do_logon(transport, port, bolt_version, auth) do
    BoltProtocolHelper.send_message(transport, port, bolt_version, {:logon, [auth]})

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, []) do
      {:success, _} -> :ok
      {:failure, response} -> {:error, response}
      other -> {:error, other}
    end
  end
end
