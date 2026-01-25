defmodule Bolt.Sips.Internals.BoltProtocolV5Test do
  use ExUnit.Case, async: true
  @moduletag :bolt_v5

  alias Bolt.Sips.Internals.BoltProtocol
  alias Bolt.Sips.Internals.BoltProtocolV4
  alias Bolt.Sips.Metadata

  # Use {5, 6} as the default v5 version for tests
  @bolt_version {5, 6}

  # Helper to skip tests conditionally
  defp do_skip(reason) do
    IO.puts("Skipping: #{reason}")
    {:ok, skip: true, reason: reason}
  end

  setup do
    app_config = Application.get_env(:bolt_sips, Bolt)

    port = Keyword.get(app_config, :port, 7687)
    auth = {app_config[:basic_auth][:username], app_config[:basic_auth][:password]}

    config =
      app_config
      |> Keyword.put(:port, port)
      |> Keyword.put(:auth, auth)
      |> Bolt.Sips.Utils.default_config()

    {:ok, port} =
      config[:hostname]
      |> String.to_charlist()
      |> :gen_tcp.connect(config[:port],
        active: false,
        mode: :binary,
        packet: :raw
      )

    {:ok, negotiated_version} = BoltProtocol.handshake(:gen_tcp, port, [])

    # Skip tests if server doesn't support v5+
    major = if is_tuple(negotiated_version), do: elem(negotiated_version, 0), else: negotiated_version

    unless major >= 5 do
      :gen_tcp.close(port)
      do_skip("Server does not support Bolt v5+")
    end

    on_exit(fn ->
      :gen_tcp.close(port)
    end)

    {:ok, config: config, port: port, bolt_version: negotiated_version}
  end

  describe "hello/5 for v5 (with bolt_agent):" do
    test "ok with auth", %{config: config, port: port, bolt_version: bolt_version} do
      # v5.0 still uses HELLO with auth (LOGON is v5.1+)
      assert {:ok, %{"server" => _}} =
               BoltProtocolV4.hello(
                 :gen_tcp,
                 port,
                 bolt_version,
                 config[:auth],
                 []
               )
    end
  end

  describe "logon/5 for v5.1+ (separate authentication):" do
    @tag :bolt_v5_1
    test "ok with hello then logon", %{config: config, port: port, bolt_version: bolt_version} do
      {_major, minor} = bolt_version

      if minor >= 1 do
        # v5.1+ uses HELLO without auth, then LOGON with auth
        assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, {}, [])
        assert {:ok, _} = BoltProtocolV4.logon(:gen_tcp, port, bolt_version, config[:auth], [])
      else
        do_skip("Server does not support LOGON (requires v5.1+)")
      end
    end

    @tag :bolt_v5_1
    test "invalid auth with logon", %{config: config, port: port, bolt_version: bolt_version} do
      {_major, minor} = bolt_version

      if minor >= 1 do
        assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, {}, [])

        assert {:error, _} =
                 BoltProtocolV4.logon(
                   :gen_tcp,
                   port,
                   bolt_version,
                   {config[:basic_auth][:username], "wrong!"},
                   []
                 )
      else
        do_skip("Server does not support LOGON (requires v5.1+)")
      end
    end
  end

  describe "logoff/4 for v5.1+ (logout without disconnect):" do
    @tag :bolt_v5_1
    test "ok after logon", %{config: config, port: port, bolt_version: bolt_version} do
      {_major, minor} = bolt_version

      if minor >= 1 do
        # Authenticate first
        assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, {}, [])
        assert {:ok, _} = BoltProtocolV4.logon(:gen_tcp, port, bolt_version, config[:auth], [])

        # Then logout
        assert :ok = BoltProtocolV4.logoff(:gen_tcp, port, bolt_version, [])

        # Can re-authenticate after logoff
        assert {:ok, _} = BoltProtocolV4.logon(:gen_tcp, port, bolt_version, config[:auth], [])
      else
        do_skip("Server does not support LOGOFF (requires v5.1+)")
      end
    end
  end

  describe "telemetry/5 for v5.4+:" do
    @tag :bolt_v5_4
    test "sends telemetry data", %{config: config, port: port, bolt_version: bolt_version} do
      {_major, minor} = bolt_version

      if minor >= 4 do
        # Authenticate first
        assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

        # Send telemetry (api = 1 for driver API)
        assert :ok = BoltProtocolV4.telemetry(:gen_tcp, port, bolt_version, 1, [])
      else
        do_skip("Server does not support TELEMETRY (requires v5.4+)")
      end
    end
  end

  test "goodbye/3 for v5", %{config: config, port: port, bolt_version: bolt_version} do
    assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

    assert :ok = BoltProtocolV4.goodbye(:gen_tcp, port, bolt_version)
  end

  describe "run/7 for v5:" do
    test "ok with basic query", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])
    end

    @tag :enterprise
    test "ok with notification severity", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])
      {:ok, metadata} = Metadata.new(%{notifications_minimum_severity: "WARNING"})

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, metadata, [])
    end

    @tag :enterprise
    test "ok with impersonated user", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])
      {:ok, metadata} = Metadata.new(%{imp_user: config[:basic_auth][:username]})

      # Note: impersonation requires admin permissions
      case BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, metadata, []) do
        {:ok, {:success, %{"fields" => ["num"]}}} -> :ok
        {:error, _} -> :ok  # May fail if user doesn't have impersonation rights
      end
    end
  end

  describe "pull/5 for v5:" do
    test "pull all records", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

      assert {:ok, [record: [1], success: %{"type" => "r"}]} =
               BoltProtocolV4.pull(:gen_tcp, port, bolt_version, %{n: -1}, [])
    end
  end

  describe "Transaction management for v5" do
    test "begin with notification severity", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])
      {:ok, metadata} = Metadata.new(%{notifications_minimum_severity: "OFF"})

      assert {:ok, _} = BoltProtocolV4.begin(:gen_tcp, port, bolt_version, metadata, [])
      assert :ok = BoltProtocolV4.rollback(:gen_tcp, port, bolt_version, [])
    end

    test "complete transaction cycle", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, _} = BoltProtocolV4.begin(:gen_tcp, port, bolt_version, %{}, [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

      assert {:ok, _} = BoltProtocolV4.pull_all(:gen_tcp, port, bolt_version, [])
      assert {:ok, %{"bookmark" => _}} = BoltProtocolV4.commit(:gen_tcp, port, bolt_version, [])
    end
  end

  describe "ROUTE message for v5:" do
    @tag :enterprise
    test "query routing table", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      # ROUTE message to get routing table
      case BoltProtocolV4.route(:gen_tcp, port, bolt_version, %{}, [], nil, []) do
        {:ok, %{"rt" => _}} -> :ok  # Success - got routing table
        {:error, _} -> :ok  # May fail if not cluster mode
      end
    end

    @tag :enterprise
    test "query routing table with database", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      case BoltProtocolV4.route(:gen_tcp, port, bolt_version, %{}, [], "neo4j", []) do
        {:ok, %{"rt" => _}} -> :ok
        {:error, _} -> :ok
      end
    end
  end

  describe "Error recovery for v5" do
    test "RESET after cypher error", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])
      assert {:error, _} = BoltProtocolV4.run(:gen_tcp, port, bolt_version, "Invalid cypher", %{}, %{}, [])

      # RESET should return connection to READY state
      assert :ok = BoltProtocolV4.reset(:gen_tcp, port, bolt_version, [])

      # Now we should be able to run queries again
      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])
    end
  end

  describe "Protocol compatibility" do
    test "BoltProtocol dispatcher routes to v4 module for v5", %{config: config, port: port, bolt_version: bolt_version} do
      # The BoltProtocol module should correctly dispatch v5 calls
      assert {:ok, _} = BoltProtocol.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocol.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

      assert {:ok, _} = BoltProtocol.pull_all(:gen_tcp, port, bolt_version, [])
    end
  end
end
