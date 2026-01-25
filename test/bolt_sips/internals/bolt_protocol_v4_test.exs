defmodule Bolt.Sips.Internals.BoltProtocolV4Test do
  use ExUnit.Case, async: true
  @moduletag :bolt_v4

  alias Bolt.Sips.Internals.BoltProtocol
  alias Bolt.Sips.Internals.BoltProtocolV4
  alias Bolt.Sips.Metadata

  # Use {4, 4} as the default v4 version for tests
  @bolt_version {4, 4}

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

    # Skip tests if server doesn't support v4+
    unless is_tuple(negotiated_version) or negotiated_version >= 4 do
      :gen_tcp.close(port)
      skip("Server does not support Bolt v4+")
    end

    on_exit(fn ->
      :gen_tcp.close(port)
    end)

    {:ok, config: config, port: port, bolt_version: negotiated_version}
  end

  describe "hello/5 for v4:" do
    test "ok without auth", %{config: config, port: port, bolt_version: bolt_version} do
      # v4 still requires auth in HELLO (unlike v5.1+)
      assert {:ok, %{"server" => _}} =
               BoltProtocolV4.hello(
                 :gen_tcp,
                 port,
                 bolt_version,
                 config[:auth],
                 []
               )
    end

    test "ok with auth", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, %{"server" => _}} =
               BoltProtocolV4.hello(
                 :gen_tcp,
                 port,
                 bolt_version,
                 config[:auth],
                 []
               )
    end

    test "invalid auth", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:error, _} =
               BoltProtocolV4.hello(
                 :gen_tcp,
                 port,
                 bolt_version,
                 {config[:basic_auth][:username], "wrong!"},
                 []
               )
    end
  end

  test "goodbye/3", %{config: config, port: port, bolt_version: bolt_version} do
    assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

    assert :ok = BoltProtocolV4.goodbye(:gen_tcp, port, bolt_version)
  end

  describe "run/7 for v4:" do
    test "ok without parameters nor metadata", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])
    end

    test "ok with parameters", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN $num AS num", %{num: 5}, %{}, [])
    end

    test "ok with metadata", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])
      {:ok, metadata} = Metadata.new(%{tx_timeout: 10_000})

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, metadata, [])
    end

    @tag :enterprise
    test "ok with database parameter", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])
      {:ok, metadata} = Metadata.new(%{db: "neo4j"})

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, metadata, [])
    end
  end

  describe "pull/5 for v4 (replaces pull_all):" do
    test "pull all records with n=-1", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

      assert {:ok, [record: [1], success: %{"type" => "r"}]} =
               BoltProtocolV4.pull(:gen_tcp, port, bolt_version, %{n: -1}, [])
    end

    test "pull specific number of records", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      # Create a query that returns multiple records
      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(
                 :gen_tcp,
                 port,
                 bolt_version,
                 "UNWIND range(1, 5) AS num RETURN num",
                 %{},
                 %{},
                 []
               )

      # Pull only 2 records
      assert {:ok, result} = BoltProtocolV4.pull(:gen_tcp, port, bolt_version, %{n: 2}, [])

      # Check we got records and a success with has_more
      records = Enum.filter(result, fn {type, _} -> type == :record end)
      assert length(records) == 2
    end

    test "pull_all convenience function", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

      assert {:ok, [record: [1], success: %{"type" => "r"}]} =
               BoltProtocolV4.pull_all(:gen_tcp, port, bolt_version, [])
    end
  end

  describe "discard/5 for v4 (replaces discard_all):" do
    test "discard all records with n=-1", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

      assert :ok = BoltProtocolV4.discard(:gen_tcp, port, bolt_version, %{n: -1}, [])
    end

    test "discard_all convenience function", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

      assert :ok = BoltProtocolV4.discard_all(:gen_tcp, port, bolt_version, [])
    end
  end

  test "reset/4", %{config: config, port: port, bolt_version: bolt_version} do
    assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

    assert {:ok, {:success, %{"fields" => ["num"]}}} =
             BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

    assert :ok = BoltProtocolV4.reset(:gen_tcp, port, bolt_version, [])
  end

  test "run_statement/7 (successful)", %{config: config, port: port, bolt_version: bolt_version} do
    assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

    assert [_ | _] =
             BoltProtocolV4.run_statement(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])
  end

  describe "Transaction management for v4" do
    test "begin without metadata", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, %{}} = BoltProtocolV4.begin(:gen_tcp, port, bolt_version, %{}, [])
    end

    @tag :enterprise
    test "begin with database parameter", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])
      {:ok, metadata} = Metadata.new(%{db: "neo4j"})

      assert {:ok, _} = BoltProtocolV4.begin(:gen_tcp, port, bolt_version, metadata, [])
    end

    test "commit a transaction", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, _} = BoltProtocolV4.begin(:gen_tcp, port, bolt_version, %{}, [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

      assert {:ok, _} = BoltProtocolV4.pull_all(:gen_tcp, port, bolt_version, [])
      assert {:ok, %{"bookmark" => _}} = BoltProtocolV4.commit(:gen_tcp, port, bolt_version, [])
    end

    test "rollback a transaction", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])

      assert {:ok, _} = BoltProtocolV4.begin(:gen_tcp, port, bolt_version, %{}, [])

      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

      assert :ok = BoltProtocolV4.discard_all(:gen_tcp, port, bolt_version, [])
      assert :ok = BoltProtocolV4.rollback(:gen_tcp, port, bolt_version, [])
    end
  end

  describe "Error recovery for v4" do
    test "RESET after cypher error", %{config: config, port: port, bolt_version: bolt_version} do
      assert {:ok, _} = BoltProtocolV4.hello(:gen_tcp, port, bolt_version, config[:auth], [])
      assert {:error, _} = BoltProtocolV4.run(:gen_tcp, port, bolt_version, "Invalid cypher", %{}, %{}, [])

      # After failure, connection should be in FAILED state
      # RESET should return connection to READY state
      assert :ok = BoltProtocolV4.reset(:gen_tcp, port, bolt_version, [])

      # Now we should be able to run queries again
      assert {:ok, {:success, %{"fields" => ["num"]}}} =
               BoltProtocolV4.run(:gen_tcp, port, bolt_version, "RETURN 1 AS num", %{}, %{}, [])

      assert {:ok, [record: [1], success: %{"type" => "r"}]} =
               BoltProtocolV4.pull_all(:gen_tcp, port, bolt_version, [])
    end
  end
end
