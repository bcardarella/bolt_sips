defmodule Bolt.Sips.SandboxReproTest do
  @moduledoc """
  Reproduces "socket is not connected" using the DEFAULT pool prefix,
  matching how a real application (e.g., Cog) starts and uses bolt_sips.
  """
  use ExUnit.Case, async: false
  # Excluded from normal runs because it restarts the default pool.
  # Run with: mix test test/sandbox_repro_test.exs --include skip
  @moduletag :skip

  alias Bolt.Sips.Response

  @cycles 200

  # Restart the default pool with DBConnection.Ownership,
  # matching how an application configures bolt_sips for tests.
  setup_all do
    # Stop the existing default pool (supervisor is named Bolt.Sips)
    case Process.whereis(Bolt.Sips) do
      nil -> :ok
      pid ->
        Supervisor.stop(pid, :normal)
        Process.sleep(200)
    end

    opts = [
      url: "bolt://localhost",
      basic_auth: [
        username: System.get_env("NEO4J_USER") || "neo4j",
        password: System.get_env("NEO4J_PASSWORD") || "testpassword"
      ],
      pool_size: 10,
      pool: DBConnection.Ownership,
      ownership_timeout: 120_000
    ]

    {:ok, _} = Bolt.Sips.start_link(opts)
    Process.sleep(100)

    conn = Bolt.Sips.conn()
    Bolt.Sips.Sandbox.mode(conn, :manual)

    {:ok, conn: conn}
  end

  describe "default prefix pool with ExUnit-like lifecycle" do
    test "shared mode cycles with writes", %{conn: conn} do
      for i <- 1..@cycles do
        owner = Bolt.Sips.Sandbox.start_owner!(conn, shared: true)

        {:ok, %Response{}} =
          Bolt.Sips.query(conn, """
          MERGE (b:Brain {id: $id})
          SET b.engram_count = 0, b.synapse_count = 0
          RETURN b
          """, %{id: "repro-brain-#{i}"})

        {:ok, %Response{}} =
          Bolt.Sips.query(conn, """
          CREATE (e:Engram {id: $id, brain_id: $brain_id, term: $term, definition: $def})
          RETURN e
          """, %{id: "repro-eng-#{i}", brain_id: "repro-brain-#{i}",
                 term: "Term #{i}", def: "Def #{i}"})

        {:ok, %Response{}} =
          Bolt.Sips.query(conn, """
          CREATE (e2:Engram {id: $id, brain_id: $brain_id, term: $term, definition: $def})
          RETURN e2
          """, %{id: "repro-eng-#{i}-b", brain_id: "repro-brain-#{i}",
                 term: "Term #{i}b", def: "Def #{i}b"})

        {:ok, %Response{}} =
          Bolt.Sips.query(conn, """
          MATCH (a:Engram {id: $from}), (b:Engram {id: $to})
          CREATE (a)-[r:RELATED_TO {weight: 1.0}]->(b)
          RETURN r
          """, %{from: "repro-eng-#{i}", to: "repro-eng-#{i}-b"})

        {:ok, %Response{}} =
          Bolt.Sips.query(conn, """
          MATCH (e:Engram {brain_id: $brain_id})
          WITH count(e) AS cnt
          MATCH (b:Brain {id: $brain_id})
          SET b.engram_count = cnt
          RETURN b.engram_count AS count
          """, %{brain_id: "repro-brain-#{i}"})

        # Stop from different process, simulating ExUnit on_exit
        Task.async(fn -> Bolt.Sips.Sandbox.stop_owner(owner) end)
        |> Task.await(5_000)
      end
    end

    test "exclusive mode cycles", %{conn: conn} do
      for i <- 1..@cycles do
        owner = Bolt.Sips.Sandbox.start_owner!(conn)

        {:ok, %Response{results: [result]}} =
          Bolt.Sips.query(conn, "RETURN $i AS n", %{i: i})

        assert result["n"] == i

        Task.async(fn -> Bolt.Sips.Sandbox.stop_owner(owner) end)
        |> Task.await(5_000)
      end
    end

    test "pool health after cycles", %{conn: conn} do
      owner = Bolt.Sips.Sandbox.start_owner!(conn)

      {:ok, %Response{results: [result]}} =
        Bolt.Sips.query(conn, "RETURN 'healthy' AS status")

      assert result["status"] == "healthy"
      Bolt.Sips.Sandbox.stop_owner(owner)
    end
  end
end
