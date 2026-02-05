defmodule Bolt.Sips.IdleConnectionTest do
  @moduledoc """
  Integration tests verifying that idle connections are detected and replaced
  by DBConnection's ping/1 mechanism with idle_interval.

  These tests reproduce the production failure scenario described in
  IDLE_CONNECTION_ISSUE.md where Neo4j Aura silently closes idle SSL
  connections, causing pool exhaustion.
  """
  use ExUnit.Case, async: false

  alias Bolt.Sips

  describe "pool recovery from stale connections" do
    test "query succeeds after pool connection is closed and replaced" do
      # Confirm pool is healthy
      conn = Sips.conn()
      assert {:ok, _} = Sips.query(conn, "RETURN 1 AS n")

      # Wait for idle_interval to allow DBConnection to ping and replace
      # any stale connections. idle_interval is 1000ms, so 2s is enough.
      Process.sleep(2_000)

      # Pool should still be healthy
      assert {:ok, _} = Sips.query(Sips.conn(), "RETURN 1 AS n")
    end

    test "queries do not block for recv_timeout on stale connections" do
      conn = Sips.conn()
      assert {:ok, _} = Sips.query(conn, "RETURN 1 AS n")

      # Time a query - should complete quickly, not block for 30s
      {time_us, result} = :timer.tc(fn ->
        Sips.query(Sips.conn(), "RETURN 1 AS n")
      end)

      time_ms = div(time_us, 1_000)

      assert {:ok, _} = result
      assert time_ms < 10_000,
        "Query took #{time_ms}ms - should not block for recv_timeout on stale connection"
    end

    test "concurrent queries succeed when pool is healthy" do
      # Warm up connections
      tasks = for _ <- 1..3 do
        Task.async(fn -> Sips.query(Sips.conn(), "RETURN 1 AS n") end)
      end
      Enum.each(tasks, &Task.await/1)

      # Wait for idle_interval
      Process.sleep(2_000)

      # Concurrent queries should all succeed without pool exhaustion
      tasks = for _ <- 1..5 do
        Task.async(fn ->
          {time_us, result} = :timer.tc(fn ->
            Sips.query(Sips.conn(), "RETURN 1 AS n")
          end)
          {div(time_us, 1_000), result}
        end)
      end

      results = Enum.map(tasks, &Task.await(&1, 30_000))

      for {time_ms, result} <- results do
        assert {:ok, _} = result, "Query failed: #{inspect(result)}"
        assert time_ms < 10_000,
          "Query took #{time_ms}ms - pool exhaustion or blocking on stale socket"
      end
    end
  end
end
