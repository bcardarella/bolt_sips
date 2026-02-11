defmodule Bolt.Sips.SandboxStressTest do
  @moduledoc """
  Stress tests for the sandbox to reproduce connection degradation
  that occurs when many tests checkout/checkin sequentially — at
  the scale of a real test suite (1600+ cycles).
  """
  use ExUnit.Case, async: false

  alias Bolt.Sips.Response

  @sandbox_prefix :sandbox_stress
  @scale 1600

  setup_all do
    opts = [
      url: "bolt://localhost",
      basic_auth: [
        username: System.get_env("NEO4J_USER") || "neo4j",
        password: System.get_env("NEO4J_PASSWORD") || "testpassword"
      ],
      pool_size: 10,
      pool: DBConnection.Ownership,
      prefix: @sandbox_prefix,
      ownership_timeout: 120_000
    ]

    Bolt.Sips.start_link(opts)
    :ok
  end

  describe "sequential checkout/checkin at scale" do
    test "#{@scale} sequential checkout/query/checkin cycles" do
      conn = Bolt.Sips.conn(:direct, prefix: @sandbox_prefix)
      Bolt.Sips.Sandbox.mode(conn, :manual)

      for i <- 1..@scale do
        Bolt.Sips.Sandbox.start_owner!(conn)

        {:ok, %Response{results: [result]}} =
          Bolt.Sips.query(conn, "RETURN $i AS n", %{i: i})

        assert result["n"] == i

        Bolt.Sips.Sandbox.stop_owner(conn)
      end
    end

    test "#{@scale} cycles with writes and rollback" do
      conn = Bolt.Sips.conn(:direct, prefix: @sandbox_prefix)
      Bolt.Sips.Sandbox.mode(conn, :manual)

      for i <- 1..@scale do
        Bolt.Sips.Sandbox.start_owner!(conn)

        Bolt.Sips.query!(conn, "CREATE (n:StressTest {seq: $i}) RETURN n", %{i: i})

        {:ok, %Response{results: [result]}} =
          Bolt.Sips.query(conn, "MATCH (n:StressTest {seq: $i}) RETURN count(n) AS cnt", %{i: i})

        assert result["cnt"] == 1, "Iteration #{i}: expected 1, got #{result["cnt"]}"

        Bolt.Sips.Sandbox.stop_owner(conn)
      end

      # Verify nothing persisted
      Bolt.Sips.Sandbox.start_owner!(conn)

      {:ok, %Response{results: [result]}} =
        Bolt.Sips.query(conn, "MATCH (n:StressTest) RETURN count(n) AS cnt")

      assert result["cnt"] == 0
      Bolt.Sips.Sandbox.stop_owner(conn)
    end
  end

  describe "spawned process lifecycle at scale" do
    test "#{@scale} spawned processes checkout, query, exit (auto-cleanup)" do
      conn = Bolt.Sips.conn(:direct, prefix: @sandbox_prefix)
      Bolt.Sips.Sandbox.mode(conn, :manual)

      for i <- 1..@scale do
        task =
          Task.async(fn ->
            Bolt.Sips.Sandbox.start_owner!(conn)

            {:ok, %Response{results: [result]}} =
              Bolt.Sips.query(conn, "RETURN $i AS n", %{i: i})

            assert result["n"] == i
            # No stop_owner — process exit triggers auto-cleanup
          end)

        Task.await(task, 5_000)
        Process.sleep(1)
      end

      # Pool still healthy
      Bolt.Sips.Sandbox.start_owner!(conn)

      {:ok, %Response{results: [result]}} =
        Bolt.Sips.query(conn, "RETURN 'alive' AS status")

      assert result["status"] == "alive"
      Bolt.Sips.Sandbox.stop_owner(conn)
    end

    test "#{@scale} shared-mode cycles" do
      conn = Bolt.Sips.conn(:direct, prefix: @sandbox_prefix)

      for i <- 1..@scale do
        Bolt.Sips.Sandbox.mode(conn, :manual)
        Bolt.Sips.Sandbox.start_owner!(conn)
        Bolt.Sips.Sandbox.mode(conn, {:shared, self()})

        {:ok, %Response{results: [result]}} =
          Bolt.Sips.query(conn, "RETURN $i AS n", %{i: i})

        assert result["n"] == i

        Bolt.Sips.Sandbox.stop_owner(conn)
      end
    end
  end
end
