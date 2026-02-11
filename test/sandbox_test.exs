defmodule Bolt.Sips.SandboxTest do
  use ExUnit.Case, async: false

  alias Bolt.Sips.Response

  @sandbox_prefix :sandbox_test

  setup_all do
    # Start connections under a separate prefix with the ownership pool.
    # The default connections (started by test_helper.exs) use the regular pool,
    # so we use a different prefix to avoid interference.
    opts = [
      url: "bolt://localhost",
      basic_auth: [
        username: System.get_env("NEO4J_USER") || "neo4j",
        password: System.get_env("NEO4J_PASSWORD") || "testpassword"
      ],
      pool_size: 5,
      pool: DBConnection.Ownership,
      prefix: @sandbox_prefix,
      ownership_timeout: 60_000
    ]

    Bolt.Sips.start_link(opts)

    :ok
  end

  setup do
    conn = Bolt.Sips.conn(:direct, prefix: @sandbox_prefix)
    Bolt.Sips.Sandbox.mode(conn, :manual)
    owner = Bolt.Sips.Sandbox.start_owner!(conn)
    on_exit(fn -> Bolt.Sips.Sandbox.stop_owner(owner) end)
    {:ok, conn: conn, owner: owner}
  end

  describe "start_owner!/1 and stop_owner/1" do
    test "start_owner! returns a pid", %{owner: owner} do
      assert is_pid(owner)
      assert Process.alive?(owner)
    end

    test "queries work inside a sandbox", %{conn: conn} do
      assert {:ok, %Response{}} =
               Bolt.Sips.query(conn, "CREATE (n:SandboxTest {name: 'hello'}) RETURN n")
    end

    test "data created in sandbox is visible within the same sandbox", %{conn: conn} do
      Bolt.Sips.query!(conn, "CREATE (n:SandboxTest {name: 'visible'}) RETURN n")

      {:ok, %Response{results: [result]}} =
        Bolt.Sips.query(conn, "MATCH (n:SandboxTest {name: 'visible'}) RETURN count(n) AS cnt")

      assert result["cnt"] == 1
    end

    test "data is rolled back when owner is stopped and re-started", %{conn: conn, owner: owner} do
      # Create data inside the sandbox
      Bolt.Sips.query!(conn, "CREATE (n:SandboxTest {name: 'rollback_me'}) RETURN n")

      {:ok, %Response{results: [result]}} =
        Bolt.Sips.query(conn, "MATCH (n:SandboxTest {name: 'rollback_me'}) RETURN count(n) AS cnt")

      assert result["cnt"] == 1

      # Stop the owner — triggers ROLLBACK via pre_checkin
      Bolt.Sips.Sandbox.stop_owner(owner)

      # Re-checkout — starts a new BEGIN
      _new_owner = Bolt.Sips.Sandbox.start_owner!(conn)

      # The node should no longer exist
      {:ok, %Response{results: [result]}} =
        Bolt.Sips.query(conn, "MATCH (n:SandboxTest {name: 'rollback_me'}) RETURN count(n) AS cnt")

      assert result["cnt"] == 0
    end

    test "stop_owner works from a different process", %{owner: owner} do
      task =
        Task.async(fn ->
          Bolt.Sips.Sandbox.stop_owner(owner)
        end)

      assert :ok = Task.await(task, 5_000)
      refute Process.alive?(owner)
    end

    test "stop_owner works from on_exit callback" do
      conn = Bolt.Sips.conn(:direct, prefix: @sandbox_prefix)
      owner = Bolt.Sips.Sandbox.start_owner!(conn)

      # Simulate what on_exit does — call from a different process
      task =
        Task.async(fn ->
          Bolt.Sips.Sandbox.stop_owner(owner)
        end)

      assert :ok = Task.await(task, 5_000)
      refute Process.alive?(owner)
    end
  end

  describe "concurrent isolation" do
    test "two sandboxed processes cannot see each other's data", %{conn: conn} do
      # Create a node in this test's sandbox
      Bolt.Sips.query!(conn, "CREATE (n:SandboxTest {name: 'process_a'}) RETURN n")

      # Spawn a task that runs in its own sandbox and checks for process_a's node
      task =
        Task.async(fn ->
          task_conn = Bolt.Sips.conn(:direct, prefix: @sandbox_prefix)
          task_owner = Bolt.Sips.Sandbox.start_owner!(task_conn)

          # This process should NOT see process_a's data
          {:ok, %Response{results: [result]}} =
            Bolt.Sips.query(
              task_conn,
              "MATCH (n:SandboxTest {name: 'process_a'}) RETURN count(n) AS cnt"
            )

          count = result["cnt"]

          # Create our own node
          Bolt.Sips.query!(task_conn, "CREATE (n:SandboxTest {name: 'process_b'}) RETURN n")

          Bolt.Sips.Sandbox.stop_owner(task_owner)
          count
        end)

      other_process_count = Task.await(task, 10_000)
      assert other_process_count == 0

      # This process should NOT see process_b's data
      {:ok, %Response{results: [result]}} =
        Bolt.Sips.query(conn, "MATCH (n:SandboxTest {name: 'process_b'}) RETURN count(n) AS cnt")

      assert result["cnt"] == 0
    end
  end

  describe "allow/3" do
    test "allowed process can use the owner's connection", %{conn: conn, owner: owner} do
      # Create data in the owner's sandbox
      Bolt.Sips.query!(conn, "CREATE (n:SandboxTest {name: 'shared'}) RETURN n")

      task =
        Task.async(fn ->
          # Allow this task to use the owner's connection
          Bolt.Sips.Sandbox.allow(conn, owner, self())

          # The allowed process should see the owner's data
          {:ok, %Response{results: [result]}} =
            Bolt.Sips.query(
              conn,
              "MATCH (n:SandboxTest {name: 'shared'}) RETURN count(n) AS cnt"
            )

          result["cnt"]
        end)

      count = Task.await(task, 10_000)
      assert count == 1
    end
  end

  describe "shared mode" do
    test "start_owner! with shared: true enables shared mode", %{owner: owner} do
      conn = Bolt.Sips.conn(:direct, prefix: @sandbox_prefix)

      # Stop the setup owner first
      Bolt.Sips.Sandbox.stop_owner(owner)

      # Start with shared mode
      shared_owner = Bolt.Sips.Sandbox.start_owner!(conn, shared: true)

      # Create data
      Bolt.Sips.query!(conn, "CREATE (n:SandboxTest {name: 'shared_data'}) RETURN n")

      # A completely unrelated process should see the data (shared mode)
      task =
        Task.async(fn ->
          {:ok, %Response{results: [result]}} =
            Bolt.Sips.query(
              conn,
              "MATCH (n:SandboxTest {name: 'shared_data'}) RETURN count(n) AS cnt"
            )

          result["cnt"]
        end)

      count = Task.await(task, 10_000)
      assert count == 1

      Bolt.Sips.Sandbox.stop_owner(shared_owner)
    end
  end

  describe "nested transactions" do
    test "Bolt.Sips.transaction inside a sandbox works without error", %{conn: conn} do
      # Code under test may call Bolt.Sips.transaction explicitly.
      # Inside the sandbox (which already has an open transaction via BEGIN),
      # this should not cause a nested BEGIN error from Neo4j.
      result =
        Bolt.Sips.transaction(conn, fn tx_conn ->
          Bolt.Sips.query!(tx_conn, "CREATE (n:SandboxTest {name: 'nested_tx'}) RETURN n")

          {:ok, %Response{results: [r]}} =
            Bolt.Sips.query(tx_conn, "MATCH (n:SandboxTest {name: 'nested_tx'}) RETURN count(n) AS cnt")

          r["cnt"]
        end)

      assert {:ok, 1} = result
    end
  end

  describe "mode/2" do
    test "auto mode allows queries without explicit checkout", %{owner: owner} do
      conn = Bolt.Sips.conn(:direct, prefix: @sandbox_prefix)

      # Stop the setup owner
      Bolt.Sips.Sandbox.stop_owner(owner)

      Bolt.Sips.Sandbox.mode(conn, :auto)

      # Should work without explicit checkout
      assert {:ok, %Response{}} = Bolt.Sips.query(conn, "RETURN 1 AS n")

      # Reset back to manual for other tests
      Bolt.Sips.Sandbox.mode(conn, :manual)
    end
  end

  describe "owner process lifecycle" do
    test "owner exits when caller exits", %{owner: owner} do
      conn = Bolt.Sips.conn(:direct, prefix: @sandbox_prefix)

      # Stop the setup owner
      Bolt.Sips.Sandbox.stop_owner(owner)

      # Spawn a process that starts an owner, then exits
      {_caller_pid, caller_ref} =
        spawn_monitor(fn ->
          _inner_owner = Bolt.Sips.Sandbox.start_owner!(conn)
          # Process exits here — owner should auto-cleanup
        end)

      # Wait for the caller to exit
      assert_receive {:DOWN, ^caller_ref, _, _, _}, 5_000

      # Give DBConnection a moment to process the exit
      Process.sleep(50)

      # Pool should be healthy — start a new owner
      new_owner = Bolt.Sips.Sandbox.start_owner!(conn)
      {:ok, %Response{results: [result]}} = Bolt.Sips.query(conn, "RETURN 'ok' AS status")
      assert result["status"] == "ok"
      Bolt.Sips.Sandbox.stop_owner(new_owner)
    end
  end
end
