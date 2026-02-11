defmodule Bolt.Sips.Sandbox do
  @moduledoc """
  A sandbox for concurrent transactional tests against Neo4j.

  Built on `DBConnection.Ownership`, this module allows each test to
  check out a dedicated connection, wrap it in a transaction, and
  roll back all changes when the test completes. This is the Neo4j
  equivalent of `Ecto.Adapters.SQL.Sandbox`.

  ## How it works

  1. The connection pool is started with `DBConnection.Ownership` instead
     of the default pool.
  2. Each test checks out a connection and a `BEGIN` is sent automatically.
  3. All queries from the test process run inside that transaction.
  4. When the test ends, a `ROLLBACK` is sent, undoing all changes.

  Because each test runs in its own transaction, tests are fully isolated
  and can run concurrently without interfering with each other.

  ## Setup

  Add `pool: DBConnection.Ownership` to your test configuration:

      # config/test.exs
      config :bolt_sips, Bolt,
        url: "bolt://localhost",
        pool: DBConnection.Ownership,
        pool_size: 10,
        ownership_timeout: 120_000

  Set the pool to manual mode in your test helper:

      # test/test_helper.exs
      Bolt.Sips.start_link(Application.get_env(:bolt_sips, Bolt))
      Bolt.Sips.Sandbox.mode(Bolt.Sips.conn(), :manual)

  Check out a connection in each test's setup:

      # test/support/data_case.ex or directly in your test module
      setup do
        conn = Bolt.Sips.conn()
        Bolt.Sips.Sandbox.start_owner!(conn)
        on_exit(fn ->
          try do
            Bolt.Sips.Sandbox.stop_owner(conn)
          catch
            _, _ -> :ok
          end
        end)
        {:ok, conn: conn}
      end

  ## Modes

  - `:manual` — Each test must explicitly check out a connection via
    `start_owner!/1`. Use this for `async: true` tests.
  - `:auto` — Connections are checked out implicitly, behaving like a
    normal pool. Useful during development or for non-isolated tests.
  - `{:shared, pid}` — All processes share the connection owned by `pid`.
    Use this for `async: false` tests where multiple processes need to
    share a single transaction.

  ## Sharing connections with spawned processes

  When a test spawns a process (e.g., a GenServer or Task) that needs to
  query Neo4j, use `allow/3` to grant it access to the test's connection:

      test "worker can query neo4j", %{conn: conn} do
        {:ok, pid} = MyWorker.start_link()
        Bolt.Sips.Sandbox.allow(conn, self(), pid)
        # pid can now use the same connection and transaction
      end

  Elixir `Task`s spawned with `Task.async/1` automatically inherit the
  caller chain (`$callers`), so `DBConnection.Ownership` can route them
  to the parent's connection without an explicit `allow/3` call.

  ## Nested transactions

  If code under test calls `Bolt.Sips.transaction/3`, it will work
  correctly inside the sandbox. Neo4j does not support savepoints or
  nested transactions, so inner `BEGIN`/`COMMIT`/`ROLLBACK` calls are
  treated as no-ops — the outer sandbox transaction remains in control
  and rolls back everything when the test ends.

  ## Pool sizing

  Each checked-out connection is held for the entire duration of its
  owning test. Size the pool to match your test concurrency. For example,
  if you run 10 async tests simultaneously, set `pool_size: 10`.
  """

  @doc """
  Sets the sandbox mode for the given connection pool.

  - `:manual` — connections must be explicitly checked out via `start_owner!/1`
  - `:auto` — connections are checked out automatically (like a regular pool)
  - `{:shared, pid}` — all processes share the given pid's connection
  """
  @spec mode(DBConnection.conn(), :auto | :manual | {:shared, pid()}) :: :ok
  def mode(conn, mode) when mode in [:auto, :manual] do
    DBConnection.Ownership.ownership_mode(conn, mode, [])
    :ok
  end

  def mode(conn, {:shared, owner_pid}) do
    DBConnection.Ownership.ownership_mode(conn, {:shared, owner_pid}, [])
    :ok
  end

  @doc """
  Checks out a connection and wraps it in a transaction.

  The calling process becomes the owner of the connection. All queries
  executed by this process (or allowed processes) will run inside the
  transaction. When `stop_owner/1` is called or the owner process exits,
  the transaction is rolled back automatically.

  Returns `:ok`.

  ## Options

  Accepts the same options as `DBConnection.Ownership.ownership_checkout/2`.
  """
  @spec start_owner!(DBConnection.conn(), keyword()) :: :ok
  def start_owner!(conn, opts \\ []) do
    ownership_opts =
      Keyword.merge(opts,
        post_checkout: &post_checkout/2,
        pre_checkin: &pre_checkin/3
      )

    case DBConnection.Ownership.ownership_checkout(conn, ownership_opts) do
      :ok ->
        :ok

      {:already, :owner} ->
        :ok

      {:already, :allowed} ->
        :ok

      other ->
        raise "Failed to checkout Neo4j sandbox connection: #{inspect(other)}"
    end
  end

  @doc """
  Stops the owner, rolling back the transaction and returning
  the connection to the pool.

  Must be called from the owner process.
  """
  @spec stop_owner(DBConnection.conn()) :: :ok | :not_owner | :not_found
  def stop_owner(conn) do
    DBConnection.Ownership.ownership_checkin(conn, [])
  end

  @doc """
  Allows `child_pid` to use the connection checked out by `owner_pid`.

  This is necessary when a test spawns processes that need to query Neo4j.
  """
  @spec allow(DBConnection.conn(), pid(), pid()) :: :ok | {:already, :owner | :allowed} | :not_found
  def allow(conn, owner_pid, child_pid) do
    DBConnection.Ownership.ownership_allow(conn, owner_pid, child_pid, [])
  end

  # Called after a connection is checked out from the ownership pool.
  # Sends BEGIN to start a transaction on the Neo4j connection.
  defp post_checkout(conn_module, conn_state) do
    case conn_module.handle_begin([], conn_state) do
      {:ok, _result, new_state} ->
        {:ok, conn_module, new_state}

      {:error, err, new_state} ->
        {:disconnect, err, conn_module, new_state}

      {:disconnect, err, new_state} ->
        {:disconnect, err, conn_module, new_state}
    end
  end

  # Called before a connection is checked back in to the ownership pool.
  # Sends ROLLBACK to undo all changes made during the test.
  defp pre_checkin(_reason, conn_module, conn_state) do
    case conn_module.handle_rollback([], conn_state) do
      {:ok, _result, new_state} ->
        {:ok, conn_module, new_state}

      {:error, _err, new_state} ->
        # Even if rollback fails, return the connection.
        # Attempt a RESET to recover the connection state.
        try_reset(conn_module, new_state)

      {:disconnect, _err, new_state} ->
        {:disconnect, :rollback_failed, conn_module, new_state}
    end
  end

  # Attempt to reset the connection after a failed rollback.
  defp try_reset(conn_module, conn_state) do
    %{sock: sock, bolt_version: bolt_version, configuration: conf} = conn_state
    socket = conf[:socket]

    try do
      Bolt.Sips.Internals.BoltProtocol.reset(socket, sock, bolt_version)
      {:ok, conn_module, conn_state}
    rescue
      _ -> {:disconnect, :reset_failed, conn_module, conn_state}
    catch
      _, _ -> {:disconnect, :reset_failed, conn_module, conn_state}
    end
  end
end
