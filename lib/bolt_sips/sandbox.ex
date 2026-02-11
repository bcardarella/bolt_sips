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
        owner = Bolt.Sips.Sandbox.start_owner!(conn)
        on_exit(fn -> Bolt.Sips.Sandbox.stop_owner(owner) end)
        {:ok, conn: conn}
      end

  For `async: false` tests that need shared mode:

      setup do
        conn = Bolt.Sips.conn()
        owner = Bolt.Sips.Sandbox.start_owner!(conn, shared: true)
        on_exit(fn -> Bolt.Sips.Sandbox.stop_owner(owner) end)
        {:ok, conn: conn}
      end

  ## Modes

  - `:manual` — Each test must explicitly check out a connection via
    `start_owner!/1`. Use this for `async: true` tests.
  - `:auto` — Connections are checked out implicitly, behaving like a
    normal pool. Useful during development or for non-isolated tests.
  - `{:shared, pid}` — All processes share the connection owned by `pid`.
    Use this for `async: false` tests where multiple processes need to
    share a single transaction. Prefer using `start_owner!(conn, shared: true)`
    which handles this automatically.

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

  Spawns a dedicated owner process that holds the connection. The calling
  process is automatically allowed to use it. Returns the owner PID, which
  must be passed to `stop_owner/1` when done.

  When the calling process exits, the owner process exits too, automatically
  rolling back the transaction and returning the connection to the pool.

  ## Options

  - `:shared` — when `true`, sets the pool to `{:shared, owner}` mode so
    all processes route through this connection. Use for `async: false` tests.
    Defaults to `false`.
  - `:ownership_timeout` — timeout for the checkout operation in milliseconds.
    Defaults to `120_000`.

  ## Examples

      # async: true test — exclusive ownership
      owner = Bolt.Sips.Sandbox.start_owner!(conn)

      # async: false test — shared ownership
      owner = Bolt.Sips.Sandbox.start_owner!(conn, shared: true)
  """
  @spec start_owner!(DBConnection.conn(), keyword()) :: pid()
  def start_owner!(conn, opts \\ []) do
    parent = self()
    shared = Keyword.get(opts, :shared, false)

    checkout_opts = [
      post_checkout: &post_checkout/2,
      pre_checkin: &pre_checkin/3
    ]

    # GenServer.start runs init synchronously — checkout + BEGIN complete
    # before returning. stop_owner/1 uses GenServer.stop which triggers
    # terminate/2 for synchronous checkin (ROLLBACK) before the process exits.
    {:ok, pid} = GenServer.start(__MODULE__.Owner, {conn, parent, shared, checkout_opts})
    pid
  end

  @doc """
  Stops the owner process, rolling back the transaction and returning
  the connection to the pool.

  This is a synchronous call — it blocks until the owner process has
  fully terminated. The owner's `terminate/2` callback runs
  `ownership_checkin` which triggers the `pre_checkin` callback,
  sending ROLLBACK to Neo4j before the process exits.
  """
  @spec stop_owner(pid()) :: :ok
  def stop_owner(pid) when is_pid(pid) do
    GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  @doc """
  Allows `child_pid` to use the connection checked out by `owner_pid`.

  This is necessary when a test spawns processes that need to query Neo4j.
  """
  @spec allow(DBConnection.conn(), pid(), pid()) :: :ok | {:already, :owner | :allowed} | :not_found
  def allow(conn, owner_pid, child_pid) do
    DBConnection.Ownership.ownership_allow(conn, owner_pid, child_pid, [])
  end

  # A minimal GenServer that holds the ownership checkout.
  # init/1 runs synchronously (checkout + BEGIN + allow/share complete before start returns).
  # terminate/2 runs synchronously on stop (checkin + ROLLBACK complete before stop returns).
  defmodule Owner do
    @moduledoc false
    use GenServer

    @impl true
    def init({conn, parent, shared, checkout_opts}) do
      case DBConnection.Ownership.ownership_checkout(conn, checkout_opts) do
        :ok -> :ok
        {:already, :owner} -> :ok
        {:already, :allowed} -> :ok
      end

      if shared do
        DBConnection.Ownership.ownership_mode(conn, {:shared, self()}, [])
      else
        DBConnection.Ownership.ownership_allow(conn, self(), parent, [])
      end

      {:ok, conn}
    end

    @impl true
    def terminate(_reason, conn) do
      DBConnection.Ownership.ownership_checkin(conn, [])
    catch
      :exit, _ -> :ok
    end
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
  # On normal checkin: sends ROLLBACK to undo all changes made during the test.
  # On disconnect/stop: skips ROLLBACK since the connection is being destroyed.
  # This matches Ecto.Adapters.SQL.Sandbox's behavior.
  defp pre_checkin(:checkin, conn_module, conn_state) do
    case conn_module.handle_rollback([], conn_state) do
      {:ok, _result, new_state} ->
        {:ok, conn_module, new_state}

      {:error, _err, new_state} ->
        # ROLLBACK failed — connection state is unreliable.
        # Disconnect so the pool creates a fresh connection.
        {:disconnect, :rollback_failed, conn_module, new_state}

      {:disconnect, _err, new_state} ->
        {:disconnect, :rollback_failed, conn_module, new_state}
    end
  end

  defp pre_checkin(_reason, conn_module, conn_state) do
    # Connection is being disconnected or stopped — don't attempt
    # ROLLBACK on a broken connection. The pool will replace it.
    {:ok, conn_module, conn_state}
  end
end
