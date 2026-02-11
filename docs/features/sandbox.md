# Sandbox: Transaction-Based Test Isolation

`Bolt.Sips.Sandbox` provides concurrent, transactional test isolation for Neo4j — the same pattern that `Ecto.Adapters.SQL.Sandbox` provides for PostgreSQL.

Each test checks out a dedicated connection, wraps it in a Neo4j transaction, and automatically rolls back all changes when the test completes. Tests never see each other's data and leave no residue in the database.

## Quick start

### 1. Configure the ownership pool

In your test config, replace the default connection pool with `DBConnection.Ownership`:

```elixir
# config/test.exs
config :bolt_sips, Bolt,
  url: "bolt://localhost",
  basic_auth: [username: "neo4j", password: "password"],
  pool: DBConnection.Ownership,
  pool_size: 10,
  ownership_timeout: 120_000
```

Key options:

| Option | Description |
|--------|-------------|
| `pool: DBConnection.Ownership` | Enables the ownership pool (required) |
| `pool_size` | Max concurrent test connections. Match to your async test concurrency. |
| `ownership_timeout` | How long a test can hold a connection before timeout (ms). |

### 2. Set manual mode in your test helper

```elixir
# test/test_helper.exs
Bolt.Sips.start_link(Application.get_env(:bolt_sips, Bolt))
Bolt.Sips.Sandbox.mode(Bolt.Sips.conn(), :manual)

ExUnit.start()
```

Manual mode ensures no test gets a connection unless it explicitly checks one out.

### 3. Check out a connection in each test

Create a shared setup module:

```elixir
# test/support/neo4j_case.ex
defmodule MyApp.Neo4jCase do
  use ExUnit.CaseTemplate

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
end
```

Use it in your tests:

```elixir
defmodule MyApp.UserGraphTest do
  use MyApp.Neo4jCase, async: true

  test "creates a user node", %{conn: conn} do
    Bolt.Sips.query!(conn, "CREATE (u:User {name: 'Alice'}) RETURN u")

    {:ok, result} =
      Bolt.Sips.query(conn, "MATCH (u:User {name: 'Alice'}) RETURN count(u) AS cnt")

    assert hd(result.results)["cnt"] == 1
  end
  # The node is automatically rolled back — no cleanup needed.
end
```

## How it works

```
Test process                    Neo4j connection
────────────                    ────────────────
start_owner!(conn)
  ├─ ownership_checkout  ──────► BEGIN
  │
query!(conn, "CREATE ...")  ──► RUN ... (inside transaction)
query!(conn, "MATCH ...")   ──► RUN ... (sees own writes)
  │
test exits / stop_owner
  └─ ownership_checkin   ──────► ROLLBACK (all changes undone)
```

The `post_checkout` hook sends `BEGIN` when a connection is checked out. The `pre_checkin` hook sends `ROLLBACK` when it is returned. Everything in between runs inside that transaction.

## Modes

### Manual mode (recommended for async tests)

```elixir
Bolt.Sips.Sandbox.mode(conn, :manual)
```

Each test must call `start_owner!/1`. This is the default for concurrent tests — each test gets its own isolated connection.

### Shared mode (for non-async tests)

```elixir
setup do
  conn = Bolt.Sips.conn()
  Bolt.Sips.Sandbox.start_owner!(conn)
  Bolt.Sips.Sandbox.mode(conn, {:shared, self()})

  on_exit(fn ->
    try do
      Bolt.Sips.Sandbox.stop_owner(conn)
    catch
      _, _ -> :ok
    end
  end)

  {:ok, conn: conn}
end
```

In shared mode, all processes route through the owner's connection. Use this for `async: false` tests that spawn processes which need database access.

### Auto mode

```elixir
Bolt.Sips.Sandbox.mode(conn, :auto)
```

Connections are checked out implicitly, like a regular pool. No transaction wrapping occurs. Useful if you need to temporarily bypass the sandbox.

## Sharing connections with spawned processes

When your test spawns a process that queries Neo4j, that process needs access to the test's connection.

### Option 1: `allow/3`

```elixir
test "worker queries neo4j", %{conn: conn} do
  {:ok, worker} = MyWorker.start_link()
  Bolt.Sips.Sandbox.allow(conn, self(), worker)

  # worker can now query through the test's sandboxed connection
  assert MyWorker.fetch_count(worker) == 0
end
```

### Option 2: Elixir Tasks (automatic)

`Task.async/1` and `Task.Supervisor.async/2` automatically set the `$callers` process dictionary. `DBConnection.Ownership` follows this chain, so tasks inherit the parent's connection without an explicit `allow/3`:

```elixir
test "task inherits sandbox", %{conn: conn} do
  Bolt.Sips.query!(conn, "CREATE (n:Item {name: 'test'})")

  count =
    Task.async(fn ->
      {:ok, r} = Bolt.Sips.query(conn, "MATCH (n:Item) RETURN count(n) AS cnt")
      hd(r.results)["cnt"]
    end)
    |> Task.await()

  assert count == 1
end
```

## Nested transactions

If code under test calls `Bolt.Sips.transaction/3`, it works correctly inside the sandbox. Neo4j does not support savepoints, so the driver tracks transaction depth internally:

- Inner `BEGIN` calls are no-ops (depth is incremented).
- Inner `COMMIT` calls are no-ops (depth is decremented).
- Inner `ROLLBACK` calls are no-ops (depth is decremented).
- Only the outermost transaction (the sandbox) actually communicates with Neo4j.

```elixir
test "code that uses transactions works in sandbox", %{conn: conn} do
  # This transaction is a no-op at the Neo4j level — the sandbox
  # transaction is already open.
  {:ok, _} =
    Bolt.Sips.transaction(conn, fn tx_conn ->
      Bolt.Sips.query!(tx_conn, "CREATE (n:Item {name: 'nested'}) RETURN n")
    end)

  {:ok, result} =
    Bolt.Sips.query(conn, "MATCH (n:Item {name: 'nested'}) RETURN count(n) AS cnt")

  assert hd(result.results)["cnt"] == 1
  # Rolled back when the sandbox closes — nothing persists.
end
```

## Using with Ecto (dual-database apps)

If your application uses both Ecto (PostgreSQL) and Bolt.Sips (Neo4j), set up both sandboxes in your shared test case:

```elixir
defmodule MyApp.DataCase do
  use ExUnit.CaseTemplate

  setup tags do
    # PostgreSQL sandbox
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(MyApp.Repo, shared: not tags[:async])
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    # Neo4j sandbox
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
end
```

Both databases now roll back automatically at the end of every test.

## Troubleshooting

### `ownership_timeout` errors

If tests fail with ownership timeout errors, increase the timeout:

```elixir
config :bolt_sips, Bolt,
  ownership_timeout: 300_000  # 5 minutes
```

Or pass it per-checkout:

```elixir
Bolt.Sips.Sandbox.start_owner!(conn, ownership_timeout: 300_000)
```

### Pool exhaustion

Each concurrent test holds one connection. If you see checkout timeouts, increase `pool_size` to match your test concurrency.

### Neo4j transaction timeout

Neo4j has a server-side transaction timeout (default 60s, configured via `dbms.transaction.timeout`). Long-running tests may hit this. Increase it in your test Neo4j configuration.
