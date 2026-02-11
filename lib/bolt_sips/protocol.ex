defmodule Bolt.Sips.Protocol do
  @moduledoc false
  # Implements callbacks required by DBConnection.
  # Each callback receives an open connection as a state.

  defmodule ConnData do
    @moduledoc false
    # Defines the state used by DbConnection implementation
    defstruct [:sock, :bolt_version, :configuration, :server_hints, status: :idle, tx_depth: 0]

    @type t :: %__MODULE__{
            sock: port(),
            bolt_version: integer() | {integer(), integer()},
            configuration: Keyword.t(),
            server_hints: map() | nil,
            status: :idle | :transaction,
            tx_depth: non_neg_integer()
          }
  end

  use DBConnection

  require Logger

  alias Bolt.Sips.QueryStatement
  alias Bolt.Sips.Internals.Error, as: BoltError
  alias Bolt.Sips.Internals.BoltProtocol

  @doc "Callback for DBConnection.connect/1"

  def connect(opts \\ [])
  def connect([]), do: connect(Bolt.Sips.Utils.default_config())

  def connect(opts) do
    conf = opts |> Bolt.Sips.Utils.default_config()
    host = _to_hostname(conf[:hostname])
    port = conf[:port]
    auth = extract_auth(conf[:basic_auth])
    timeout = conf[:timeout]
    socket = conf[:socket]
    default_socket_options = [packet: :raw, mode: :binary, active: false]

    # Debug logging to help diagnose connection issues during app boot
    # This can be removed once the Phoenix boot issue is resolved
    if Application.get_env(:bolt_sips, :debug_connect, false) do
      Logger.debug("""
      [Bolt.Sips.Protocol] connect called with:
        raw opts keys: #{inspect(Keyword.keys(opts))}
        hostname from opts: #{inspect(opts[:hostname])}
        hostname from conf: #{inspect(conf[:hostname])}
        resolved host: #{inspect(host)}
        port: #{inspect(port)}
        socket module: #{inspect(socket)}
        ssl config: #{inspect(conf[:ssl])}
      """)
    end

    socket_opts =
      case conf[:ssl] do
        list when is_list(list) -> Keyword.merge(default_socket_options, conf[:ssl])
        _ -> default_socket_options
      end

    with {:ok, sock} <- socket.connect(host, port, socket_opts, timeout),
         {:ok, bolt_version} <- BoltProtocol.handshake(socket, sock),
         {:ok, server_version, server_hints} <- do_init(socket, sock, bolt_version, auth),
         :ok <- socket.setopts(sock, active: :once) do
      {:ok,
       %ConnData{
         sock: sock,
         bolt_version: bolt_version,
         configuration: Keyword.merge(conf, server_version: server_version),
         server_hints: server_hints
       }}
    else
      {:error, %BoltError{}} = error ->
        error

      {:error, reason} ->
        {:error, BoltError.exception(reason, nil, :connect)}
    end
  end

  # v5.1+ uses HELLO (without auth) + LOGON (with auth)
  defp do_init(transport, port, {major, minor} = bolt_version, auth)
       when major >= 5 and minor >= 1 do
    # For v5.1+, HELLO doesn't include auth credentials
    # LOGON is sent separately after HELLO
    with {:ok, hello_info} <- BoltProtocol.hello(transport, port, bolt_version, {}),
         {:ok, server_info} <- do_logon(transport, port, bolt_version, auth) do
      hints = extract_server_hints(hello_info)
      {:ok, server_info, hints}
    end
  end

  # v4+ uses HELLO with auth (same as v3)
  defp do_init(transport, port, bolt_version, auth) when is_tuple(bolt_version) do
    case BoltProtocol.hello(transport, port, bolt_version, auth) do
      {:ok, server_info} ->
        hints = extract_server_hints(server_info)
        {:ok, server_info, hints}

      error ->
        error
    end
  end

  defp do_init(transport, port, bolt_version, auth) when bolt_version >= 3 do
    case BoltProtocol.hello(transport, port, bolt_version, auth) do
      {:ok, server_info} ->
        hints = extract_server_hints(server_info)
        {:ok, server_info, hints}

      error ->
        error
    end
  end

  defp do_init(transport, port, bolt_version, auth) do
    case BoltProtocol.init(transport, port, bolt_version, auth) do
      {:ok, server_info} ->
        # v1/v2 don't have hints
        {:ok, server_info, nil}

      error ->
        error
    end
  end

  # Extract server hints from HELLO/INIT response
  # Known hints:
  #   - "connection.recv_timeout_seconds": Recommended receive timeout
  #   - "telemetry.enabled": Whether telemetry is enabled on server
  #   - "ssr.enabled": Whether Server-Side Routing is enabled
  @spec extract_server_hints(map()) :: map()
  defp extract_server_hints(server_info) when is_map(server_info) do
    hint_keys = [
      "connection.recv_timeout_seconds",
      "telemetry.enabled",
      "ssr.enabled",
      "hints"
    ]

    # Extract known hint keys and any nested hints map
    hints =
      server_info
      |> Map.take(hint_keys)
      |> Map.merge(Map.get(server_info, "hints", %{}))

    if map_size(hints) > 0, do: hints, else: nil
  end

  defp extract_server_hints(_), do: nil

  # LOGON for v5.1+ - sends authentication credentials
  defp do_logon(transport, port, bolt_version, auth) do
    alias Bolt.Sips.Internals.BoltProtocolHelper

    BoltProtocolHelper.send_message(transport, port, bolt_version, {:logon, [auth]})

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, []) do
      {:success, info} ->
        {:ok, info}

      {:failure, response} ->
        {:error, BoltError.exception(response, port, :logon)}

      other ->
        {:error, BoltError.exception(other, port, :logon)}
    end
  end

  @doc "Callback for DBConnection.checkout/1"
  def checkout(%ConnData{sock: sock, configuration: conf} = conn_data) do
    case conf[:socket].setopts(sock, active: false) do
      :ok -> {:ok, conn_data}
      other -> other
    end
  end

  @doc "Callback for DBConnection.checkin/1"
  def checkin(%ConnData{sock: sock, configuration: conf} = conn_data) do
    case conf[:socket].setopts(sock, active: :once) do
      :ok -> {:ok, conn_data}
      other -> other
    end
  end

  # v4+ and v3 use GOODBYE
  def disconnect(_err, %ConnData{sock: sock, bolt_version: bolt_version, configuration: conf})
      when is_tuple(bolt_version) or bolt_version >= 3 do
    socket = conf[:socket]

    # Send GOODBYE message, but don't require success
    # The socket may already be closed by the server (e.g., Neo4j Aura idle timeout)
    try do
      BoltProtocol.goodbye(socket, sock, bolt_version)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    # Close the socket, ignoring errors if already closed
    try do
      socket.close(sock)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    :ok
  end

  @doc "Callback for DBConnection.disconnect/1"
  def disconnect(_err, %ConnData{sock: sock, configuration: conf}) do
    # Close the socket, ignoring errors if already closed
    try do
      conf[:socket].close(sock)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    :ok
  end

  # Catch-all handler for unexpected state formats
  # This can happen when:
  # - Connection was never fully established
  # - DBConnection passes options instead of state during certain error scenarios
  # - State was corrupted during a connection error
  def disconnect(reason, state) do
    state_type =
      cond do
        is_nil(state) -> :nil
        is_map(state) and Map.has_key?(state, :__struct__) -> state.__struct__
        is_map(state) -> :map
        is_list(state) -> :keyword_list
        true -> :unknown
      end

    Logger.warning(
      "[Bolt.Sips.Protocol] disconnect called with unexpected state format. " <>
        "Reason: #{inspect(reason)}, State type: #{inspect(state_type)}"
    )

    # Attempt to extract and close socket if present
    try do
      socket_module = extract_socket_module(state)
      sock = extract_sock(state)

      if socket_module && sock do
        socket_module.close(sock)
      end
    rescue
      _ -> :ok
    end

    :ok
  end

  # Helper to extract socket module from various state formats
  defp extract_socket_module(%{configuration: conf}) when is_list(conf),
    do: Keyword.get(conf, :socket)

  defp extract_socket_module(%{configuration: conf}) when is_map(conf),
    do: Map.get(conf, :socket)

  defp extract_socket_module(%{socket: socket}), do: socket
  defp extract_socket_module(state) when is_list(state), do: Keyword.get(state, :socket)
  defp extract_socket_module(state) when is_map(state), do: Map.get(state, :socket)
  defp extract_socket_module(_), do: nil

  # Helper to extract socket from various state formats
  defp extract_sock(%{sock: sock}), do: sock
  defp extract_sock(_), do: nil

  @doc "Callback for DBConnection.handle_begin/1"
  # Already in a transaction — increment depth without sending BEGIN to Neo4j.
  # This supports nested Bolt.Sips.transaction calls inside a sandbox.
  def handle_begin(_, %ConnData{status: :transaction, tx_depth: depth} = conn_data) do
    {:ok, :began, %{conn_data | tx_depth: depth + 1}}
  end

  # v4+ and v3 use BEGIN message
  def handle_begin(_, %ConnData{sock: sock, bolt_version: bolt_version, configuration: conf} = conn_data)
      when is_tuple(bolt_version) or bolt_version >= 3 do
    socket = conf[:socket]

    case BoltProtocol.begin(socket, sock, bolt_version) do
      {:ok, _} ->
        {:ok, :began, %{conn_data | status: :transaction, tx_depth: 1}}

      {:error, %BoltError{type: :connection_error} = error} ->
        {:disconnect, error, conn_data}

      {:error, %BoltError{} = error} ->
        # Reset connection state on failure
        BoltProtocol.reset(socket, sock, bolt_version)
        {:error, error, conn_data}
    end
  end

  def handle_begin(_opts, conn_data) do
    %QueryStatement{statement: "BEGIN"}
    |> handle_execute(%{}, [], conn_data)

    {:ok, :began, %{conn_data | status: :transaction, tx_depth: 1}}
  end

  @doc "Callback for DBConnection.handle_rollback/1"
  # Nested transaction — decrement depth without sending ROLLBACK to Neo4j.
  def handle_rollback(_, %ConnData{tx_depth: depth} = conn_data) when depth > 1 do
    {:ok, :rolledback, %{conn_data | tx_depth: depth - 1}}
  end

  # v4+ and v3 use ROLLBACK message
  def handle_rollback(_, %ConnData{sock: sock, bolt_version: bolt_version, configuration: conf} = conn_data)
      when is_tuple(bolt_version) or bolt_version >= 3 do
    socket = conf[:socket]

    case BoltProtocol.rollback(socket, sock, bolt_version) do
      :ok ->
        {:ok, :rolledback, %{conn_data | status: :idle, tx_depth: 0}}

      %BoltError{type: :connection_error} = error ->
        {:disconnect, error, conn_data}

      %BoltError{} = error ->
        # Reset connection state on failure
        BoltProtocol.reset(socket, sock, bolt_version)
        {:error, error, conn_data}
    end
  end

  def handle_rollback(_opts, conn_data) do
    %QueryStatement{statement: "ROLLBACK"}
    |> handle_execute(%{}, [], conn_data)

    {:ok, :rolledback, %{conn_data | status: :idle, tx_depth: 0}}
  end

  @doc "Callback for DBConnection.handle_commit/1"
  # Nested transaction — decrement depth without sending COMMIT to Neo4j.
  def handle_commit(_, %ConnData{tx_depth: depth} = conn_data) when depth > 1 do
    {:ok, :committed, %{conn_data | tx_depth: depth - 1}}
  end

  # v4+ and v3 use COMMIT message
  def handle_commit(_, %ConnData{sock: sock, bolt_version: bolt_version, configuration: conf} = conn_data)
      when is_tuple(bolt_version) or bolt_version >= 3 do
    socket = conf[:socket]

    case BoltProtocol.commit(socket, sock, bolt_version) do
      {:ok, _} ->
        {:ok, :committed, %{conn_data | status: :idle, tx_depth: 0}}

      {:error, %BoltError{type: :connection_error} = error} ->
        {:disconnect, error, conn_data}

      {:error, %BoltError{} = error} ->
        # Reset connection state on failure
        BoltProtocol.reset(socket, sock, bolt_version)
        {:error, error, conn_data}
    end
  end

  def handle_commit(_opts, conn_data) do
    %QueryStatement{statement: "COMMIT"}
    |> handle_execute(%{}, [], conn_data)

    {:ok, :committed, %{conn_data | status: :idle, tx_depth: 0}}
  end

  @doc "Callback for DBConnection.handle_execute/1"
  def handle_execute(query, params, opts, conn_data) do
    execute(query, params, opts, conn_data)
  end

  # Handle server-initiated connection close while idle in pool.
  # With active: :once set during checkin, we receive these messages
  # when Neo4j closes the connection (idle timeout, restart, etc.).
  # Returning {:disconnect, ...} tells DBConnection to remove this
  # connection and replace it with a fresh one immediately.
  def handle_info({:tcp_closed, _sock}, state) do
    {:disconnect, :tcp_closed, state}
  end

  def handle_info({:tcp_error, _sock, reason}, state) do
    {:disconnect, {:tcp_error, reason}, state}
  end

  def handle_info({:ssl_closed, _sock}, state) do
    {:disconnect, :ssl_closed, state}
  end

  def handle_info({:ssl_error, _sock, reason}, state) do
    {:disconnect, {:ssl_error, reason}, state}
  end

  def handle_info(msg, state) do
    Logger.warning(fn ->
      [inspect(__MODULE__), ?\s, inspect(self()), " received unexpected message: " | inspect(msg)]
    end)

    {:ok, state}
  end

  ### Calming the warnings
  # Callbacks for ...

  @doc "Callback for DBConnection.ping/1"
  def ping(%ConnData{sock: nil} = state) do
    {:disconnect, :stale_connection, state}
  end

  def ping(%ConnData{sock: sock, bolt_version: bolt_version, configuration: conf} = state) do
    socket = conf[:socket]
    ping_timeout = Keyword.get(conf, :ping_timeout, 5_000)

    try do
      case BoltProtocol.reset(socket, sock, bolt_version, recv_timeout: ping_timeout) do
        :ok -> {:ok, state}
        _ -> {:disconnect, :stale_connection, state}
      end
    rescue
      _ -> {:disconnect, :stale_connection, state}
    catch
      _, _ -> {:disconnect, :stale_connection, state}
    end
  end
  def handle_prepare(query, _opts, state), do: {:ok, query, state}
  def handle_close(query, _opts, state), do: {:ok, query, state}
  def handle_deallocate(query, _cursor, _opts, state), do: {:ok, query, state}
  def handle_declare(query, _params, _opts, state), do: {:ok, query, state, nil}
  def handle_fetch(query, _cursor, _opts, state), do: {:cont, query, state}
  def handle_status(_opts, %ConnData{status: status} = state), do: {status, state}
  def handle_status(_opts, state), do: {:idle, state}

  defp extract_auth(nil), do: {}

  defp extract_auth(basic_auth), do: {basic_auth[:username], basic_auth[:password]}

  defp execute(q, params, _, conn_data) do
    %QueryStatement{statement: statement} = q
    %ConnData{sock: sock, bolt_version: bolt_version, configuration: conf} = conn_data
    socket = conf |> Keyword.get(:socket)

    case BoltProtocol.run_statement(socket, sock, bolt_version, statement, params) do
      [{:success, _} | _] = data ->
        {:ok, q, data, conn_data}

      # IGNORED means the server is in FAILED state - reset and return error
      [{:ignored, _} | _] ->
        BoltProtocol.reset(socket, sock, bolt_version)
        error = BoltError.exception("Server in FAILED state, connection has been reset", sock, :ignored)
        {:error, error, conn_data}

      {:ignored, _} ->
        BoltProtocol.reset(socket, sock, bolt_version)
        error = BoltError.exception("Server in FAILED state, connection has been reset", sock, :ignored)
        {:error, error, conn_data}

      %BoltError{type: :cypher_error} = error ->
        BoltProtocol.reset(socket, sock, bolt_version)
        {:error, error, conn_data}

      %BoltError{type: :protocol_error} = error ->
        # Protocol errors also require RESET to recover
        BoltProtocol.reset(socket, sock, bolt_version)
        {:error, error, conn_data}

      %BoltError{type: :connection_error} = error ->
        {:disconnect, error, conn_data}

      %BoltError{} = error ->
        # For any other error type, attempt RESET to ensure clean state
        BoltProtocol.reset(socket, sock, bolt_version)
        {:error, error, conn_data}
    end
  rescue
    e ->
      %ConnData{sock: sock, bolt_version: bolt_version, configuration: conf} = conn_data
      socket = conf |> Keyword.get(:socket)

      # Attempt to reset connection state after exception
      try do
        BoltProtocol.reset(socket, sock, bolt_version)
      rescue
        _ -> :ok
      end

      msg =
        case e do
          %Bolt.Sips.Internals.PackStreamError{data: data} ->
            "unable to encode value: #{inspect(data)}"

          %BoltError{message: message, type: type} ->
            "#{message}, type: #{type}"

          _ ->
            Exception.message(e)
        end

      {:error, %{code: :failure, message: msg}, conn_data}
  end

  # Convert hostname to the correct format for :gen_tcp.connect
  # IP addresses must be tuples (e.g., {127, 0, 0, 1}), not charlists
  # Hostnames should be charlists for DNS resolution
  #
  # In OTP 27+, passing an IP address as a charlist may trigger DNS lookup
  # which fails with :nxdomain. This function ensures IP addresses are
  # passed as tuples to avoid DNS lookup.
  defp _to_hostname(hostname) when is_binary(hostname) and byte_size(hostname) > 0 do
    charlist = String.to_charlist(hostname)
    case :inet.parse_address(charlist) do
      {:ok, ip_tuple} -> ip_tuple  # IP address - use tuple
      {:error, _} -> charlist       # Hostname - use charlist for DNS
    end
  end

  defp _to_hostname(hostname) when is_list(hostname) and length(hostname) > 0 do
    case :inet.parse_address(hostname) do
      {:ok, ip_tuple} -> ip_tuple  # IP address - use tuple
      {:error, _} -> hostname       # Hostname - keep as charlist
    end
  end

  defp _to_hostname(hostname) when is_tuple(hostname), do: hostname  # Already a tuple

  # Handle nil, empty string, empty list - fall back to localhost
  # This can happen if config is not properly loaded during application boot
  defp _to_hostname(_) do
    Logger.warning("[Bolt.Sips] hostname is nil or empty, falling back to localhost")
    ~c"localhost"
  end
end
