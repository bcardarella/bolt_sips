defmodule Bolt.Sips.Internals.BoltProtocolV4 do
  @moduledoc """
  Bolt Protocol v4+ implementation.

  Key changes from v3:
  - PULL_ALL replaced with PULL (n, qid parameters)
  - DISCARD_ALL replaced with DISCARD (n, qid parameters)
  - Database name support in RUN and BEGIN
  - ROUTE message for routing queries (v4.3+)
  """

  alias Bolt.Sips.Internals.BoltProtocolHelper
  alias Bolt.Sips.Internals.Error
  alias Bolt.Sips.Metadata

  @doc """
  Implementation of Bolt's HELLO for v4+.

  In v4+, HELLO can include routing context for cluster routing.

  ## Options

  See "Shared options" in `Bolt.Sips.Internals.BoltProtocolHelper` documentation.

  ## Examples

      iex> Bolt.Sips.Internals.BoltProtocolV4.hello(:gen_tcp, port, {4, 4}, {}, [])
      {:ok, info}

      iex> Bolt.Sips.Internals.BoltProtocolV4.hello(:gen_tcp, port, {4, 4}, {"username", "password"}, [])
      {:ok, info}
  """
  @spec hello(atom(), port(), integer() | {integer(), integer()}, tuple(), Keyword.t()) ::
          {:ok, any()} | {:error, Bolt.Sips.Internals.Error.t()}
  def hello(transport, port, bolt_version, auth, options \\ [recv_timeout: 15_000]) do
    BoltProtocolHelper.send_message(transport, port, bolt_version, {:hello, [auth]})

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, options) do
      {:success, info} ->
        {:ok, info}

      {:failure, response} ->
        {:error, Error.exception(response, port, :hello)}

      other ->
        {:error, Error.exception(other, port, :hello)}
    end
  end

  @doc """
  Implementation of Bolt's LOGON for v5.1+.

  In v5.1+, authentication is separated from HELLO. LOGON sends credentials
  after HELLO has established the connection.

  ## Options

  See "Shared options" in `Bolt.Sips.Internals.BoltProtocolHelper` documentation.

  ## Examples

      iex> BoltProtocolV4.logon(:gen_tcp, port, {5, 1}, {"username", "password"}, [])
      {:ok, info}
  """
  @spec logon(atom(), port(), integer() | {integer(), integer()}, tuple(), Keyword.t()) ::
          {:ok, any()} | {:error, Bolt.Sips.Internals.Error.t()}
  def logon(transport, port, bolt_version, auth, options \\ [recv_timeout: 15_000]) do
    BoltProtocolHelper.send_message(transport, port, bolt_version, {:logon, [auth]})

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, options) do
      {:success, info} ->
        {:ok, info}

      {:failure, response} ->
        {:error, Error.exception(response, port, :logon)}

      other ->
        {:error, Error.exception(other, port, :logon)}
    end
  end

  @doc """
  Implementation of Bolt's LOGOFF for v5.1+.

  LOGOFF logs out the current user without closing the connection.
  After LOGOFF, the connection returns to the AUTHENTICATION state
  and can accept a new LOGON message.

  ## Options

  See "Shared options" in `Bolt.Sips.Internals.BoltProtocolHelper` documentation.

  ## Examples

      iex> BoltProtocolV4.logoff(:gen_tcp, port, {5, 1}, [])
      :ok
  """
  @spec logoff(atom(), port(), integer() | {integer(), integer()}, Keyword.t()) ::
          :ok | {:error, Bolt.Sips.Internals.Error.t()}
  def logoff(transport, port, bolt_version, options \\ [recv_timeout: 15_000]) do
    BoltProtocolHelper.send_message(transport, port, bolt_version, {:logoff, []})

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, options) do
      {:success, _} ->
        :ok

      {:failure, response} ->
        {:error, Error.exception(response, port, :logoff)}

      other ->
        {:error, Error.exception(other, port, :logoff)}
    end
  end

  @doc """
  Implementation of Bolt's TELEMETRY for v5.4+.

  TELEMETRY sends driver analytics data to the server.

  ## Parameters

  - api: Integer representing the API being used

  ## Options

  See "Shared options" in `Bolt.Sips.Internals.BoltProtocolHelper` documentation.
  """
  @spec telemetry(atom(), port(), integer() | {integer(), integer()}, integer(), Keyword.t()) ::
          :ok | {:error, Bolt.Sips.Internals.Error.t()}
  def telemetry(transport, port, bolt_version, api, options \\ [recv_timeout: 15_000]) do
    BoltProtocolHelper.send_message(transport, port, bolt_version, {:telemetry, [api]})

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, options) do
      {:success, _} ->
        :ok

      {:failure, response} ->
        {:error, Error.exception(response, port, :telemetry)}

      other ->
        {:error, Error.exception(other, port, :telemetry)}
    end
  end

  @doc """
  Implementation of Bolt's GOODBYE for v4+.
  """
  def goodbye(transport, port, bolt_version) do
    BoltProtocolHelper.send_message(transport, port, bolt_version, {:goodbye, []})

    try do
      Port.close(port)
      :ok
    rescue
      ArgumentError -> Error.exception("Can't close port", port, :goodbye)
    end
  end

  @doc """
  Implementation of Bolt's RUN for v4+.

  In v4+, RUN supports additional metadata including database name.

  ## Options

  See "Shared options" in `Bolt.Sips.Internals.BoltProtocolHelper` documentation.
  """
  @spec run(
          atom(),
          port(),
          integer() | {integer(), integer()},
          String.t(),
          map(),
          Metadata.t() | map(),
          Keyword.t()
        ) ::
          {:ok, any()} | {:error, Bolt.Sips.Internals.Error.t()}
  def run(transport, port, bolt_version, statement, params, metadata, options) do
    extra =
      case metadata do
        %Metadata{} -> Metadata.to_map(metadata)
        %{} = map -> map
      end

    BoltProtocolHelper.send_message(
      transport,
      port,
      bolt_version,
      {:run, [statement, params, extra]}
    )

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, options) do
      {:success, _} = result ->
        {:ok, result}

      {:failure, response} ->
        {:error, Error.exception(response, port, :run)}

      %Error{} = error ->
        {:error, error}

      other ->
        {:error, Error.exception(other, port, :run)}
    end
  end

  @doc """
  Implementation of Bolt's PULL for v4+ (replaces PULL_ALL).

  PULL takes an extra map with:
  - n: number of records to fetch (-1 for all)
  - qid: query ID for explicit transactions (-1 for last statement)

  ## Options

  See "Shared options" in `Bolt.Sips.Internals.BoltProtocolHelper` documentation.

  ## Example

      iex> BoltProtocolV4.pull(:gen_tcp, port, {4, 4}, %{n: -1}, [])
      {:ok, [record: [5], success: %{"type" => "r"}]}

      iex> BoltProtocolV4.pull(:gen_tcp, port, {4, 4}, %{n: 100}, [])
      {:ok, [record: [...], success: %{"has_more" => true}]}
  """
  @spec pull(atom(), port(), integer() | {integer(), integer()}, map(), Keyword.t()) ::
          {:ok, list()} | {:failure, Error.t()} | {:error, Error.t()}
  def pull(transport, port, bolt_version, extra \\ %{n: -1}, options) do
    BoltProtocolHelper.send_message(transport, port, bolt_version, {:pull, [extra]})

    with data <- BoltProtocolHelper.receive_data(transport, port, bolt_version, options),
         data <- List.wrap(data),
         {:success, _} <- List.last(data) do
      {:ok, data}
    else
      {:failure, response} ->
        {:failure, Error.exception(response, port, :pull)}

      other ->
        {:error, Error.exception(other, port, :pull)}
    end
  end

  @doc """
  Convenience function that fetches all records (n=-1).
  """
  @spec pull_all(atom(), port(), integer() | {integer(), integer()}, Keyword.t()) ::
          {:ok, list()} | {:failure, Error.t()} | {:error, Error.t()}
  def pull_all(transport, port, bolt_version, options) do
    pull(transport, port, bolt_version, %{n: -1}, options)
  end

  @doc """
  Implementation of Bolt's DISCARD for v4+ (replaces DISCARD_ALL).

  DISCARD takes an extra map with:
  - n: number of records to discard (-1 for all)
  - qid: query ID for explicit transactions (-1 for last statement)

  ## Options

  See "Shared options" in `Bolt.Sips.Internals.BoltProtocolHelper` documentation.
  """
  @spec discard(atom(), port(), integer() | {integer(), integer()}, map(), Keyword.t()) ::
          :ok | Error.t()
  def discard(transport, port, bolt_version, extra \\ %{n: -1}, options) do
    BoltProtocolHelper.send_message(transport, port, bolt_version, {:discard, [extra]})

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, options) do
      {:success, _} ->
        :ok

      {:failure, response} ->
        Error.exception(response, port, :discard)

      other ->
        Error.exception(other, port, :discard)
    end
  end

  @doc """
  Convenience function that discards all records (n=-1).
  """
  @spec discard_all(atom(), port(), integer() | {integer(), integer()}, Keyword.t()) ::
          :ok | Error.t()
  def discard_all(transport, port, bolt_version, options) do
    discard(transport, port, bolt_version, %{n: -1}, options)
  end

  @doc """
  Runs a statement and returns records (RUN + PULL).

  ## Options

  See "Shared options" in `Bolt.Sips.Internals.BoltProtocolHelper` documentation.
  """
  @spec run_statement(
          atom(),
          port(),
          integer() | {integer(), integer()},
          String.t(),
          map(),
          Metadata.t() | map(),
          Keyword.t()
        ) ::
          [Bolt.Sips.Internals.PackStream.Message.decoded()]
          | Bolt.Sips.Internals.Error.t()
  def run_statement(transport, port, bolt_version, statement, params, metadata, options) do
    with {:ok, run_data} <-
           run(transport, port, bolt_version, statement, params, metadata, options),
         {:ok, result} <- pull_all(transport, port, bolt_version, options) do
      [run_data | result]
    else
      {:error, %Error{} = error} ->
        error

      other ->
        Error.exception(other, port, :run_statement)
    end
  end

  @doc """
  Implementation of Bolt's BEGIN for v4+.

  In v4+, BEGIN supports database name in metadata.

  ## Options

  See "Shared options" in `Bolt.Sips.Internals.BoltProtocolHelper` documentation.
  """
  @spec begin(
          atom(),
          port(),
          integer() | {integer(), integer()},
          Metadata.t() | map(),
          Keyword.t()
        ) ::
          {:ok, any()} | {:error, Error.t()}
  def begin(transport, port, bolt_version, metadata, options) do
    extra =
      case metadata do
        %Metadata{} -> Metadata.to_map(metadata)
        %{} = map -> map
      end

    BoltProtocolHelper.send_message(transport, port, bolt_version, {:begin, [extra]})

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, options) do
      {:success, info} ->
        {:ok, info}

      {:failure, response} ->
        {:error, Error.exception(response, port, :begin)}

      other ->
        {:error, Error.exception(other, port, :begin)}
    end
  end

  @doc """
  Implementation of Bolt's COMMIT for v4+.
  """
  @spec commit(atom(), port(), integer() | {integer(), integer()}, Keyword.t()) ::
          {:ok, any()} | {:error, Error.t()}
  def commit(transport, port, bolt_version, options) do
    BoltProtocolHelper.send_message(transport, port, bolt_version, {:commit, []})

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, options) do
      {:success, info} ->
        {:ok, info}

      {:failure, response} ->
        {:error, Error.exception(response, port, :commit)}

      other ->
        {:error, Error.exception(other, port, :commit)}
    end
  end

  @doc """
  Implementation of Bolt's ROLLBACK for v4+.
  """
  @spec rollback(atom(), port(), integer() | {integer(), integer()}, Keyword.t()) ::
          :ok | Error.t()
  def rollback(transport, port, bolt_version, options) do
    BoltProtocolHelper.treat_simple_message(:rollback, transport, port, bolt_version, options)
  end

  @doc """
  Implementation of Bolt's RESET for v4+.
  """
  @spec reset(atom(), port(), integer() | {integer(), integer()}, Keyword.t()) ::
          :ok | Error.t()
  def reset(transport, port, bolt_version, options) do
    BoltProtocolHelper.treat_simple_message(:reset, transport, port, bolt_version, options)
  end

  @doc """
  Implementation of Bolt's ROUTE for v4.3+.

  ROUTE queries the routing table for cluster routing.

  ## Parameters

  - routing_context: Map with routing context (e.g., %{address: "localhost:7687"})
  - bookmarks: List of bookmarks
  - database: Database name (optional, nil for default)
  """
  @spec route(
          atom(),
          port(),
          integer() | {integer(), integer()},
          map(),
          list(),
          String.t() | nil,
          Keyword.t()
        ) ::
          {:ok, any()} | {:error, Error.t()}
  def route(transport, port, bolt_version, routing_context, bookmarks, database, options) do
    BoltProtocolHelper.send_message(
      transport,
      port,
      bolt_version,
      {:route, [routing_context, bookmarks, database]}
    )

    case BoltProtocolHelper.receive_data(transport, port, bolt_version, options) do
      {:success, info} ->
        {:ok, info}

      {:failure, response} ->
        {:error, Error.exception(response, port, :route)}

      other ->
        {:error, Error.exception(other, port, :route)}
    end
  end
end
