defmodule Bolt.Sips.Internals.ConnectionHealth do
  @moduledoc """
  Connection health checking and circuit breaker for cluster connections.

  This module provides utilities for:
  - Checking connection health
  - Circuit breaker pattern for failing connections
  - Retry logic with exponential backoff
  """

  require Logger

  # Circuit breaker states
  @circuit_closed :closed       # Connection is healthy, allow requests
  @circuit_open :open           # Connection is failing, reject requests
  @circuit_half_open :half_open # Testing if connection recovered

  # Default configuration
  @default_failure_threshold 5          # Number of failures before opening circuit
  @default_recovery_timeout 30_000      # Time before trying half-open (ms)
  @default_health_check_interval 60_000 # Health check interval (ms)
  @default_max_retries 3                # Max retry attempts
  @default_base_delay 100               # Base delay for exponential backoff (ms)

  @doc """
  Circuit breaker state structure.
  """
  defstruct [
    :url,
    state: @circuit_closed,
    failure_count: 0,
    last_failure_time: nil,
    last_success_time: nil,
    failure_threshold: @default_failure_threshold,
    recovery_timeout: @default_recovery_timeout
  ]

  @type t :: %__MODULE__{
          url: String.t(),
          state: :closed | :open | :half_open,
          failure_count: non_neg_integer(),
          last_failure_time: integer() | nil,
          last_success_time: integer() | nil,
          failure_threshold: non_neg_integer(),
          recovery_timeout: non_neg_integer()
        }

  @doc """
  Create a new circuit breaker for a connection URL.
  """
  @spec new(String.t(), Keyword.t()) :: t()
  def new(url, opts \\ []) do
    %__MODULE__{
      url: url,
      failure_threshold: Keyword.get(opts, :failure_threshold, @default_failure_threshold),
      recovery_timeout: Keyword.get(opts, :recovery_timeout, @default_recovery_timeout)
    }
  end

  @doc """
  Check if a connection is allowed based on circuit breaker state.
  Returns `{:ok, circuit}` if allowed, `{:error, :circuit_open}` if blocked.
  """
  @spec allow?(t()) :: {:ok, t()} | {:error, :circuit_open}
  def allow?(%__MODULE__{state: @circuit_closed} = circuit), do: {:ok, circuit}

  def allow?(%__MODULE__{state: @circuit_half_open} = circuit), do: {:ok, circuit}

  def allow?(%__MODULE__{state: @circuit_open} = circuit) do
    now = System.monotonic_time(:millisecond)

    if now - (circuit.last_failure_time || 0) >= circuit.recovery_timeout do
      # Recovery timeout passed, try half-open
      {:ok, %{circuit | state: @circuit_half_open}}
    else
      {:error, :circuit_open}
    end
  end

  @doc """
  Record a successful operation, resetting the circuit breaker.
  """
  @spec record_success(t()) :: t()
  def record_success(circuit) do
    %{circuit |
      state: @circuit_closed,
      failure_count: 0,
      last_success_time: System.monotonic_time(:millisecond)
    }
  end

  @doc """
  Record a failed operation, potentially opening the circuit.
  """
  @spec record_failure(t()) :: t()
  def record_failure(%__MODULE__{state: @circuit_half_open} = circuit) do
    # Failure during half-open immediately opens circuit
    %{circuit |
      state: @circuit_open,
      failure_count: circuit.failure_count + 1,
      last_failure_time: System.monotonic_time(:millisecond)
    }
  end

  def record_failure(%__MODULE__{} = circuit) do
    new_count = circuit.failure_count + 1
    now = System.monotonic_time(:millisecond)

    if new_count >= circuit.failure_threshold do
      %{circuit |
        state: @circuit_open,
        failure_count: new_count,
        last_failure_time: now
      }
    else
      %{circuit |
        failure_count: new_count,
        last_failure_time: now
      }
    end
  end

  @doc """
  Check if a circuit is in a healthy state (closed or half-open).
  """
  @spec healthy?(t()) :: boolean()
  def healthy?(%__MODULE__{state: state}) do
    state in [@circuit_closed, @circuit_half_open]
  end

  @doc """
  Execute a function with retry logic and exponential backoff.

  ## Options

  - `:max_retries` - Maximum number of retry attempts (default: 3)
  - `:base_delay` - Base delay in milliseconds (default: 100)
  - `:max_delay` - Maximum delay in milliseconds (default: 5000)
  - `:jitter` - Add randomness to delay (default: true)
  """
  @spec with_retry((() -> result), Keyword.t()) :: result when result: any()
  def with_retry(fun, opts \\ []) do
    max_retries = Keyword.get(opts, :max_retries, @default_max_retries)
    base_delay = Keyword.get(opts, :base_delay, @default_base_delay)
    max_delay = Keyword.get(opts, :max_delay, 5_000)
    jitter = Keyword.get(opts, :jitter, true)

    do_with_retry(fun, 0, max_retries, base_delay, max_delay, jitter)
  end

  defp do_with_retry(fun, attempt, max_retries, _base_delay, _max_delay, _jitter) when attempt >= max_retries do
    fun.()
  end

  defp do_with_retry(fun, attempt, max_retries, base_delay, max_delay, jitter) do
    case fun.() do
      {:error, :transient_failure} = _error ->
        delay = calculate_delay(attempt, base_delay, max_delay, jitter)
        Logger.debug("Transient failure, retrying in #{delay}ms (attempt #{attempt + 1}/#{max_retries})")
        Process.sleep(delay)
        do_with_retry(fun, attempt + 1, max_retries, base_delay, max_delay, jitter)

      result ->
        result
    end
  end

  @doc """
  Calculate exponential backoff delay with optional jitter.
  """
  @spec calculate_delay(non_neg_integer(), non_neg_integer(), non_neg_integer(), boolean()) ::
          non_neg_integer()
  def calculate_delay(attempt, base_delay, max_delay, jitter) do
    # Exponential backoff: base_delay * 2^attempt
    delay = min(base_delay * :math.pow(2, attempt), max_delay) |> round()

    if jitter do
      # Add up to 25% random jitter
      jitter_amount = round(delay * 0.25 * :rand.uniform())
      delay + jitter_amount
    else
      delay
    end
  end

  @doc """
  Check if an error is transient and should be retried.
  """
  @spec transient_error?(any()) :: boolean()
  def transient_error?({:error, :timeout}), do: true
  def transient_error?({:error, :closed}), do: true
  def transient_error?({:error, :econnrefused}), do: true
  def transient_error?({:error, :econnreset}), do: true
  def transient_error?({:error, :ehostunreach}), do: true
  def transient_error?({:error, %{type: :connection_error}}), do: true
  def transient_error?(_), do: false

  @doc """
  Perform a health check on a connection.
  Returns `:ok` if healthy, `{:error, reason}` if not.
  """
  @spec check_health(pid()) :: :ok | {:error, any()}
  def check_health(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      :ok
    else
      {:error, :not_alive}
    end
  end

  def check_health(_), do: {:error, :invalid_pid}

  @doc """
  Get default health check interval.
  """
  @spec default_health_check_interval() :: non_neg_integer()
  def default_health_check_interval, do: @default_health_check_interval
end
