defmodule Bolt.Sips.Internals.ConnectionHealthTest do
  use ExUnit.Case, async: true

  alias Bolt.Sips.Internals.ConnectionHealth

  describe "new/2" do
    test "creates a new circuit breaker with defaults" do
      circuit = ConnectionHealth.new("localhost:7687")

      assert circuit.url == "localhost:7687"
      assert circuit.state == :closed
      assert circuit.failure_count == 0
      assert circuit.failure_threshold == 5
    end

    test "creates a new circuit breaker with custom options" do
      circuit = ConnectionHealth.new("localhost:7687",
        failure_threshold: 10,
        recovery_timeout: 60_000
      )

      assert circuit.failure_threshold == 10
      assert circuit.recovery_timeout == 60_000
    end
  end

  describe "allow?/1" do
    test "allows requests when circuit is closed" do
      circuit = ConnectionHealth.new("localhost:7687")

      assert {:ok, ^circuit} = ConnectionHealth.allow?(circuit)
    end

    test "allows requests when circuit is half-open" do
      circuit = %ConnectionHealth{
        url: "localhost:7687",
        state: :half_open
      }

      assert {:ok, ^circuit} = ConnectionHealth.allow?(circuit)
    end

    test "blocks requests when circuit is open and recovery timeout not passed" do
      circuit = %ConnectionHealth{
        url: "localhost:7687",
        state: :open,
        last_failure_time: System.monotonic_time(:millisecond),
        recovery_timeout: 30_000
      }

      assert {:error, :circuit_open} = ConnectionHealth.allow?(circuit)
    end

    test "transitions to half-open when recovery timeout passed" do
      # Set last failure time to be in the past
      past_time = System.monotonic_time(:millisecond) - 40_000
      circuit = %ConnectionHealth{
        url: "localhost:7687",
        state: :open,
        last_failure_time: past_time,
        recovery_timeout: 30_000
      }

      assert {:ok, updated_circuit} = ConnectionHealth.allow?(circuit)
      assert updated_circuit.state == :half_open
    end
  end

  describe "record_success/1" do
    test "resets circuit to closed state" do
      circuit = %ConnectionHealth{
        url: "localhost:7687",
        state: :half_open,
        failure_count: 3
      }

      updated = ConnectionHealth.record_success(circuit)

      assert updated.state == :closed
      assert updated.failure_count == 0
      assert updated.last_success_time != nil
    end
  end

  describe "record_failure/1" do
    test "increments failure count" do
      circuit = ConnectionHealth.new("localhost:7687")

      updated = ConnectionHealth.record_failure(circuit)

      assert updated.failure_count == 1
      assert updated.state == :closed
    end

    test "opens circuit when failure threshold reached" do
      circuit = %ConnectionHealth{
        url: "localhost:7687",
        state: :closed,
        failure_count: 4,
        failure_threshold: 5
      }

      updated = ConnectionHealth.record_failure(circuit)

      assert updated.failure_count == 5
      assert updated.state == :open
      assert updated.last_failure_time != nil
    end

    test "opens circuit immediately when half-open fails" do
      circuit = %ConnectionHealth{
        url: "localhost:7687",
        state: :half_open,
        failure_count: 0
      }

      updated = ConnectionHealth.record_failure(circuit)

      assert updated.state == :open
    end
  end

  describe "healthy?/1" do
    test "returns true when circuit is closed" do
      circuit = ConnectionHealth.new("localhost:7687")

      assert ConnectionHealth.healthy?(circuit)
    end

    test "returns true when circuit is half-open" do
      circuit = %ConnectionHealth{url: "localhost:7687", state: :half_open}

      assert ConnectionHealth.healthy?(circuit)
    end

    test "returns false when circuit is open" do
      circuit = %ConnectionHealth{url: "localhost:7687", state: :open}

      refute ConnectionHealth.healthy?(circuit)
    end
  end

  describe "calculate_delay/4" do
    test "calculates exponential backoff" do
      # Without jitter for predictable testing
      assert ConnectionHealth.calculate_delay(0, 100, 5000, false) == 100
      assert ConnectionHealth.calculate_delay(1, 100, 5000, false) == 200
      assert ConnectionHealth.calculate_delay(2, 100, 5000, false) == 400
      assert ConnectionHealth.calculate_delay(3, 100, 5000, false) == 800
    end

    test "respects max delay" do
      delay = ConnectionHealth.calculate_delay(10, 100, 1000, false)

      assert delay == 1000
    end

    test "adds jitter when enabled" do
      delay = ConnectionHealth.calculate_delay(0, 100, 5000, true)

      # With jitter, delay should be between 100 and 125
      assert delay >= 100
      assert delay <= 125
    end
  end

  describe "with_retry/2" do
    test "returns result immediately on success" do
      result = ConnectionHealth.with_retry(fn -> {:ok, :success} end)

      assert result == {:ok, :success}
    end

    test "does not retry non-transient errors" do
      counter = :counters.new(1, [:atomics])

      result = ConnectionHealth.with_retry(fn ->
        :counters.add(counter, 1, 1)
        {:error, :permanent_failure}
      end, max_retries: 3)

      assert result == {:error, :permanent_failure}
      # Should only be called once (no retries for non-transient errors)
      assert :counters.get(counter, 1) == 1
    end
  end

  describe "transient_error?/1" do
    test "identifies transient errors" do
      assert ConnectionHealth.transient_error?({:error, :timeout})
      assert ConnectionHealth.transient_error?({:error, :closed})
      assert ConnectionHealth.transient_error?({:error, :econnrefused})
      assert ConnectionHealth.transient_error?({:error, :econnreset})
      assert ConnectionHealth.transient_error?({:error, :ehostunreach})
      assert ConnectionHealth.transient_error?({:error, %{type: :connection_error}})
    end

    test "identifies non-transient errors" do
      refute ConnectionHealth.transient_error?({:error, :auth_failed})
      refute ConnectionHealth.transient_error?({:error, %{type: :cypher_error}})
      refute ConnectionHealth.transient_error?({:ok, :result})
    end
  end

  describe "check_health/1" do
    test "returns ok for alive process" do
      pid = spawn(fn -> Process.sleep(1000) end)

      assert :ok = ConnectionHealth.check_health(pid)
    end

    test "returns error for dead process" do
      pid = spawn(fn -> :ok end)
      # Wait for process to die
      Process.sleep(10)

      assert {:error, :not_alive} = ConnectionHealth.check_health(pid)
    end

    test "returns error for invalid pid" do
      assert {:error, :invalid_pid} = ConnectionHealth.check_health(:not_a_pid)
    end
  end
end
