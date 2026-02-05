defmodule Bolt.Sips.PingTest do
  use ExUnit.Case, async: false

  alias Bolt.Sips.Protocol
  alias Bolt.Sips.Protocol.ConnData

  # Helper: DBConnection calls checkout before ping, so we must too.
  # checkout sets the socket to active: false (blocking recv mode).
  defp checkout_then_ping(conn_data) do
    {:ok, conn_data} = Protocol.checkout(conn_data)
    Protocol.ping(conn_data)
  end

  describe "ping/1 with live connection" do
    test "returns {:ok, state} for a healthy connection" do
      {:ok, %ConnData{} = conn_data} = Protocol.connect([])

      assert {:ok, %ConnData{}} = checkout_then_ping(conn_data)

      Protocol.disconnect(:stop, conn_data)
    end

    test "returns {:disconnect, _, state} when socket is closed" do
      {:ok, %ConnData{sock: sock, configuration: conf} = conn_data} = Protocol.connect([])

      # Simulate server-side close (Neo4j Aura idle timeout)
      conf[:socket].close(sock)

      assert {:disconnect, _reason, %ConnData{}} = Protocol.ping(conn_data)
    end

    test "ping completes quickly on a healthy connection" do
      {:ok, %ConnData{} = conn_data} = Protocol.connect([])

      {time_us, result} = :timer.tc(fn ->
        checkout_then_ping(conn_data)
      end)

      time_ms = div(time_us, 1_000)

      assert {:ok, %ConnData{}} = result
      assert time_ms < 5_000, "ping took #{time_ms}ms, expected < 5000ms"

      Protocol.disconnect(:stop, conn_data)
    end

    test "ping on closed socket fails fast, not blocking for recv_timeout" do
      {:ok, %ConnData{sock: sock, configuration: conf} = conn_data} = Protocol.connect([])

      conf[:socket].close(sock)

      {time_us, _result} = :timer.tc(fn ->
        Protocol.ping(conn_data)
      end)

      time_ms = div(time_us, 1_000)

      # Should fail fast, not block for 30s recv_timeout
      assert time_ms < 5_000,
        "ping on closed socket took #{time_ms}ms, expected fast failure"
    end
  end

  describe "ping/1 with invalid state" do
    test "returns {:disconnect, _, state} when socket is nil" do
      conn_data = %ConnData{
        sock: nil,
        bolt_version: {5, 4},
        configuration: [socket: Bolt.Sips.Socket],
        server_hints: nil
      }

      assert {:disconnect, _reason, %ConnData{}} = Protocol.ping(conn_data)
    end

    test "returns {:disconnect, _, state} when socket is nil with v3" do
      conn_data = %ConnData{
        sock: nil,
        bolt_version: 3,
        configuration: [socket: Bolt.Sips.Socket],
        server_hints: nil
      }

      assert {:disconnect, _reason, %ConnData{}} = Protocol.ping(conn_data)
    end

    test "returns {:disconnect, _, state} when socket is nil with v1" do
      conn_data = %ConnData{
        sock: nil,
        bolt_version: 1,
        configuration: [socket: Bolt.Sips.Socket],
        server_hints: nil
      }

      assert {:disconnect, _reason, %ConnData{}} = Protocol.ping(conn_data)
    end
  end
end
