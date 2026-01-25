defmodule Bolt.Sips.DisconnectTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bolt.Sips.Protocol
  alias Bolt.Sips.Protocol.ConnData

  describe "disconnect/2 with v3+ (GOODBYE message)" do
    test "handles goodbye failure gracefully when server already closed connection" do
      # This simulates the scenario where Neo4j Aura closes an idle connection
      # before the client tries to disconnect. The GOODBYE message fails because
      # the socket is already closed.

      # Create a ConnData with a closed/invalid socket
      # We use a port that doesn't exist to simulate a closed connection
      conn_data = %ConnData{
        sock: nil,  # No valid socket - simulates closed connection
        bolt_version: {5, 6},
        configuration: [socket: Bolt.Sips.Socket],
        server_hints: nil
      }

      # This should NOT crash with MatchError
      # Previously: :ok = BoltProtocol.goodbye(...) would crash when goodbye returns error
      # The fix should ignore goodbye failures since we're disconnecting anyway
      result = Protocol.disconnect(:server_closed, conn_data)
      assert result == :ok
    end

    test "handles goodbye failure for v3 bolt version" do
      conn_data = %ConnData{
        sock: nil,
        bolt_version: 3,
        configuration: [socket: Bolt.Sips.Socket],
        server_hints: nil
      }

      result = Protocol.disconnect(:timeout, conn_data)
      assert result == :ok
    end

    test "handles goodbye failure for v4 tuple bolt version" do
      conn_data = %ConnData{
        sock: nil,
        bolt_version: {4, 4},
        configuration: [socket: Bolt.Sips.Socket],
        server_hints: nil
      }

      result = Protocol.disconnect(:normal, conn_data)
      assert result == :ok
    end
  end

  describe "disconnect/2 with v1/v2 (no GOODBYE)" do
    test "handles close failure gracefully" do
      conn_data = %ConnData{
        sock: nil,
        bolt_version: 2,
        configuration: [socket: Bolt.Sips.Socket],
        server_hints: nil
      }

      # v1/v2 doesn't send GOODBYE, just closes socket
      # Should handle socket.close failure gracefully
      result = Protocol.disconnect(:normal, conn_data)
      assert result == :ok
    end
  end

  describe "disconnect/2 handler robustness (unexpected state)" do
    test "handles unexpected map state gracefully" do
      # This is the bug case - when state is a map instead of ConnData struct
      # This can happen in certain DBConnection error scenarios
      map_state = %{port: 7687, hostname: "localhost", socket: Bolt.Sips.Socket}

      # Should not raise ArgumentError about Keyword.get
      # Should log a warning and return :ok
      log =
        capture_log(fn ->
          result = Protocol.disconnect(:connection_closed, map_state)
          assert result == :ok
        end)

      assert log =~ "[Bolt.Sips.Protocol] disconnect called with unexpected state format"
      assert log =~ "State type: :map"
    end

    test "handles unexpected keyword list state gracefully" do
      # Another potential edge case - keyword list instead of ConnData
      keyword_state = [port: 7687, hostname: "localhost", socket: Bolt.Sips.Socket]

      log =
        capture_log(fn ->
          result = Protocol.disconnect(:connection_closed, keyword_state)
          assert result == :ok
        end)

      assert log =~ "State type: :keyword_list"
    end

    test "handles nil state gracefully" do
      log =
        capture_log(fn ->
          result = Protocol.disconnect(:connection_closed, nil)
          assert result == :ok
        end)

      assert log =~ "State type: nil"
    end

    test "handles empty map state gracefully" do
      log =
        capture_log(fn ->
          result = Protocol.disconnect(:connection_closed, %{})
          assert result == :ok
        end)

      assert log =~ "State type: :map"
    end

    test "handles map with socket info and attempts cleanup" do
      # When state has socket info, the handler should attempt to close it
      map_state = %{
        sock: nil,  # No actual socket
        socket: Bolt.Sips.Socket,
        configuration: [socket: Bolt.Sips.Socket]
      }

      log =
        capture_log(fn ->
          result = Protocol.disconnect(:timeout, map_state)
          assert result == :ok
        end)

      assert log =~ "disconnect called with unexpected state format"
    end

    test "handles map with nested configuration as keyword list" do
      map_state = %{
        configuration: [socket: Bolt.Sips.Socket, hostname: "localhost"]
      }

      log =
        capture_log(fn ->
          result = Protocol.disconnect(:normal, map_state)
          assert result == :ok
        end)

      assert log =~ "State type: :map"
    end

    test "handles map with nested configuration as map" do
      map_state = %{
        configuration: %{socket: Bolt.Sips.Socket, hostname: "localhost"}
      }

      log =
        capture_log(fn ->
          result = Protocol.disconnect(:normal, map_state)
          assert result == :ok
        end)

      assert log =~ "State type: :map"
    end
  end
end
