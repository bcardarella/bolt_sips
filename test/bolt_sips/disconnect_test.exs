defmodule Bolt.Sips.DisconnectTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bolt.Sips.Protocol
  alias Bolt.Sips.Protocol.ConnData

  describe "disconnect/2 handler robustness" do
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
