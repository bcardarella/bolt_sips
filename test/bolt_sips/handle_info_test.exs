defmodule Bolt.Sips.HandleInfoTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bolt.Sips.Protocol
  alias Bolt.Sips.Protocol.ConnData

  @conn_data %ConnData{
    sock: nil,
    bolt_version: {5, 6},
    configuration: [socket: Bolt.Sips.Socket],
    server_hints: nil
  }

  describe "handle_info/2 TCP close" do
    test "disconnects on tcp_closed" do
      result = Protocol.handle_info({:tcp_closed, :dummy_port}, @conn_data)
      assert {:disconnect, :tcp_closed, %ConnData{}} = result
    end

    test "disconnects on tcp_error" do
      result = Protocol.handle_info({:tcp_error, :dummy_port, :econnreset}, @conn_data)
      assert {:disconnect, {:tcp_error, :econnreset}, %ConnData{}} = result
    end

    test "preserves state through tcp_closed disconnect" do
      {:disconnect, _, returned_state} = Protocol.handle_info({:tcp_closed, :dummy_port}, @conn_data)
      assert returned_state == @conn_data
    end
  end

  describe "handle_info/2 SSL close" do
    test "disconnects on ssl_closed" do
      result = Protocol.handle_info({:ssl_closed, :dummy_port}, @conn_data)
      assert {:disconnect, :ssl_closed, %ConnData{}} = result
    end

    test "disconnects on ssl_error" do
      result = Protocol.handle_info({:ssl_error, :dummy_port, :closed}, @conn_data)
      assert {:disconnect, {:ssl_error, :closed}, %ConnData{}} = result
    end
  end

  describe "handle_info/2 unexpected messages" do
    test "returns ok and logs warning for unknown messages" do
      log =
        capture_log(fn ->
          result = Protocol.handle_info({:unexpected, :data}, @conn_data)
          assert {:ok, %ConnData{}} = result
        end)

      assert log =~ "received unexpected message"
      assert log =~ "unexpected"
    end

    test "does not disconnect for unknown messages" do
      capture_log(fn ->
        {status, _} = Protocol.handle_info(:random_atom, @conn_data)
        assert status == :ok
      end)
    end
  end
end
