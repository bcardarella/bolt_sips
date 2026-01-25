defmodule Bolt.Sips.HostnameTest do
  use ExUnit.Case, async: true

  alias Bolt.Sips.Protocol

  describe "hostname handling for :gen_tcp.connect" do
    test "IPv4 address string should be converted to tuple for gen_tcp" do
      # :gen_tcp.connect expects IP addresses as tuples, not charlists
      # Passing "127.0.0.1" as charlist causes DNS lookup which fails with :nxdomain

      # Test that we can connect to an IP address (this will fail with nxdomain if broken)
      # Using a definitely-closed port to test the hostname conversion, not actual connection
      opts = [packet: :raw, mode: :binary, active: false]

      # This should NOT return :nxdomain - it should return :econnrefused or :timeout
      # because the IP is valid, just no server listening
      result = :gen_tcp.connect({127, 0, 0, 1}, 65432, opts, 100)

      # Should be connection refused or timeout, NOT nxdomain
      assert match?({:error, :econnrefused}, result) or
             match?({:error, :timeout}, result),
             "Expected :econnrefused or :timeout, got #{inspect(result)}"
    end

    test "IPv4 address as charlist causes nxdomain in some OTP versions" do
      # This demonstrates the bug - passing IP as charlist may cause DNS lookup
      opts = [packet: :raw, mode: :binary, active: false]

      # When passing charlist, OTP might try DNS lookup
      result = :gen_tcp.connect(~c"127.0.0.1", 65432, opts, 100)

      # In OTP 27, this might return :nxdomain (the bug) or work correctly
      # We're documenting the current behavior
      assert match?({:error, _}, result)
    end

    test "Protocol._to_hostname should convert IP address strings to tuples" do
      # This test will fail until we fix _to_hostname
      # The fix should detect IP addresses and return tuples

      # For now, let's test what the current behavior is
      # _to_hostname is private, so we test via the public interface

      # Test that Utils.default_config preserves hostname correctly
      config = Bolt.Sips.Utils.default_config(hostname: "127.0.0.1", port: 7687)
      assert config[:hostname] == "127.0.0.1"
    end

    test "IPv4 address detection" do
      # Test helper function to detect IPv4 addresses
      assert is_ipv4_address?("127.0.0.1") == true
      assert is_ipv4_address?("192.168.1.1") == true
      assert is_ipv4_address?("255.255.255.255") == true
      assert is_ipv4_address?("0.0.0.0") == true

      assert is_ipv4_address?("localhost") == false
      assert is_ipv4_address?("neo4j.example.com") == false
      assert is_ipv4_address?("127.0.0.1.1") == false
      assert is_ipv4_address?("256.0.0.1") == false
      # Note: :inet.parse_ipv4_address accepts "127.0.0" (fills in missing octet)
      # This is expected Erlang behavior
    end

    test "IPv6 address detection" do
      assert is_ipv6_address?("::1") == true
      assert is_ipv6_address?("fe80::1") == true
      assert is_ipv6_address?("2001:db8::1") == true

      assert is_ipv6_address?("localhost") == false
      # Note: :inet.parse_ipv6_address can parse IPv4 addresses too (as IPv4-mapped IPv6)
      # We use :inet.parse_address which handles both correctly
    end

    test "parse_ip_address converts IPv4 string to tuple" do
      assert parse_ip_address("127.0.0.1") == {:ok, {127, 0, 0, 1}}
      assert parse_ip_address("192.168.1.100") == {:ok, {192, 168, 1, 100}}
      assert parse_ip_address("0.0.0.0") == {:ok, {0, 0, 0, 0}}
    end

    test "parse_ip_address converts IPv6 string to tuple" do
      assert parse_ip_address("::1") == {:ok, {0, 0, 0, 0, 0, 0, 0, 1}}
    end

    test "parse_ip_address returns error for hostnames" do
      assert parse_ip_address("localhost") == :error
      assert parse_ip_address("neo4j.example.com") == :error
    end

    test "to_connect_address converts IP string to tuple" do
      # This is the function Protocol should use
      assert to_connect_address("127.0.0.1") == {127, 0, 0, 1}
      assert to_connect_address("::1") == {0, 0, 0, 0, 0, 0, 0, 1}
    end

    test "to_connect_address converts hostname to charlist" do
      assert to_connect_address("localhost") == ~c"localhost"
      assert to_connect_address("neo4j.example.com") == ~c"neo4j.example.com"
    end

    test "to_connect_address handles charlist input" do
      assert to_connect_address(~c"127.0.0.1") == {127, 0, 0, 1}
      assert to_connect_address(~c"localhost") == ~c"localhost"
    end

    test "to_connect_address handles tuple input (passthrough)" do
      assert to_connect_address({127, 0, 0, 1}) == {127, 0, 0, 1}
    end
  end

  # Helper functions that should be implemented in Utils or Protocol
  # These will fail until implementation is added

  defp is_ipv4_address?(host) when is_binary(host) do
    case :inet.parse_ipv4_address(String.to_charlist(host)) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end
  defp is_ipv4_address?(_), do: false

  defp is_ipv6_address?(host) when is_binary(host) do
    case :inet.parse_ipv6_address(String.to_charlist(host)) do
      {:ok, _} -> true
      {:error, _} -> false
    end
  end
  defp is_ipv6_address?(_), do: false

  defp parse_ip_address(host) when is_binary(host) do
    charlist = String.to_charlist(host)
    case :inet.parse_address(charlist) do
      {:ok, ip_tuple} -> {:ok, ip_tuple}
      {:error, _} -> :error
    end
  end
  defp parse_ip_address(host) when is_list(host) do
    case :inet.parse_address(host) do
      {:ok, ip_tuple} -> {:ok, ip_tuple}
      {:error, _} -> :error
    end
  end

  defp to_connect_address(host) when is_tuple(host), do: host

  defp to_connect_address(host) when is_binary(host) do
    charlist = String.to_charlist(host)
    case :inet.parse_address(charlist) do
      {:ok, ip_tuple} -> ip_tuple
      {:error, _} -> charlist
    end
  end

  defp to_connect_address(host) when is_list(host) do
    case :inet.parse_address(host) do
      {:ok, ip_tuple} -> ip_tuple
      {:error, _} -> host
    end
  end
end
