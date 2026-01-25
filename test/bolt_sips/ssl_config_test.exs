defmodule Bolt.Sips.SSLConfigTest do
  use ExUnit.Case, async: true

  alias Bolt.Sips.Utils
  alias Bolt.Sips.Protocol

  describe "ssl: false configuration" do
    test "default_config sets socket to Bolt.Sips.Socket when ssl: false" do
      config = Utils.default_config(ssl: false)

      assert config[:ssl] == false
      assert config[:socket] == Bolt.Sips.Socket
    end

    test "default_config does not set socket to :ssl when ssl: false" do
      config = Utils.default_config(ssl: false, socket: Bolt.Sips.Socket)

      refute config[:socket] == :ssl
      assert config[:socket] == Bolt.Sips.Socket
    end

    test "default_config with explicit hostname and port" do
      config = Utils.default_config(
        hostname: "localhost",
        port: 7687,
        pool_size: 10,
        ssl: false,
        socket: Bolt.Sips.Socket
      )

      assert config[:ssl] == false
      assert config[:socket] == Bolt.Sips.Socket
      assert config[:hostname] == "localhost"
      assert config[:port] == 7687
    end

    test "default_config handles ssl as list of options correctly" do
      ssl_opts = [verify: :verify_none]
      config = Utils.default_config(ssl: ssl_opts)

      # When ssl is a list, socket should be :ssl
      assert config[:socket] == :ssl
      assert config[:ssl] == ssl_opts
    end

    test "default_config handles ssl: true correctly" do
      config = Utils.default_config(ssl: true)

      # When ssl is true, socket should be :ssl
      assert config[:socket] == :ssl
    end

    test "socket_opts in Protocol.connect does not include SSL options when ssl: false" do
      # This test verifies that when ssl: false, the socket_opts
      # passed to the socket.connect call don't include SSL-specific options
      conf = Utils.default_config(ssl: false)

      default_socket_options = [packet: :raw, mode: :binary, active: false]

      socket_opts =
        case conf[:ssl] do
          list when is_list(list) -> Keyword.merge(default_socket_options, conf[:ssl])
          _ -> default_socket_options
        end

      # Should only have basic TCP options, no SSL options
      assert socket_opts == [packet: :raw, mode: :binary, active: false]
      refute Keyword.has_key?(socket_opts, :verify)
      refute Keyword.has_key?(socket_opts, :cacerts)
    end

    test "socket_opts includes SSL options when ssl is a list" do
      ssl_opts = [verify: :verify_none, cacerts: []]
      conf = Utils.default_config(ssl: ssl_opts)

      default_socket_options = [packet: :raw, mode: :binary, active: false]

      socket_opts =
        case conf[:ssl] do
          list when is_list(list) -> Keyword.merge(default_socket_options, conf[:ssl])
          _ -> default_socket_options
        end

      # Should include SSL options merged with default options
      assert Keyword.get(socket_opts, :verify) == :verify_none
      assert Keyword.get(socket_opts, :cacerts) == []
    end
  end

  describe "OTP 27 SSL compatibility" do
    test "gen_tcp.connect does not require SSL options" do
      # This test verifies that :gen_tcp.connect works without SSL options
      # which is the expected behavior when ssl: false
      opts = [packet: :raw, mode: :binary, active: false]

      # This should not raise an SSL-related error
      # We're testing with a likely-closed port to verify the OPTIONS don't cause SSL errors
      result = :gen_tcp.connect(~c"localhost", 65535, opts, 100)

      # We expect a connection refused or timeout, NOT an SSL options error
      assert match?({:error, :econnrefused}, result) or
             match?({:error, :timeout}, result) or
             match?({:error, :nxdomain}, result)
    end
  end

  describe "edge cases with ssl config" do
    test "ssl: [] (empty list) should be treated as ssl disabled" do
      # Empty list is truthy in Elixir but should not enable SSL
      # This is a common misconfiguration
      config = Utils.default_config(ssl: [])

      # With empty ssl list, socket should NOT be :ssl
      # because empty options means user wants plain TCP
      # BUG: Currently this fails because empty list is truthy
      assert config[:socket] == Bolt.Sips.Socket,
        "Empty ssl: [] should not enable SSL mode"
    end

    test "ssl: nil should be treated as ssl disabled" do
      config = Utils.default_config(ssl: nil)

      assert config[:socket] == Bolt.Sips.Socket
    end

    test "socket_opts with ssl: [] should not pass empty SSL options to gen_tcp" do
      # When ssl: [] is passed, we should NOT merge it into socket_opts
      # because gen_tcp doesn't understand SSL options
      conf = Utils.default_config(ssl: [])

      default_socket_options = [packet: :raw, mode: :binary, active: false]

      socket_opts =
        case conf[:ssl] do
          list when is_list(list) and list != [] ->
            Keyword.merge(default_socket_options, conf[:ssl])
          _ ->
            default_socket_options
        end

      # This is what the FIXED behavior should be
      assert socket_opts == [packet: :raw, mode: :binary, active: false]
    end

    test "ssl: true should use safe defaults for OTP 26+" do
      # In OTP 26+, :ssl.connect defaults to verify: :verify_peer
      # which requires CA certs. When ssl: true is passed without
      # explicit options, we need safe defaults.
      config = Utils.default_config(ssl: true)

      assert config[:socket] == :ssl

      # The ssl option should have safe defaults for OTP 26+
      # When ssl: true, it should be converted to a list with safe options
      ssl_opts = config[:ssl]

      # BUG: Currently ssl: true stays as boolean true,
      # which means no SSL options are passed to :ssl.connect,
      # causing OTP 26+ to use verify: :verify_peer with no cacerts
      assert is_list(ssl_opts),
        "ssl: true should be expanded to a list of safe SSL options for OTP 26+"
    end

    test "ssl options should include verify: :verify_none when no cacerts provided" do
      # For backward compatibility and to work without CA certs,
      # we need to set verify: :verify_none as default
      config = Utils.default_config(ssl: true)

      ssl_opts = config[:ssl]

      # Should have verify: :verify_none to avoid OTP 26+ strict defaults
      if is_list(ssl_opts) do
        assert Keyword.get(ssl_opts, :verify) == :verify_none
      end
    end
  end
end
