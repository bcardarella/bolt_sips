defmodule Bolt.Sips.IdleConfigTest do
  use ExUnit.Case, async: true

  alias Bolt.Sips.Utils

  describe "default_config/1 includes idle connection options" do
    test "includes idle_interval in defaults" do
      config = Utils.default_config([])

      assert Keyword.has_key?(config, :idle_interval),
        "default config should include :idle_interval for DBConnection health checks"

      assert is_integer(config[:idle_interval])
      assert config[:idle_interval] > 0
    end

    test "idle_interval defaults to 1000ms" do
      config = Utils.default_config([])

      assert config[:idle_interval] == 1_000
    end

    test "idle_interval can be overridden" do
      config = Utils.default_config(idle_interval: 5_000)

      assert config[:idle_interval] == 5_000
    end

    test "includes ping_timeout in defaults" do
      config = Utils.default_config([])

      assert Keyword.has_key?(config, :ping_timeout),
        "default config should include :ping_timeout for ping/1 RESET timeout"

      assert is_integer(config[:ping_timeout])
      assert config[:ping_timeout] > 0
    end

    test "ping_timeout defaults to 5000ms" do
      config = Utils.default_config([])

      assert config[:ping_timeout] == 5_000
    end

    test "ping_timeout can be overridden" do
      config = Utils.default_config(ping_timeout: 2_000)

      assert config[:ping_timeout] == 2_000
    end

    test "ping_timeout is shorter than default recv_timeout" do
      config = Utils.default_config([])

      # ping_timeout should be much shorter than the 30s recv_timeout
      # so that ping doesn't block the pool for too long
      assert config[:ping_timeout] < 30_000
    end
  end
end
