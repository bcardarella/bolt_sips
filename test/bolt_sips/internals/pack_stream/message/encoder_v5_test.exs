defmodule Bolt.Sips.Internals.PackStream.Message.EncoderV5Test do
  use ExUnit.Case, async: true

  alias Bolt.Sips.Internals.PackStream.Message.Encoder
  alias Bolt.Sips.Metadata

  @bolt_version 5

  describe "Encode HELLO for v5" do
    test "without params" do
      # v5 HELLO includes bolt_agent
      assert <<0x0, _, 0xB1, 0x1, _::binary>> =
               :erlang.iolist_to_binary(Encoder.encode({:hello, []}, @bolt_version))
    end

    test "with auth params" do
      assert <<0x0, _, 0xB1, 0x1, _::binary>> =
               :erlang.iolist_to_binary(Encoder.encode({:hello, [{"neo4j", "test"}]}, @bolt_version))
    end

    test "with routing context" do
      extra = %{routing: %{address: "localhost:7687"}}

      assert <<0x0, _, 0xB1, 0x1, _::binary>> =
               :erlang.iolist_to_binary(Encoder.encode({:hello, [{}, extra]}, @bolt_version))
    end

    test "with notification config (v5.2+)" do
      extra = %{notifications_minimum_severity: "WARNING"}

      assert <<0x0, _, 0xB1, 0x1, _::binary>> =
               :erlang.iolist_to_binary(Encoder.encode({:hello, [{}, extra]}, @bolt_version))
    end

    test "with disabled classifications (v5.6+)" do
      extra = %{
        notifications_minimum_severity: "WARNING",
        notifications_disabled_classifications: ["HINT", "PERFORMANCE"]
      }

      assert <<0x0, _, 0xB1, 0x1, _::binary>> =
               :erlang.iolist_to_binary(Encoder.encode({:hello, [{}, extra]}, @bolt_version))
    end
  end

  describe "Encode LOGON for v5.1+" do
    test "with auth credentials" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:logon, [{"neo4j", "test"}]}, @bolt_version))
      # Signature 0x6A for LOGON
      assert <<0x0, _, 0xB1, 0x6A, _::binary>> = encoded
    end

    test "without auth credentials" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:logon, [{}]}, @bolt_version))
      # Signature 0x6A for LOGON
      assert <<0x0, _, 0xB1, 0x6A, _::binary>> = encoded
    end
  end

  describe "Encode LOGOFF for v5.1+" do
    test "encodes correctly" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:logoff, []}, @bolt_version))
      # Signature 0x6B for LOGOFF
      assert <<0x0, 0x2, 0xB0, 0x6B, 0x0, 0x0>> = encoded
    end
  end

  describe "Encode TELEMETRY for v5.4+" do
    test "with api parameter" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:telemetry, [1]}, @bolt_version))
      # Signature 0x54 for TELEMETRY
      assert <<0x0, _, 0xB1, 0x54, _::binary>> = encoded
    end
  end

  describe "Encode PULL for v5" do
    test "without params (defaults to n=-1)" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, []}, @bolt_version))
      # Signature 0x3F for PULL
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "with n and qid parameters" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: 50, qid: 0}]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end
  end

  describe "Encode DISCARD for v5" do
    test "without params (defaults to n=-1)" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:discard, []}, @bolt_version))
      # Signature 0x2F for DISCARD
      assert <<0x0, _, 0xB1, 0x2F, _::binary>> = encoded
    end
  end

  describe "Encode RUN for v5 with all v5 features" do
    test "with notification severity in metadata" do
      {:ok, metadata} = Metadata.new(%{
        db: "neo4j",
        notifications_minimum_severity: "WARNING"
      })

      encoded =
        :erlang.iolist_to_binary(
          Encoder.encode({:run, ["RETURN 1 AS num", %{}, metadata]}, @bolt_version)
        )

      assert <<0x0, _, 0xB3, 0x10, _::binary>> = encoded
    end

    test "with notification classifications in metadata" do
      {:ok, metadata} = Metadata.new(%{
        db: "neo4j",
        notifications_minimum_severity: "INFORMATION",
        notifications_disabled_classifications: ["HINT"]
      })

      encoded =
        :erlang.iolist_to_binary(
          Encoder.encode({:run, ["RETURN 1 AS num", %{}, metadata]}, @bolt_version)
        )

      assert <<0x0, _, 0xB3, 0x10, _::binary>> = encoded
    end

    test "with impersonated user (v4.4+)" do
      {:ok, metadata} = Metadata.new(%{
        db: "neo4j",
        imp_user: "testuser"
      })

      encoded =
        :erlang.iolist_to_binary(
          Encoder.encode({:run, ["RETURN 1 AS num", %{}, metadata]}, @bolt_version)
        )

      assert <<0x0, _, 0xB3, 0x10, _::binary>> = encoded
    end
  end

  describe "Encode BEGIN for v5 with all v5 features" do
    test "with notification severity" do
      {:ok, metadata} = Metadata.new(%{
        db: "neo4j",
        notifications_minimum_severity: "OFF"
      })

      encoded = :erlang.iolist_to_binary(Encoder.encode({:begin, [metadata]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x11, _::binary>> = encoded
    end

    test "with impersonated user" do
      {:ok, metadata} = Metadata.new(%{
        db: "neo4j",
        imp_user: "admin"
      })

      encoded = :erlang.iolist_to_binary(Encoder.encode({:begin, [metadata]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x11, _::binary>> = encoded
    end
  end

  describe "Encode ROUTE for v5" do
    test "with routing context and bookmarks" do
      routing_context = %{address: "localhost:7687"}
      bookmarks = ["neo4j:bookmark:v1:tx123"]

      encoded =
        :erlang.iolist_to_binary(
          Encoder.encode({:route, [routing_context, bookmarks, nil]}, @bolt_version)
        )

      # Signature 0x66 for ROUTE
      assert <<0x0, _, 0xB3, 0x66, _::binary>> = encoded
    end

    test "with database parameter" do
      routing_context = %{}
      bookmarks = []
      database = "neo4j"

      encoded =
        :erlang.iolist_to_binary(
          Encoder.encode({:route, [routing_context, bookmarks, database]}, @bolt_version)
        )

      assert <<0x0, _, 0xB3, 0x66, _::binary>> = encoded
    end
  end

  describe "Encode COMMIT for v5" do
    test "encodes correctly" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:commit, []}, @bolt_version))
      # Signature 0x12 for COMMIT
      assert <<0x0, 0x2, 0xB0, 0x12, 0x0, 0x0>> = encoded
    end
  end

  describe "Encode ROLLBACK for v5" do
    test "encodes correctly" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:rollback, []}, @bolt_version))
      # Signature 0x13 for ROLLBACK
      assert <<0x0, 0x2, 0xB0, 0x13, 0x0, 0x0>> = encoded
    end
  end

  describe "Encode RESET for v5" do
    test "encodes correctly" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:reset, []}, @bolt_version))
      # Signature 0x0F for RESET
      assert <<0x0, 0x2, 0xB0, 0x0F, 0x0, 0x0>> = encoded
    end
  end

  describe "Encode GOODBYE for v5" do
    test "encodes correctly" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:goodbye, []}, @bolt_version))
      # Signature 0x02 for GOODBYE
      assert <<0x0, 0x2, 0xB0, 0x02, 0x0, 0x0>> = encoded
    end
  end

  describe "Tuple version format for v5.x" do
    test "handles {5, 6} version format" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: -1}]}, {5, 6}))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "handles {5, 4} version format" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: -1}]}, {5, 4}))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "handles {5, 2} version format" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: -1}]}, {5, 2}))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "handles {5, 1} version format" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: -1}]}, {5, 1}))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "handles {5, 0} version format" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: -1}]}, {5, 0}))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end
  end

  describe "valid_signatures/1 for v5" do
    test "returns v5 signatures" do
      signatures = Encoder.valid_signatures(5)

      # Check some v5-specific signatures are included
      assert 0x6A in signatures  # LOGON
      assert 0x6B in signatures  # LOGOFF
      assert 0x54 in signatures  # TELEMETRY
    end

    test "returns v5 signatures for tuple format" do
      signatures = Encoder.valid_signatures({5, 4})

      assert 0x6A in signatures  # LOGON
      assert 0x6B in signatures  # LOGOFF
      assert 0x54 in signatures  # TELEMETRY
    end
  end
end
