defmodule Bolt.Sips.Internals.PackStream.Message.EncoderV4Test do
  use ExUnit.Case, async: true

  alias Bolt.Sips.Internals.PackStream.Message.Encoder
  alias Bolt.Sips.Metadata

  @bolt_version 4

  describe "Encode HELLO for v4" do
    test "without params" do
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
  end

  describe "Encode PULL for v4 (replaces PULL_ALL)" do
    test "without params (defaults to n=-1)" do
      # PULL with empty params should default to n=-1 (all records)
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, []}, @bolt_version))
      # Signature 0x3F for PULL
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "with n parameter" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: 100}]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "with n and qid parameters" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: 50, qid: 0}]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "with n=-1 (all records)" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: -1}]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "invalid n parameter returns error" do
      assert {:error, :invalid_n_parameter} = Encoder.encode({:pull, [%{n: 0}]}, @bolt_version)
    end

    test "invalid qid parameter returns error" do
      assert {:error, :invalid_qid_parameter} = Encoder.encode({:pull, [%{n: -1, qid: -2}]}, @bolt_version)
    end
  end

  describe "Encode PULL_ALL backward compatibility for v4" do
    test "encodes as PULL with n=-1" do
      # PULL_ALL should work in v4+ by delegating to PULL
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull_all, []}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end
  end

  describe "Encode DISCARD for v4 (replaces DISCARD_ALL)" do
    test "without params (defaults to n=-1)" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:discard, []}, @bolt_version))
      # Signature 0x2F for DISCARD
      assert <<0x0, _, 0xB1, 0x2F, _::binary>> = encoded
    end

    test "with n parameter" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:discard, [%{n: 100}]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x2F, _::binary>> = encoded
    end

    test "with n and qid parameters" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:discard, [%{n: 50, qid: 0}]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x2F, _::binary>> = encoded
    end

    test "invalid n parameter returns error" do
      assert {:error, :invalid_n_parameter} = Encoder.encode({:discard, [%{n: 0}]}, @bolt_version)
    end
  end

  describe "Encode DISCARD_ALL backward compatibility for v4" do
    test "encodes as DISCARD with n=-1" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:discard_all, []}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x2F, _::binary>> = encoded
    end
  end

  describe "Encode RUN for v4 with database support" do
    test "without params nor metadata" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:run, ["RETURN 1 AS num"]}, @bolt_version))
      # Signature 0x10 for RUN, B3 means 3 parameters (statement, params, extra)
      assert <<0x0, _, 0xB3, 0x10, _::binary>> = encoded
    end

    test "with params without metadata" do
      encoded =
        :erlang.iolist_to_binary(
          Encoder.encode({:run, ["RETURN $num AS num", %{num: 5}]}, @bolt_version)
        )

      assert <<0x0, _, 0xB3, 0x10, _::binary>> = encoded
    end

    test "with params and metadata including database" do
      {:ok, metadata} = Metadata.new(%{tx_timeout: 5000, db: "neo4j"})

      encoded =
        :erlang.iolist_to_binary(
          Encoder.encode({:run, ["RETURN $num AS num", %{num: 5}, metadata]}, @bolt_version)
        )

      assert <<0x0, _, 0xB3, 0x10, _::binary>> = encoded
    end

    test "with mode parameter" do
      {:ok, metadata} = Metadata.new(%{db: "neo4j", mode: "r"})

      encoded =
        :erlang.iolist_to_binary(
          Encoder.encode({:run, ["RETURN 1 AS num", %{}, metadata]}, @bolt_version)
        )

      assert <<0x0, _, 0xB3, 0x10, _::binary>> = encoded
    end
  end

  describe "Encode BEGIN for v4 with database support" do
    test "without params" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:begin, []}, @bolt_version))
      # Signature 0x11 for BEGIN
      assert <<0x0, _, 0xB1, 0x11, _::binary>> = encoded
    end

    test "with empty params" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:begin, [%{}]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x11, _::binary>> = encoded
    end

    test "with database parameter" do
      {:ok, metadata} = Metadata.new(%{db: "neo4j"})
      encoded = :erlang.iolist_to_binary(Encoder.encode({:begin, [metadata]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x11, _::binary>> = encoded
    end

    test "with mode and timeout parameters" do
      {:ok, metadata} = Metadata.new(%{db: "neo4j", mode: "w", tx_timeout: 10_000})
      encoded = :erlang.iolist_to_binary(Encoder.encode({:begin, [metadata]}, @bolt_version))
      assert <<0x0, _, 0xB1, 0x11, _::binary>> = encoded
    end
  end

  describe "Encode ROUTE for v4.3+" do
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

  describe "Encode COMMIT for v4" do
    test "encodes correctly" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:commit, []}, @bolt_version))
      # Signature 0x12 for COMMIT
      assert <<0x0, 0x2, 0xB0, 0x12, 0x0, 0x0>> = encoded
    end
  end

  describe "Encode ROLLBACK for v4" do
    test "encodes correctly" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:rollback, []}, @bolt_version))
      # Signature 0x13 for ROLLBACK
      assert <<0x0, 0x2, 0xB0, 0x13, 0x0, 0x0>> = encoded
    end
  end

  describe "Encode RESET for v4" do
    test "encodes correctly" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:reset, []}, @bolt_version))
      # Signature 0x0F for RESET
      assert <<0x0, 0x2, 0xB0, 0x0F, 0x0, 0x0>> = encoded
    end
  end

  describe "Encode GOODBYE for v4" do
    test "encodes correctly" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:goodbye, []}, @bolt_version))
      # Signature 0x02 for GOODBYE
      assert <<0x0, 0x2, 0xB0, 0x02, 0x0, 0x0>> = encoded
    end
  end

  describe "Tuple version format for v4.x" do
    test "handles {4, 4} version format" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: -1}]}, {4, 4}))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "handles {4, 3} version format" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: -1}]}, {4, 3}))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end

    test "handles {4, 0} version format" do
      encoded = :erlang.iolist_to_binary(Encoder.encode({:pull, [%{n: -1}]}, {4, 0}))
      assert <<0x0, _, 0xB1, 0x3F, _::binary>> = encoded
    end
  end
end
