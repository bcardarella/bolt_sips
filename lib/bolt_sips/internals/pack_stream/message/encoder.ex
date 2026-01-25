defmodule Bolt.Sips.Internals.PackStream.Message.Encoder do
  @moduledoc false
  _module_doc = """
  Manages the message encoding.

  A mesage is a tuple formated as:
  `{message_type, data}`
  with:
  - message_type: atom amongst the valid message type (:init, :discard_all, :pull_all,
  :ack_failure, :reset, :run)
  - data: a list of data to be used by the message

  Messages are passed in one or more chunk. The structure of a chunk is as follow: `chunk_size` `data`
  with `chunk_size` beign a 16-bit integer.
  A message always ends with the end marker `0x00 0x00`.
  Thus the possible typologies of messages are:
  - One-chunk message:
  `chunk_size` `message_data` `end_marker`
  - multiple-chunk message:
  `chunk_1_size` `message_data` `chunk_n_size` `message_data`...`end_marker`
  More documentation on message transfer encoding:
  [https://boltprotocol.org/v1/#message_transfer_encoding](https://boltprotocol.org/v1/#message_transfer_encoding)

  All messages are serialized structures. See `Bolt.Sips.Internals.PackStream.EncoderV1` for
  more information about structure encoding).

  An extensive documentation on messages can be found here:
  [https://boltprotocol.org/v1/#messages](https://boltprotocol.org/v1/#messages)
  """

  alias Bolt.Sips.Metadata

  @max_chunk_size 65_535
  @end_marker <<0x00, 0x00>>

  # Message signatures
  @ack_failure_signature 0x0E
  @begin_signature 0x11
  @commit_signature 0x12
  @discard_all_signature 0x2F  # Also used for DISCARD in v4+
  @discard_signature 0x2F      # Same as DISCARD_ALL
  @goodbye_signature 0x02
  @hello_signature 0x01
  @init_signature 0x01
  @pull_all_signature 0x3F     # Also used for PULL in v4+
  @pull_signature 0x3F         # Same as PULL_ALL
  @reset_signature 0x0F
  @rollback_signature 0x13
  @run_signature 0x10
  @route_signature 0x66        # v4.3+
  @logon_signature 0x6A        # v5.1+
  @logoff_signature 0x6B       # v5.1+
  @telemetry_signature 0x54    # v5.4+

  # OUT Signature lists for validation
  @valid_v1_signatures [
    @ack_failure_signature,
    @discard_all_signature,
    @init_signature,
    @pull_all_signature,
    @reset_signature,
    @run_signature
  ]

  @valid_v3_signatures [
    @ack_failure_signature,
    @begin_signature,
    @commit_signature,
    @discard_all_signature,
    @goodbye_signature,
    @hello_signature,
    @pull_all_signature,
    @reset_signature,
    @rollback_signature,
    @run_signature
  ]

  @valid_v4_signatures [
    @begin_signature,
    @commit_signature,
    @discard_signature,
    @goodbye_signature,
    @hello_signature,
    @pull_signature,
    @reset_signature,
    @rollback_signature,
    @route_signature,
    @run_signature
  ]

  @valid_v5_signatures [
    @begin_signature,
    @commit_signature,
    @discard_signature,
    @goodbye_signature,
    @hello_signature,
    @logoff_signature,
    @logon_signature,
    @pull_signature,
    @reset_signature,
    @rollback_signature,
    @route_signature,
    @run_signature,
    @telemetry_signature
  ]

  @valid_v1_message_types [
    :ack_failure,
    :discard_all,
    :init,
    :pull_all,
    :reset,
    :run
  ]

  @valid_v3_message_types [
    :ack_failure,
    :begin,
    :commit,
    :discard_all,
    :goodbye,
    :hello,
    :rollback,
    :pull_all,
    :reset,
    :run
  ]

  @valid_v4_message_types [
    :begin,
    :commit,
    :discard,
    :goodbye,
    :hello,
    :pull,
    :reset,
    :rollback,
    :route,
    :run
  ]

  @valid_v5_message_types [
    :begin,
    :commit,
    :discard,
    :goodbye,
    :hello,
    :logoff,
    :logon,
    :pull,
    :reset,
    :rollback,
    :route,
    :run,
    :telemetry
  ]

  # For backward compatibility
  @valid_signatures @valid_v3_signatures
  @valid_message_types @valid_v3_message_types

  @last_bolt_version 5

  @spec signature(Bolt.Sips.Internals.PackStream.Message.out_signature()) :: integer()
  defp signature(:ack_failure), do: @ack_failure_signature
  defp signature(:discard_all), do: @discard_all_signature
  defp signature(:discard), do: @discard_signature
  defp signature(:pull_all), do: @pull_all_signature
  defp signature(:pull), do: @pull_signature
  defp signature(:reset), do: @reset_signature
  defp signature(:begin), do: @begin_signature
  defp signature(:commit), do: @commit_signature
  defp signature(:goodbye), do: @goodbye_signature
  defp signature(:hello), do: @hello_signature
  defp signature(:rollback), do: @rollback_signature
  defp signature(:run), do: @run_signature
  defp signature(:init), do: @init_signature
  defp signature(:route), do: @route_signature
  defp signature(:logon), do: @logon_signature
  defp signature(:logoff), do: @logoff_signature
  defp signature(:telemetry), do: @telemetry_signature

  @doc """
  Return client name (based on bolt_sips version)
  """
  def client_name() do
    "BoltSips/" <> to_string(Application.spec(:bolt_sips, :vsn))
  end

  @doc """
  Return the valid message signatures depending on the Bolt version.
  Accepts both integer versions (1, 2, 3) and tuple versions ({4, 4}, {5, 4}).
  """
  @spec valid_signatures(integer() | {integer(), integer()}) :: [integer()]
  def valid_signatures(bolt_version) when is_tuple(bolt_version) do
    valid_signatures(elem(bolt_version, 0))
  end

  def valid_signatures(bolt_version) when bolt_version <= 2 do
    @valid_v1_signatures
  end

  def valid_signatures(3) do
    @valid_v3_signatures
  end

  def valid_signatures(4) do
    @valid_v4_signatures
  end

  def valid_signatures(bolt_version) when bolt_version >= 5 do
    @valid_v5_signatures
  end

  # Encode messages for bolt version 3

  # Encode HELLO message without auth token
  @spec encode({Bolt.Sips.Internals.PackStream.Message.out_signature(), list()}, integer() | {integer(), integer()}) ::
          Bolt.Sips.Internals.PackStream.Message.encoded()
          | {:error, :not_implemented}
          | {:error, :invalid_message}

  # Handle tuple versions by extracting major version (must be first clause)
  def encode(data, bolt_version) when is_tuple(bolt_version) do
    encode(data, extract_major_version(bolt_version))
  end

  def encode({:hello, []}, 3) do
    encode({:hello, [{}]}, 3)
  end

  # Encode INIT message with a valid auth token.
  # The auth token is tuple formated as: {user, password}
  def encode({:hello, [auth]}, 3) do
    do_encode(:hello, [auth_params(auth)], 3)
  end

  # Encode BEGIN message without metadata.

  # BEGIN is used to open a transaction.
  def encode({:begin, []}, 3) do
    encode({:begin, [%{}]}, 3)
  end

  # Encode BEGIN message with metadata
  def encode({:begin, [%Metadata{} = metadata]}, 3) do
    do_encode(:begin, [Metadata.to_map(metadata)], 3)
  end

  def encode({:begin, [%{} = map]}, 3) when map_size(map) == 0 do
    {:ok, metadata} = Metadata.new(%{})
    encode({:begin, [metadata]}, 3)
  end

  # Catch-all for invalid BEGIN data in v3 only (v4+ has its own clauses later)
  def encode({:begin, _}, bolt_version) when bolt_version <= 3 do
    {:error, :invalid_data}
  end

  # Encode RUN without params nor metadata
  def encode({:run, [statement]}, 3) do
    do_encode(:run, [statement, %{}, %{}], 3)
  end

  # Encode RUN message with its data: statement and parameters
  def encode({:run, [statement]}, bolt_version) when bolt_version <= 2 do
    do_encode(:run, [statement, %{}], bolt_version)
  end

  # Encode RUN with params but without metadata
  def encode({:run, [statement, params]}, 3) do
    do_encode(:run, [statement, params, %{}], 3)
  end

  # Encode RUN with params and metadata
  def encode({:run, [statement, params, %Metadata{} = metadata]}, 3) do
    do_encode(:run, [statement, params, Metadata.to_map(metadata)], 3)
  end

  # INIT is no more a valid message in Bolt V3
  def encode({:init, _}, 3) do
    {:error, :invalid_message}
  end

  # Encode INIT message without auth token
  def encode({:init, []}, bolt_version) when bolt_version <= 2 do
    encode({:init, [{}]}, bolt_version)
  end

  # Encode INIT message with a valid auth token.
  # The auth token is tuple formated as: {user, password}
  def encode({:init, [auth]}, bolt_version) when bolt_version <= 2 do
    do_encode(:init, [client_name(), auth_params_v1(auth)], bolt_version)
  end

  # Encode messages that don't need any data formating
  def encode({message_type, data}, 3) when message_type in @valid_message_types do
    do_encode(message_type, data, 3)
  end

  # Encode messages that don't need any data formating
  def encode({message_type, data}, bolt_version)
      when bolt_version <= 2 and message_type in @valid_v1_message_types do
    do_encode(message_type, data, bolt_version)
  end

  @doc """
  Encode Bolt V3 messages

  Not that INIT is not valid in bolt V3, it is replaced by HELLO

  ## HELLO
  Usage: intialize the session.

  Signature: `0x01` (Same as INIT in previous bolt version)

  Struct: `auth_parms`

  with:

  | data | type |
  |-----|-----|
  |auth_token | map: {scheme: string, principal: string, credentials: string, user_agent: string}|

  Note: `user_agent` is equivalent to `client_name` in bolt previous version.

  Examples (excluded from doctest because client_name changes at each bolt_sips version)

      # without auth token
      diex> :erlang.iolist_to_binary(Encoder.encode({:hello, []}, 3))
      <<0x0, 0x1D, 0xB1, 0x1, 0xA1, 0x8A, 0x75, 0x73, 0x65, 0x72, 0x5F, 0x61, 0x67, 0x65, 0x6E,
      0x74, 0x8E, 0x42, 0x6F, 0x6C, 0x74, 0x53, 0x69, 0x70, 0x73, 0x2F, 0x31, 0x2E, 0x34, 0x2E,
      0x30, 0x0, 0x0>>

      # with auth token
      diex(20)> :erlang.iolist_to_binary(Encoder.encode({:hello, [{"neo4j", "test"}]}, 3))
      <<0x0, 0x4B, 0xB1, 0x1, 0xA4, 0x8B, 0x63, 0x72, 0x65, 0x64, 0x65, 0x6E, 0x74, 0x69, 0x61,
      0x6C, 0x73, 0x84, 0x74, 0x65, 0x73, 0x74, 0x89, 0x70, 0x72, 0x69, 0x6E, 0x63, 0x69, 0x70,
      0x61, 0x6C, 0x85, 0x6E, 0x65, 0x6F, 0x34, 0x6A, 0x86, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x65,
      0x85, 0x62, 0x61, 0x73, 0x69, ...>>

  ## GOODBYE
  Usage: close the connection with the server

  Signature: `0x02`

  Struct: no data

  Example

      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> :erlang.iolist_to_binary(Encoder.encode({:goodbye, []}, 3))
      <<0x0, 0x2, 0xB0, 0x2, 0x0, 0x0>>

  ## BEGIN
  Usage: Open a transaction

  Signature: `0x11`

  Struct: `metadata`

  with:

  | data | type |
  |------|------|
  | metadata | See Bolt.Sips.Metadata

  Example

      # without metadata
      # iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      # iex> :erlang.iolist_to_binary(Encoder.encode({:begin, []}, 3))
      # <<0x0, 0x3, 0xB1, 0x11, 0xA0, 0x0, 0x0>>

      # # with metadata
      # iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      # iex> alias Bolt.Sips.Metadata
      # iex> {:ok, metadata} = Metadata.new(%{tx_timeout: 5000})
      # {:ok,
      # %Bolt.Sips.Metadata{
      #   bookmarks: nil,
      #   metadata: nil,
      #   tx_timeout: 5000
      # }}
      # iex> :erlang.iolist_to_binary(Encoder.encode({:begin, [metadata]}, 3))
      # <<0x0, 0x11, 0xB1, 0x11, 0xA1, 0x8A, 0x74, 0x78, 0x5F, 0x74, 0x69, 0x6D, 0x65, 0x6F, 0x75,
      # 0x74, 0xC9, 0x13, 0x88, 0x0, 0x0>>

  ## COMMIT
  Usage: commit the currently open transaction

  Signature: `0x12`

  Struct: no data

  Example

      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> :erlang.iolist_to_binary(Encoder.encode({:commit, []}, 3))
      <<0x0, 0x2, 0xB0, 0x12, 0x0, 0x0>>

  ## ROLLBACK
  Usage: rollback the currently open transaction

  Signature: `0x13`

  Struct: no data

  Example

      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> :erlang.iolist_to_binary(Encoder.encode({:rollback, []}, 3))
      <<0x0, 0x2, 0xB0, 0x13, 0x0, 0x0>>

  ## RUN
  Usage: pass statement for execution to the server. Same as in bolt previous version.
  The only difference: `metadata` are passed as well since bolt v3.

  Signature: `0x10`

  Struct: `statement` `parameters` `metadata`

  with:

  | data | type |
  |-----|-----|
  | statement | string |
  | parameters | map |
  | metadata | See Bolt.Sips.Metadata

  Example

      # without params nor metadata
      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> :erlang.iolist_to_binary(Encoder.encode({:run, ["RETURN 'hello' AS str"]}, 3))
      <<0x0, 0x1B, 0xB3, 0x10, 0xD0, 0x15, 0x52, 0x45, 0x54, 0x55, 0x52, 0x4E, 0x20, 0x27, 0x68,
      0x65, 0x6C, 0x6C, 0x6F, 0x27, 0x20, 0x41, 0x53, 0x20, 0x73, 0x74, 0x72, 0xA0, 0xA0, 0x0,
      0x0>>

      # without params but with metadata
      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> alias Bolt.Sips.Metadata
      iex> {:ok, metadata} = Metadata.new(%{tx_timeout: 4500})
      {:ok,
      %Bolt.Sips.Metadata{
        bookmarks: nil,
        metadata: nil,
        tx_timeout: 4500
      }}
      iex> :erlang.iolist_to_binary(Encoder.encode({:run, ["RETURN 'hello' AS str", %{}, metadata]}, 3))
      <<0x0, 0x29, 0xB3, 0x10, 0xD0, 0x15, 0x52, 0x45, 0x54, 0x55, 0x52, 0x4E, 0x20, 0x27, 0x68,
      0x65, 0x6C, 0x6C, 0x6F, 0x27, 0x20, 0x41, 0x53, 0x20, 0x73, 0x74, 0x72, 0xA0, 0xA1, 0x8A,
      0x74, 0x78, 0x5F, 0x74, 0x69, 0x6D, 0x65, 0x6F, 0x75, 0x74, 0xC9, 0x11, 0x94, 0x0, 0x0>>

      # with params but without metadata
      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> :erlang.iolist_to_binary(Encoder.encode({:run, ["RETURN $str AS str", %{str: "hello"}]}, 3))
      <<0x0, 0x22, 0xB3, 0x10, 0xD0, 0x12, 0x52, 0x45, 0x54, 0x55, 0x52, 0x4E, 0x20,
      0x24, 0x73, 0x74, 0x72, 0x20, 0x41, 0x53, 0x20, 0x73, 0x74, 0x72, 0xA1, 0x83,
      0x73, 0x74, 0x72, 0x85, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0xA0, 0x0, 0x0>>

      # with params and metadata
      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> alias Bolt.Sips.Metadata
      iex> {:ok, metadata} = Metadata.new(%{tx_timeout: 4500})
      {:ok,
      %Bolt.Sips.Metadata{
        bookmarks: nil,
        metadata: nil,
        tx_timeout: 4500
      }}
      iex> :erlang.iolist_to_binary(Encoder.encode({:run, ["RETURN $str AS str", %{str: "hello"}, metadata]}, 3))
      <<0x0, 0x30, 0xB3, 0x10, 0xD0, 0x12, 0x52, 0x45, 0x54, 0x55, 0x52, 0x4E, 0x20,
      0x24, 0x73, 0x74, 0x72, 0x20, 0x41, 0x53, 0x20, 0x73, 0x74, 0x72, 0xA1, 0x83,
      0x73, 0x74, 0x72, 0x85, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0xA1, 0x8A, 0x74, 0x78,
      0x5F, 0x74, 0x69, 0x6D, 0x65, 0x6F, 0x75, 0x74, 0xC9, 0x11, 0x94, 0x0, 0x0>>

   #   Encode  messages v1

  # Supported messages

  ## INIT
  Usage: intialize the session.

  Signature: `0x01`

  Struct: `client_name` `auth_token`

  with:

  | data | type |
  |-----|-----|
  |client_name | string|
  |auth_token | map: {scheme: string, principal: string, credentials: string}|

  Examples (excluded from doctest because client_name changes at each bolt_sips version)

      # without auth token
      diex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      :erlang.iolist_to_binary(Encoder.encode({:init, []}, 1))
      <<0x0, 0x10, 0xB2, 0x1, 0x8C, 0x42, 0x6F, 0x6C, 0x74, 0x65, 0x78, 0x2F, 0x30, 0x2E, 0x34,
      0x2E, 0x30, 0xA0, 0x0, 0x0>>

      # with auth token
      # The auth token is tuple formated as: {user, password}
      diex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      diex> :erlang.iolist_to_binary(Encoder.encode({:init, [{"neo4j", "password"}]}))
      <<0x0, 0x42, 0xB2, 0x1, 0x8C, 0x42, 0x6F, 0x6C, 0x74, 0x65, 0x78, 0x2F, 0x30, 0x2E, 0x34,
      0x2E, 0x30, 0xA3, 0x8B, 0x63, 0x72, 0x65, 0x64, 0x65, 0x6E, 0x74, 0x69, 0x61, 0x6C, 0x73,
      0x88, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6F, 0x72, 0x64, 0x89, 0x70, 0x72, 0x69, 0x6E, 0x63,
      0x69, 0x70, 0x61, 0x6C, 0x85, ...>>


  ## RUN
  Usage: pass statement for execution to the server.

  Signature: `0x10`

  Struct: `statement` `parameters`

  with:

  | data | type |
  |-----|-----|
  | statement | string |
  | parameters | map |

  Examples
      # without parameters
      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> :erlang.iolist_to_binary(Encoder.encode({:run, ["RETURN 1 AS num"]}, 1))
      <<0x0, 0x13, 0xB2, 0x10, 0x8F, 0x52, 0x45, 0x54, 0x55, 0x52, 0x4E, 0x20, 0x31, 0x20, 0x41,
      0x53, 0x20, 0x6E, 0x75, 0x6D, 0xA0, 0x0, 0x0>>
      # with parameters
      iex> :erlang.iolist_to_binary(Encoder.encode({:run, ["RETURN $num AS num", %{num: 1}]}, 1))
      <<0x0, 0x1C, 0xB2, 0x10, 0xD0, 0x12, 0x52, 0x45, 0x54, 0x55, 0x52, 0x4E, 0x20,
      0x24, 0x6E, 0x75, 0x6D, 0x20, 0x41, 0x53, 0x20, 0x6E, 0x75, 0x6D, 0xA1, 0x83,
      0x6E, 0x75, 0x6D, 0x1, 0x0, 0x0>>

  ## ACK_FAILURE
  Usage: Acknowledge a failure the server has sent.

  Signature: `0x0E`

  Struct: no data

  Example

      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> :erlang.iolist_to_binary(Encoder.encode({:ack_failure, []}, 1))
      <<0x0, 0x2, 0xB0, 0xE, 0x0, 0x0>>

  ## DISCARD_ALL
  Uage: Discard all remaining items from the active result stream.

  Signature: `0x2F`

  Struct: no data

  Example

      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> :erlang.iolist_to_binary(Encoder.encode({:discard_all, []}, 1))
      <<0x0, 0x2, 0xB0, 0x2F, 0x0, 0x0>>

  ## PULL_ALL
  Usage: Retrieve all remaining items from the active result stream.

  Signature: `0x3F`

  Struct: no data

  Example

      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> :erlang.iolist_to_binary(Encoder.encode({:pull_all, []}, 1))
      <<0x0, 0x2, 0xB0, 0x3F, 0x0, 0x0>>

  ## RESET
  Usage: Return the current session to a "clean" state.

  Signature: `0x0F`

  Struct: no data

  Example

      iex> alias Bolt.Sips.Internals.PackStream.Message.Encoder
      iex> :erlang.iolist_to_binary(Encoder.encode({:reset, []}, 1))
      <<0x0, 0x2, 0xB0, 0xF, 0x0, 0x0>>


  Check if the encoder for the given bolt version is capable of encoding the given message
  If it is the case, the encoding function will be called
  If not, fallback to previous bolt version

  If encoding function is not present in any of the bolt  version, an error will be raised

  ## Bolt v4+ Changes

  ### PULL (replaces PULL_ALL)
  In v4+, PULL takes extra parameters: `{n: Integer, qid: Integer}`
  - n: number of records to fetch (-1 for all)
  - qid: query ID for explicit transactions (-1 for last statement)

  ### DISCARD (replaces DISCARD_ALL)
  Same as PULL, takes `{n: Integer, qid: Integer}`

  ### HELLO
  v4.1+ adds routing context to HELLO

  ### RUN/BEGIN
  v4+ adds database name support
  """

  # Encode messages for bolt version 5+

  # HELLO for v5+ (same as v4 but with bolt_agent and notification support)
  # v5.2+ supports: notifications_minimum_severity
  # v5.6+ supports: notifications_disabled_classifications
  def encode({:hello, []}, bolt_version) when bolt_version >= 5 do
    encode({:hello, [{}]}, bolt_version)
  end

  def encode({:hello, [auth]}, bolt_version) when bolt_version >= 5 do
    do_encode(:hello, [auth_params_v4(auth)], bolt_version)
  end

  # HELLO with notification configuration for v5+
  # Extra map can include: notifications_minimum_severity, notifications_disabled_classifications
  def encode({:hello, [auth, extra]}, bolt_version) when bolt_version >= 5 and is_map(extra) do
    params = auth_params_v4(auth) |> Map.merge(extra)
    do_encode(:hello, [params], bolt_version)
  end

  # LOGON for v5.1+ - separate authentication from HELLO
  def encode({:logon, [auth]}, bolt_version) when bolt_version >= 5 do
    auth_map = case auth do
      {} -> %{}
      {username, password} ->
        %{
          scheme: "basic",
          principal: username,
          credentials: password
        }
    end
    do_encode(:logon, [auth_map], bolt_version)
  end

  # LOGOFF for v5.1+
  def encode({:logoff, []}, bolt_version) when bolt_version >= 5 do
    do_encode(:logoff, [], bolt_version)
  end

  # TELEMETRY for v5.4+ - sends driver API usage info
  def encode({:telemetry, [api]}, bolt_version) when bolt_version >= 5 do
    do_encode(:telemetry, [%{api: api}], bolt_version)
  end

  # Encode messages for bolt version 4+

  # HELLO for v4+ (includes routing context support)
  def encode({:hello, []}, bolt_version) when bolt_version >= 4 do
    encode({:hello, [{}]}, bolt_version)
  end

  def encode({:hello, [auth]}, bolt_version) when bolt_version >= 4 do
    do_encode(:hello, [auth_params_v4(auth)], bolt_version)
  end

  def encode({:hello, [auth, extra]}, bolt_version) when bolt_version >= 4 do
    params = auth_params_v4(auth) |> Map.merge(extra)
    do_encode(:hello, [params], bolt_version)
  end

  # PULL for v4+ (replaces PULL_ALL, takes n and qid)
  # Default: n=-1 (all records), qid=-1 (last statement)
  # Parameters:
  #   n: Integer - number of records to fetch (-1 for all)
  #   qid: Integer - query ID for explicit transactions (-1 for last statement)
  def encode({:pull, []}, bolt_version) when bolt_version >= 4 do
    encode({:pull, [%{n: -1}]}, bolt_version)
  end

  def encode({:pull, [extra]}, bolt_version) when bolt_version >= 4 and is_map(extra) do
    case validate_pull_discard_extra(extra) do
      {:ok, validated_extra} ->
        do_encode(:pull, [validated_extra], bolt_version)

      {:error, _} = error ->
        error
    end
  end

  # PULL_ALL backward compatibility for v4+ - delegates to PULL with n=-1
  def encode({:pull_all, []}, bolt_version) when bolt_version >= 4 do
    encode({:pull, [%{n: -1}]}, bolt_version)
  end

  # DISCARD for v4+ (replaces DISCARD_ALL, takes n and qid)
  # Parameters:
  #   n: Integer - number of records to discard (-1 for all)
  #   qid: Integer - query ID for explicit transactions (-1 for last statement)
  def encode({:discard, []}, bolt_version) when bolt_version >= 4 do
    encode({:discard, [%{n: -1}]}, bolt_version)
  end

  def encode({:discard, [extra]}, bolt_version) when bolt_version >= 4 and is_map(extra) do
    case validate_pull_discard_extra(extra) do
      {:ok, validated_extra} ->
        do_encode(:discard, [validated_extra], bolt_version)

      {:error, _} = error ->
        error
    end
  end

  # DISCARD_ALL backward compatibility for v4+ - delegates to DISCARD with n=-1
  def encode({:discard_all, []}, bolt_version) when bolt_version >= 4 do
    encode({:discard, [%{n: -1}]}, bolt_version)
  end

  # RUN for v4+ (adds database name support in extra)
  def encode({:run, [statement]}, bolt_version) when bolt_version >= 4 do
    do_encode(:run, [statement, %{}, %{}], bolt_version)
  end

  def encode({:run, [statement, params]}, bolt_version) when bolt_version >= 4 do
    do_encode(:run, [statement, params, %{}], bolt_version)
  end

  def encode({:run, [statement, params, %Metadata{} = metadata]}, bolt_version)
      when bolt_version >= 4 do
    do_encode(:run, [statement, params, Metadata.to_map(metadata)], bolt_version)
  end

  def encode({:run, [statement, params, extra]}, bolt_version)
      when bolt_version >= 4 and is_map(extra) do
    do_encode(:run, [statement, params, extra], bolt_version)
  end

  # BEGIN for v4+ (adds database name support)
  def encode({:begin, []}, bolt_version) when bolt_version >= 4 do
    encode({:begin, [%{}]}, bolt_version)
  end

  def encode({:begin, [%Metadata{} = metadata]}, bolt_version) when bolt_version >= 4 do
    do_encode(:begin, [Metadata.to_map(metadata)], bolt_version)
  end

  def encode({:begin, [%{} = extra]}, bolt_version) when bolt_version >= 4 do
    do_encode(:begin, [extra], bolt_version)
  end

  # ROUTE for v4.3+ - queries routing table
  def encode({:route, [routing_context, bookmarks, database]}, bolt_version)
      when bolt_version >= 4 do
    do_encode(:route, [routing_context, bookmarks, database], bolt_version)
  end

  def encode({:route, [routing_context, bookmarks]}, bolt_version)
      when bolt_version >= 4 do
    do_encode(:route, [routing_context, bookmarks, nil], bolt_version)
  end

  # COMMIT for v4+ (explicit handler instead of catch-all)
  def encode({:commit, []}, bolt_version) when bolt_version >= 4 do
    do_encode(:commit, [], bolt_version)
  end

  # ROLLBACK for v4+ (explicit handler instead of catch-all)
  def encode({:rollback, []}, bolt_version) when bolt_version >= 4 do
    do_encode(:rollback, [], bolt_version)
  end

  # RESET for v4+ (explicit handler instead of catch-all)
  def encode({:reset, []}, bolt_version) when bolt_version >= 4 do
    do_encode(:reset, [], bolt_version)
  end

  # GOODBYE for v4+ (explicit handler instead of catch-all)
  def encode({:goodbye, []}, bolt_version) when bolt_version >= 4 do
    do_encode(:goodbye, [], bolt_version)
  end

  # Simple messages for v4+ (no parameters)
  def encode({message_type, data}, bolt_version)
      when bolt_version >= 4 and message_type in @valid_v4_message_types do
    do_encode(message_type, data, bolt_version)
  end

  # Fallback for versions beyond what we support
  def encode(data, bolt_version)
      when is_integer(bolt_version) and bolt_version > @last_bolt_version do
    encode(data, @last_bolt_version)
  end

  def encode(_data, _bolt_version) do
    {:error, :not_implemented}
  end

  defp do_encode(message_type, data, bolt_version) do
    signature = signature(message_type)
    encode_message(message_type, signature, data, bolt_version)
  end

  # Helper to extract major version from tuple format
  defp extract_major_version({major, _minor}), do: major

  # Format the auth params for v1 to v2
  @spec auth_params_v1({} | {String.t(), String.t()}) :: map()
  defp auth_params_v1({}), do: %{}

  defp auth_params_v1({username, password}) do
    %{
      scheme: "basic",
      principal: username,
      credentials: password
    }
  end

  # Format the auth params for v3
  @spec auth_params({} | {String.t(), String.t()}) :: map()
  defp auth_params({}), do: user_agent()

  defp auth_params({username, password}) do
    %{
      scheme: "basic",
      principal: username,
      credentials: password
    }
    |> Map.merge(user_agent())
  end

  # Format the auth params for v4+ (includes routing support)
  @spec auth_params_v4({} | {String.t(), String.t()}) :: map()
  defp auth_params_v4({}), do: user_agent_v4()

  defp auth_params_v4({username, password}) do
    %{
      scheme: "basic",
      principal: username,
      credentials: password
    }
    |> Map.merge(user_agent_v4())
  end

  defp user_agent() do
    %{user_agent: client_name()}
  end

  # For v5.3+, bolt_agent is required and must be a map with string keys and string values
  defp user_agent_v4() do
    %{
      user_agent: client_name(),
      bolt_agent: %{
        "product" => "BoltSips/" <> to_string(Application.spec(:bolt_sips, :vsn)),
        "platform" => platform_info(),
        "language" => "Elixir/" <> System.version()
      }
    }
  end

  defp platform_info() do
    {os_family, os_name} = :os.type()
    "#{os_family}/#{os_name}"
  end

  # Validate extra parameters for PULL and DISCARD messages
  # Valid keys: n (required), qid (optional)
  # n: Integer (-1 for all, or positive integer for specific count)
  # qid: Integer (-1 for last statement, or non-negative for specific query)
  @spec validate_pull_discard_extra(map()) :: {:ok, map()} | {:error, atom()}
  defp validate_pull_discard_extra(extra) when is_map(extra) do
    # Ensure n is present
    extra = Map.put_new(extra, :n, -1)

    # Validate n parameter
    n = Map.get(extra, :n)

    unless is_integer(n) and (n == -1 or n > 0) do
      {:error, :invalid_n_parameter}
    else
      # Validate qid parameter if present
      case Map.get(extra, :qid) do
        nil ->
          {:ok, extra}

        qid when is_integer(qid) and (qid == -1 or qid >= 0) ->
          {:ok, extra}

        _ ->
          {:error, :invalid_qid_parameter}
      end
    end
  end

  @doc """
  Perform the final message:
  - add header
  - manage chunk if necessary
  - add end marker
  """
  @spec encode_message(
          Bolt.Sips.Internals.PackStream.Message.out_signature(),
          integer(),
          list(),
          integer()
        ) ::
          [[Bolt.Sips.Internals.PackStream.Message.encoded()]]

  def encode_message(message_type, signature, data, bolt_version) do
    Bolt.Sips.Internals.Logger.log_message(:client, message_type, data)

    encoded =
      {signature, data}
      |> Bolt.Sips.Internals.PackStream.encode(bolt_version)
      |> generate_chunks([])

    Bolt.Sips.Internals.Logger.log_message(:client, message_type, encoded, :hex)
    encoded
  end

  @spec generate_chunks(Bolt.Sips.Internals.PackStream.value() | <<>>, list()) ::
          [[Bolt.Sips.Internals.PackStream.Message.encoded()]]
  defp generate_chunks(<<>>, chunks) do
    [chunks, [@end_marker], []]
  end

  defp generate_chunks(data, chunks) do
    data_size = :erlang.iolist_size(data)

    case data_size > @max_chunk_size do
      true ->
        bindata = :erlang.iolist_to_binary(data)
        <<chunk::binary-@max_chunk_size, rest::binary>> = bindata
        new_chunk = format_chunk(chunk)
        # [new_chunk, generate_chunks(rest,[])]
        generate_chunks(rest, [chunks, new_chunk])

      # generate_chunks(<<rest>>, [new_chunk, chunks])

      _ ->
        generate_chunks(<<>>, [chunks, format_chunk(data)])
    end
  end

  @spec format_chunk(Bolt.Sips.Internals.PackStream.value()) ::
          [Bolt.Sips.Internals.PackStream.Message.encoded()]
  defp format_chunk(chunk) do
    [<<:erlang.iolist_size(chunk)::16>>, chunk]
  end
end
