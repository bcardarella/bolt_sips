defmodule Bolt.Sips.Internals.BoltVersionHelper do
  @moduledoc false

  # Available Bolt protocol versions (major versions only for simplicity)
  # v4 and v5 use major.minor format but we track by major version
  @available_bolt_versions [1, 2, 3, 4, 5]

  # Default minor versions for each major version when negotiating
  # Neo4j 2025.x supports Bolt 5.6+
  # Note: v5.5 is not used by any Neo4j server - skip it in negotiation
  @default_minor_versions %{
    1 => 0,
    2 => 0,
    3 => 0,
    4 => 4,  # 4.4 is the latest v4 minor
    5 => 6   # 5.6 is latest (5.5 is skipped, never used)
  }

  # Minor versions to skip in negotiation (never used by servers)
  @skip_minor_versions %{
    5 => [5]  # v5.5 is never used
  }

  @doc """
  List bolt versions.
  Only bolt version that have specific encoding functions are listed.
  """
  @spec available_versions() :: [integer()]
  def available_versions(), do: @available_bolt_versions

  @doc """
  Retrieve previous valid version.
  Return nil if there is no previous version.

  ## Example

      iex> Bolt.Sips.Internals.BoltVersionHelper.previous(2)
      1
      iex> Bolt.Sips.Internals.BoltVersionHelper.previous(1)
      nil
      iex> Bolt.Sips.Internals.BoltVersionHelper.previous(15)
      5
  """
  @spec previous(integer() | {integer(), integer()}) :: nil | integer()
  def previous(version) when is_tuple(version), do: previous(elem(version, 0))

  def previous(version) do
    @available_bolt_versions
    |> Enum.take_while(&(&1 < version))
    |> List.last()
  end

  @doc """
  Return the last available bolt version.

  ## Example:

      iex> Bolt.Sips.Internals.BoltVersionHelper.last()
      5
  """
  def last() do
    List.last(@available_bolt_versions)
  end

  @doc """
  Get the default minor version for a major version.
  """
  @spec default_minor(integer()) :: integer()
  def default_minor(major_version) do
    Map.get(@default_minor_versions, major_version, 0)
  end

  @doc """
  Encode a version for the handshake.
  For v1-v3: just the major version as 32-bit integer
  For v4+: uses the format <<reserved::8, range::8, minor::8, major::8>>

  The range byte allows specifying support for consecutive minor versions.
  For example, range=2 with minor=4 and major=5 means support for 5.4, 5.3, 5.2
  """
  @spec encode_version(integer()) :: binary()
  def encode_version(major) when major <= 3 do
    <<major::32>>
  end

  def encode_version(major) when major >= 4 do
    minor = default_minor(major)
    # range of 4 means we support minor, minor-1, minor-2, minor-3, minor-4
    # This gives us flexibility to connect to servers with slightly older minors
    # For v5, this means: 5.6, 5.4, 5.3, 5.2, 5.1 (skipping 5.5 which is never used)
    range = min(minor, 4)
    <<0::8, range::8, minor::8, major::8>>
  end

  @doc """
  Get the list of minor versions to skip for a major version.
  Some minor versions (like v5.5) are never used by servers.
  """
  @spec skip_minor_versions(integer()) :: [integer()]
  def skip_minor_versions(major) do
    Map.get(@skip_minor_versions, major, [])
  end

  @doc """
  Decode a version from the server's handshake response.
  Returns {major, minor} tuple for v4+ or just major for v1-v3.
  """
  @spec decode_version(binary()) :: integer() | {integer(), integer()}
  def decode_version(<<0::16, 0::8, major::8>>) when major <= 3 do
    major
  end

  def decode_version(<<0::8, _range::8, minor::8, major::8>>) when major >= 4 do
    {major, minor}
  end

  def decode_version(<<version::32>>) when version <= 3 do
    version
  end

  def decode_version(<<version::32>>) do
    # Fallback: treat as major version only
    version
  end

  @doc """
  Get the major version from a version (handles both integer and tuple formats).
  """
  @spec major_version(integer() | {integer(), integer()}) :: integer()
  def major_version({major, _minor}), do: major
  def major_version(major) when is_integer(major), do: major

  @doc """
  Get the minor version from a version (handles both integer and tuple formats).
  Returns 0 for v1-v3.
  """
  @spec minor_version(integer() | {integer(), integer()}) :: integer()
  def minor_version({_major, minor}), do: minor
  def minor_version(_major), do: 0
end
