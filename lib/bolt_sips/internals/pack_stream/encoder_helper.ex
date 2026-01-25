defmodule Bolt.Sips.Internals.PackStream.EncoderHelper do
  @moduledoc false
  alias Bolt.Sips.Internals.BoltVersionHelper
  alias Bolt.Sips.Internals.PackStreamError

  use Bolt.Sips.Internals.PackStream.V1
  use Bolt.Sips.Internals.PackStream.V2
  use Bolt.Sips.Internals.PackStream.Utils

  @available_bolt_versions BoltVersionHelper.available_versions()
  @last_version BoltVersionHelper.last()

  @doc """
  For the given `data_type` and `bolt_version`, determine the right encoding function
  and call it against `data`.

  Accepts both integer versions (1, 2, 3) and tuple versions ({4, 4}, {5, 4}).
  """
  @spec call_encode(atom(), any(), any()) :: binary() | PackStreamError.t()

  # Handle tuple versions by extracting major version
  def call_encode(data_type, data, {major, _minor}) when is_integer(major) do
    call_encode(data_type, data, major)
  end

  def call_encode(data_type, data, bolt_version)
      when is_integer(bolt_version) and bolt_version in @available_bolt_versions do
    do_call_encode(data_type, data, bolt_version)
  end

  def call_encode(data_type, data, bolt_version) when is_integer(bolt_version) do
    if bolt_version > @last_version do
      call_encode(data_type, data, @last_version)
    else
      raise PackStreamError,
        data_type: data_type,
        data: data,
        bolt_version: bolt_version,
        message: "Unsupported encoder version"
    end
  end

  def call_encode(data_type, data, bolt_version) do
    raise PackStreamError,
      data_type: data_type,
      data: data,
      bolt_version: bolt_version,
      message: "Unsupported encoder version"
  end
end
