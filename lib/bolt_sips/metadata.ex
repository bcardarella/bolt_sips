defmodule Bolt.Sips.Metadata do
  @moduledoc """
  Transaction and query metadata for Bolt protocol v3+.

  ## Fields

  - `:bookmarks` - List of bookmarks for causal consistency
  - `:tx_timeout` - Transaction timeout in milliseconds
  - `:metadata` - Generic metadata map for logging
  - `:db` - Database name (v4+)
  - `:mode` - Access mode: "r" for read, "w" for write (v4+)
  - `:imp_user` - Impersonated user (v4.4+)
  - `:notifications_minimum_severity` - Minimum severity for notifications (v5.2+)
  - `:notifications_disabled_classifications` - Disabled notification classifications (v5.6+)

  ## Example

      {:ok, metadata} = Bolt.Sips.Metadata.new(%{
        bookmarks: ["neo4j:bookmark:v1:tx123"],
        tx_timeout: 5000,
        db: "neo4j",
        mode: "r"
      })
  """

  defstruct [
    :bookmarks,
    :tx_timeout,
    :metadata,
    :db,
    :mode,
    :imp_user,
    :notifications_minimum_severity,
    :notifications_disabled_classifications
  ]

  @type t :: %__MODULE__{
          bookmarks: [String.t()] | nil,
          tx_timeout: non_neg_integer() | nil,
          metadata: map() | nil,
          db: String.t() | nil,
          mode: String.t() | nil,
          imp_user: String.t() | nil,
          notifications_minimum_severity: String.t() | nil,
          notifications_disabled_classifications: [String.t()] | nil
        }

  @valid_modes ["r", "w"]
  @valid_severities ["OFF", "WARNING", "INFORMATION"]

  alias Bolt.Sips.Metadata

  @doc """
  Create a new metadata structure.
  Data must be valid.
  """
  @spec new(map()) :: {:ok, Bolt.Sips.Metadata.t()} | {:error, String.t()}
  def new(data) do
    with {:ok, data} <- check_keys(data),
         {:ok, bookmarks} <- validate_bookmarks(Map.get(data, :bookmarks, [])),
         {:ok, tx_timeout} <- validate_timeout(Map.get(data, :tx_timeout)),
         {:ok, metadata} <- validate_metadata(Map.get(data, :metadata, %{})),
         {:ok, db} <- validate_db(Map.get(data, :db)),
         {:ok, mode} <- validate_mode(Map.get(data, :mode)),
         {:ok, imp_user} <- validate_imp_user(Map.get(data, :imp_user)),
         {:ok, severity} <- validate_severity(Map.get(data, :notifications_minimum_severity)),
         {:ok, classifications} <-
           validate_classifications(Map.get(data, :notifications_disabled_classifications)) do
      {:ok,
       %__MODULE__{
         bookmarks: bookmarks,
         tx_timeout: tx_timeout,
         metadata: metadata,
         db: db,
         mode: mode,
         imp_user: imp_user,
         notifications_minimum_severity: severity,
         notifications_disabled_classifications: classifications
       }}
    else
      error -> error
    end
  end

  @doc """
  Convert the Metadata struct to a map suitable for Bolt protocol.
  All `nil` values will be stripped.
  """
  @spec to_map(Bolt.Sips.Metadata.t()) :: map()
  def to_map(%Metadata{} = metadata) do
    metadata
    |> Map.from_struct()
    |> Enum.filter(fn {_, value} -> value != nil end)
    |> Enum.into(%{})
  end

  defp check_keys(data) when is_map(data) do
    valid_keys =
      MapSet.new([
        :bookmarks,
        :tx_timeout,
        :metadata,
        :db,
        :mode,
        :imp_user,
        :notifications_minimum_severity,
        :notifications_disabled_classifications
      ])

    data_keys = MapSet.new(Map.keys(data))
    invalid_keys = MapSet.difference(data_keys, valid_keys)

    if MapSet.size(invalid_keys) == 0 do
      {:ok, struct(Metadata, data)}
    else
      {:error, "[Metadata] Invalid keys: #{inspect(MapSet.to_list(invalid_keys))}"}
    end
  end

  defp check_keys(_), do: {:error, "[Metadata] Data must be a map"}

  @spec validate_bookmarks(any()) :: {:ok, list() | nil} | {:error, String.t()}
  defp validate_bookmarks(bookmarks) when is_list(bookmarks) and length(bookmarks) > 0 do
    {:ok, bookmarks}
  end

  defp validate_bookmarks([]), do: {:ok, nil}
  defp validate_bookmarks(nil), do: {:ok, nil}

  defp validate_bookmarks(_) do
    {:error, "[Metadata] Invalid bookmarks. Should be a list."}
  end

  @spec validate_timeout(any()) :: {:ok, integer() | nil} | {:error, String.t()}
  defp validate_timeout(timeout) when is_integer(timeout) and timeout > 0 do
    {:ok, timeout}
  end

  defp validate_timeout(nil), do: {:ok, nil}

  defp validate_timeout(_) do
    {:error, "[Metadata] Invalid timeout. Should be a positive integer."}
  end

  @spec validate_metadata(any()) :: {:ok, map() | nil} | {:error, String.t()}
  defp validate_metadata(metadata) when is_map(metadata) and map_size(metadata) > 0 do
    {:ok, metadata}
  end

  defp validate_metadata(%{}), do: {:ok, nil}
  defp validate_metadata(nil), do: {:ok, nil}

  defp validate_metadata(_) do
    {:error, "[Metadata] Invalid metadata. Should be a valid map or nil."}
  end

  @spec validate_db(any()) :: {:ok, String.t() | nil} | {:error, String.t()}
  defp validate_db(db) when is_binary(db) and byte_size(db) > 0 do
    {:ok, db}
  end

  defp validate_db(nil), do: {:ok, nil}

  defp validate_db(_) do
    {:error, "[Metadata] Invalid db. Should be a non-empty string."}
  end

  @spec validate_mode(any()) :: {:ok, String.t() | nil} | {:error, String.t()}
  defp validate_mode(mode) when mode in @valid_modes do
    {:ok, mode}
  end

  defp validate_mode(nil), do: {:ok, nil}

  defp validate_mode(_) do
    {:error, "[Metadata] Invalid mode. Should be \"r\" (read) or \"w\" (write)."}
  end

  @spec validate_imp_user(any()) :: {:ok, String.t() | nil} | {:error, String.t()}
  defp validate_imp_user(user) when is_binary(user) and byte_size(user) > 0 do
    {:ok, user}
  end

  defp validate_imp_user(nil), do: {:ok, nil}

  defp validate_imp_user(_) do
    {:error, "[Metadata] Invalid imp_user. Should be a non-empty string."}
  end

  @spec validate_severity(any()) :: {:ok, String.t() | nil} | {:error, String.t()}
  defp validate_severity(severity) when severity in @valid_severities do
    {:ok, severity}
  end

  defp validate_severity(nil), do: {:ok, nil}

  defp validate_severity(_) do
    {:error,
     "[Metadata] Invalid notifications_minimum_severity. Should be \"OFF\", \"WARNING\", or \"INFORMATION\"."}
  end

  @spec validate_classifications(any()) :: {:ok, [String.t()] | nil} | {:error, String.t()}
  defp validate_classifications(classifications)
       when is_list(classifications) and length(classifications) > 0 do
    if Enum.all?(classifications, &is_binary/1) do
      {:ok, classifications}
    else
      {:error,
       "[Metadata] Invalid notifications_disabled_classifications. Should be a list of strings."}
    end
  end

  defp validate_classifications([]), do: {:ok, nil}
  defp validate_classifications(nil), do: {:ok, nil}

  defp validate_classifications(_) do
    {:error,
     "[Metadata] Invalid notifications_disabled_classifications. Should be a list of strings."}
  end
end
