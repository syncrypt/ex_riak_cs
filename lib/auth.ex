defmodule ExRiakCS.Auth do
  import ExRiakCS.Auth.Utils
  import ExRiakCS.Config

  @moduledoc false

  def signature_params(path, request_type, headers \\ %{}, opts \\ []) do
    expires = case opts[:expires] do
      t when is_number(t) ->
        t
      nil ->
        expiration_date(opts[:exp_days] || exp_days)
    end
    %{
      "AWSAccessKeyId": opts[:key_id] || key_id,
      "Expires": expires,
      "Signature": signature(request_type, expires, path, headers, opts)
      }
  end

  defp signature(request_type, exp_date, path, headers, opts \\ []) do
    string = string_to_sign(request_type, exp_date, path, headers)
    string |> encrypt(opts[:secret_key] || secret_key)
  end

  defp string_to_sign(request_type, exp_date, path, headers) do
    content_type = Map.get(headers, "Content-Type")
    headers = Map.delete(headers, "Content-Type")
    string_to_sign = ""
    string_to_sign = string_to_sign <> request_type <> "\n\n"
    string_to_sign = if content_type, do: string_to_sign <> content_type <> "\n", else: string_to_sign <> "\n"
    string_to_sign = string_to_sign <> Integer.to_string(exp_date) <> "\n"
    string_to_sign = Enum.reduce(headers, string_to_sign, fn(header, string_to_sign) ->
                                                            {key, value} = header
                                                            string_to_sign <> "#{key}:" <> value <> "\n"
                                                          end)
    string_to_sign <> path
  end
end
