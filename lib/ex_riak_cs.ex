defmodule ExRiakCS do
  alias ExRiakCS.Utils

  def request_url(request_type, path, opts \\ []) when is_list(opts) do
    params = opts[:params] || %{}
    headers = opts[:headers] || %{}
    root = opts[:root] || ExRiakCS.Config.base_url

    params = Utils.encode_params(request_type, path, headers, params)
    root <> Utils.path_without_params(path) <> "?" <> params
  end
end
