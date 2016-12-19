defmodule ExRiakCS.Request do
  import ExRiakCS.Utils

  @moduledoc false

  def request(type, path, opts) when is_list(opts) do
    params = opts[:params] || %{}
    headers = opts[:headers] || %{}
    body = opts[:body] || []

    url = ExRiakCS.request_url(type, path, opts)
    HTTPoison.request!(type, url, body, headers)
  end

  def request(type, path, params \\ %{}, headers \\ %{}, body \\ []) do
    url = request_url(type, path, headers, params)
    HTTPoison.request!(type, url, body, headers)
  end

  def get_async(target, path, params \\ %{}, headers \\ %{}) do
    url = request_url(:get, path, headers, params)
    HTTPoison.get!(url, headers, stream_to: target)
  end

  def get_async_throttled(target, path, params \\ %{}, headers \\ %{}) do
    url = request_url(:get, path, headers, params)
    HTTPoison.get!(url, headers, stream_to: target, async: :once)
  end

  def put(path, body, params \\ %{}, headers \\ %{}) do
    import ExRiakCS.Config, only: [base_url: 0, key_id: 0]
    url = request_url(:put, path, headers, params)
    HTTPoison.put(url, body, headers, timeout: :infinity)
  end

  def put_stream(path, file_stream, params \\ %{}, headers \\ %{}) do
    url = request_url(:put, path, headers, params)
    stream = ExRiakCS.Object.UploadStream.new(path, file_stream)
    HTTPoison.put(url, stream, headers)
  end
end
