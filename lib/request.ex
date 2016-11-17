defmodule ExRiakCS.Request do
  import ExRiakCS.Utils

  @moduledoc false

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
    url = request_url(:put, path, headers, params)
    HTTPoison.put(url, body, headers)
  end
end
