defmodule ExRiakCS.Request do
  import ExRiakCS.Utils

  @moduledoc false

  def request(type, path, params \\ %{}, headers \\ %{}, body \\ []) do
    url = request_url(type, path, headers, params)
    {:ok, %HTTPoison.Response{
      status_code: code,
      body: body,
      headers: headers}} = HTTPoison.request(type, url, body, headers)
    %{status_code: code, body: body, headers: headers}
  end

  def async_get(target, path, params \\ %{}, headers \\ %{}) do
    url = request_url(:get, path, headers, params)
    HTTPoison.get!(url, headers, stream_to: target)
  end
end
