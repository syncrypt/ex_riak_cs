defmodule ExRiakCS.Bucket do
  alias ExRiakCS.Request
  import ExRiakCS.Config

  def put(bucket_name, acl \\ ExRiakCS.Config.acl) do
    headers = %{"x-amz-acl" => acl}

    case Request.request(:put, "/#{bucket_name}", %{}, headers) do
      %{status_code: 200, headers: headers, body: body} -> :ok
      %{status_code: code, body: body} -> {:error, {code, body}}
    end
  end

  def delete(bucket_name) do
    case Request.request(:delete, "/#{bucket_name}") do
      %{status_code: 204, headers: headers, body: body} -> :ok
      %{status_code: code, body: body} -> {:error, {code, body}}
    end
  end
end
