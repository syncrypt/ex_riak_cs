defmodule ExRiakCS.Object do
  alias ExRiakCS.Request
  import ExRiakCS.Config

  @moduledoc """
  This module contains object-level operations

  More info at: http://docs.basho.com/riak/cs/2.1.1/references/apis/storage/#object-level-operations
  """

  @doc """
  Deletes an object and returns {:ok, nil} if request was successful, otherwise returns {:error, {response_code, response_body} }

  ## Example

      {:ok, _} = Object.delete("test-bucket", "key")

  More info at: http://docs.basho.com/riak/cs/2.1.1/references/apis/storage/s3/delete-object/
  """
  def delete(bucket, key) do
    case Request.request(:delete, bucket |> path(key)) do
      %{status_code: 204} -> {:ok, nil}
      %{status_code: code, body: body} -> {:error, {code, body}}
    end
  end

  @doc """
  Retrieves object metadata (not the full content of the object) and returns {:ok, headers} if request was successful, otherwise returns {:error, {response_code, response_body} }

  ## Example

      {:ok, headers} = Object.head("test-bucket", "key")

  More info at: http://docs.basho.com/riak/cs/2.1.1/references/apis/storage/s3/head-object/
  """

  def head(bucket, key) do
    case Request.request(:head, bucket |> path(key)) do
      %{status_code: 200, headers: headers} -> {:ok, headers}
      %{status_code: code, body: body} -> {:error, {code, body}}
    end
  end


  def get(bucket, key) do
    case Request.request(:get, bucket |> path(key)) do
      %{status_code: 200, body: body} -> {:ok, body}
      %{status_code: code, body: body} -> {:error, {code, body}}
    end
  end

  def get_stream(bucket, key) do
    alias ExRiakCS.Object.DownloadStream

    bucket
    |> path(key)
    |> DownloadStream.start
  end

  @min_part_size 5 * 1024 * 1024 # 5MB

  def put_stream(bucket, key,
                 file_size, stream_chunk_size, data_stream,
                 mime_type \\ "application/octet-stream") do

    if file_size < @min_part_size do
      single_put_stream(bucket, key, file_size, stream_chunk_size, data_stream, mime_type)
    else
      multipart_put_stream(bucket, key, file_size, stream_chunk_size, data_stream, mime_type)
    end
  end

  def single_put_stream(bucket, key, file_size, stream_chunk_size, data_stream, mime_type) do
    headers = %{"Content-Type" => mime_type,
                "x-amz-acl" => acl}

    data = data_stream |> Enum.into([])

    bucket
    |> path(key)
    |> Request.put(data, %{}, headers)
  end

  def multipart_put_stream(bucket, key, file_size, stream_chunk_size, data_stream, mime_type) do
    alias ExRiakCS.MultipartUpload

    {:ok, upload_id} = MultipartUpload.initiate_multipart_upload(bucket, key, mime_type)

    chunk_by = (@min_part_size / stream_chunk_size) |> Float.ceil |> round

    parts =
      data_stream
      |> Stream.chunk(chunk_by, chunk_by, [])
      |> Stream.with_index
      |> Enum.reduce([], fn({data, number}, parts) ->
        {:ok, part_etag} = MultipartUpload.upload_part(bucket, key, upload_id, number + 1, data)
        [{number + 1, part_etag} | parts]
      end)
      |> Enum.reverse

    MultipartUpload.complete_multipart_upload(bucket, key, upload_id, parts)
  end

  def path(bucket, key), do: "/#{bucket}/#{key}"
end
