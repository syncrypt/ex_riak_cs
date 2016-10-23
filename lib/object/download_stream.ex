defmodule ExRiakCS.Object.DownloadStream do
  require Logger
  alias ExRiakCS.Request
  alias __MODULE__

  @timeout ExRiakCS.Config.stream_get_timeout

  defstruct [:id, :path]

  def new(path) do
    Elixir.Stream.resource(
      fn -> %DownloadStream{path: path} end,
      &next/1,
      &cleanup/1
    )
  end

  # start stream lazily when we call `next` the first time.
  def next(%DownloadStream{id: nil, path: path} = stream) do
    Logger.debug "ExRiakCS.Object.DownloadStream #{inspect stream} | Starting"

    %{stream | id: Request.get_async_throttled(self, path)}
    |> read_status
    |> read_headers
    |> next
  end

  def next(stream) do
    stream_next(stream)
    receive do
      %HTTPoison.AsyncChunk{chunk: data} ->
        {[data], stream}

      %HTTPoison.AsyncEnd{} ->
        {:halt, stream}

      other ->
        raise "ExRiakCS.Object.DownloadStream #{inspect stream} | Unexpected message: #{inspect other}"

      after @timeout ->
        raise "ExRiakCS.Object.DownloadStream #{inspect stream} | Timed out"
    end
  end

  def cleanup(stream) do
    Logger.debug "ExRiakCS.Object.DownloadStream #{inspect stream} | Finished"
  end

  defp read_status(stream) do
    receive do
      %HTTPoison.AsyncStatus{code: 200} ->
        stream

      after @timeout ->
        raise "ExRiakCS.Object.DownloadStream #{inspect stream} | Timed out"
    end
  end

  defp read_headers(stream) do
    stream_next(stream)
    receive do
      %HTTPoison.AsyncHeaders{} ->
        stream

      after @timeout ->
        raise "ExRiakCS.Object.DownloadStream #{inspect stream} timed out"
    end
  end

  def stream_next(%DownloadStream{id: id}), do: HTTPoison.stream_next(id)
end
