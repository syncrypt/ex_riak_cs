defmodule ExRiakCS.Object.DownloadStream do
  require Logger
  alias ExRiakCS.Request
  alias __MODULE__

  @timeout ExRiakCS.Config.get_stream_timeout

  defstruct [:id, :path]

  @doc """
  Creates a DownloadStream without starting it.
  """
  def new(path) do
    Elixir.Stream.resource(
      fn -> %DownloadStream{path: path} end,
      &next/1,
      &cleanup/1
    )
  end

  @doc """
  Starts a given DownloadStream & checks if the HTTP resource it tries to download
  exists.
  Returns {:ok, %DownloadStream{}} on success, {:error, %HTTPoison.Error{}} otherwise.
  """
  def start(%DownloadStream{id: nil, path: path} = stream) do
    Logger.debug "ExRiakCS.Object.DownloadStream #{inspect stream} | Starting"
    Logger.debug "ExRiakCS.Object.DownloadStream #{inspect stream} | Starting"

    %{stream | id: Request.get_async_throttled(self, path)}
    |> read_status
    |> read_headers
  end

  @doc """
  Creates and starts a new DownloadStream for a given path.
  """
  def start(path) when is_binary(path) do
    with {:ok, stream} <- %DownloadStream{path: path} |> start do
      stream = Elixir.Stream.resource(
        fn -> stream end,
        &next/1,
        &cleanup/1
      )
      {:ok, stream}
    end
  end

  # start stream lazily when we call `next` the first time and the stream
  # hasn't yet been started.
  def next(%DownloadStream{id: nil} = stream) do
    with {:ok, stream} <- stream |> start do
      stream
      |> next
    end
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
        {:ok, stream}

      %HTTPoison.AsyncStatus{code: code} when code in 400..499 ->
        {:error, %HTTPoison.Error{reason: "File not found: #{stream.path}", id: stream.id}}

      after @timeout ->
        raise "ExRiakCS.Object.DownloadStream #{inspect stream} | Timed out"
    end
  end

  defp read_headers({:error, _} = err), do: err

  defp read_headers({:ok, stream}) do
    stream_next(stream)
    receive do
      %HTTPoison.AsyncHeaders{} ->
        {:ok, stream}

      after @timeout ->
        raise "ExRiakCS.Object.DownloadStream #{inspect stream} timed out"
    end
  end

  def stream_next(%DownloadStream{id: id}), do: HTTPoison.stream_next(id)
end
