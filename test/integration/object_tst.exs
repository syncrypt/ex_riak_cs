defmodule ExRiakCS.MultipartUploadIntegrationTest do
  use ExUnit.Case, async: true
  import ExRiakCS.ObjectHelpers
  alias ExRiakCS.Object

  @bucket "test-bucket"

  test "deletes file" do
    file = "./test/files/file.mp3"
    key = "key#{:rand.uniform(1000)}"
    {:ok, _} = upload_object(file, @bucket, key, "audio/mp3")
    {:ok, _} = Object.delete(@bucket, key)
  end

  test "gets file headers" do
    file = "./test/files/file.mp3"
    key = "key#{:rand.uniform(1000)}"
    {:ok, _} = upload_object(file, @bucket, key, "audio/mp3")
    {:ok, _} = Object.head(@bucket, key)
  end

  test "uploads a file" do
    file = "./test/files/file.mp3"
    key = "key#{:rand.uniform(1000)}"
    chunk_size = 10000
    file_size = File.stat!(file).size
    file_stream = File.stream!(file, [:binary], chunk_size)
    {:ok, _} = Object.put_stream(@bucket, key, file_size, chunk_size, file_stream)
    {:ok, _} = Object.head(@bucket, key)
    {:ok, _} = Object.delete(@bucket, key)
  end
end
