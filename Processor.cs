using System.Buffers;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace _1brc;

internal class Processor {
  private const int BUFFER_SIZE = 4096;
  private const int CHUNK_SIZE = 256 * 1024 * 1024;
  private readonly ArrayPool<byte> _bytePool = ArrayPool<byte>.Shared;
  private readonly FileStream _fileStream;
  private readonly MemoryMappedFile _mmf;

  public Processor(FileStream fileStream) {
    _fileStream = fileStream;
    _mmf = MemoryMappedFile.CreateFromFile(
      _fileStream.SafeFileHandle,
      access: MemoryMappedFileAccess.Read,
      inheritability: HandleInheritability.None,
      mapName: null,
      leaveOpen: true,
      capacity: fileStream.Length);
  }

  public readonly ConcurrentDictionary<ByteArray, Stats> StationStats = new();

  public void ProcessChunks(Chunk[] chunks) {
    OrderablePartitioner<Chunk> partitioner = Partitioner.Create(chunks, true);
    Parallel.ForEach(partitioner,
                     InitDict,
                     ReadChunkBody,
                     LocalMerge);

    Dictionary<ByteArray, Stats> InitDict() => new();

    Dictionary<ByteArray, Stats> ReadChunkBody(Chunk chunk, ParallelLoopState loopState, long index,
                                               Dictionary<ByteArray, Stats> localDict) {
      ReadChunk(chunk, localDict);
      return localDict;
    }

    void LocalMerge(Dictionary<ByteArray, Stats> localDict) {
      foreach ((ByteArray station, Stats value) in localDict)
        StationStats.AddOrUpdate(station, value, (_, existingValue) => {
          existingValue.Min = Math.Min(existingValue.Min, value.Min);
          existingValue.Max = Math.Max(existingValue.Max, value.Max);
          existingValue.Sum += value.Sum;
          existingValue.Count += value.Count;
          return existingValue;
        });
    }

    _mmf.Dispose();
  }

  public Chunk[] Chunks() {
    int chunkCount = (int)Math.Ceiling((double)_fileStream.Length / CHUNK_SIZE);
    Chunk[] chunks = new Chunk[chunkCount];
    long chunkStartPosition = 0;
    for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
      long chunkEndPosition = GetChunkEndPosition(chunkStartPosition, CHUNK_SIZE);
      chunks[chunkIndex] = new Chunk(chunkStartPosition, chunkEndPosition - chunkStartPosition);
      chunkStartPosition = chunkEndPosition;
    }

    return chunks.ToArray();
  }

  private long GetChunkEndPosition(long chunkStartPosition, long chunkSizeInBytes) {
    long proposedEndPosition = chunkStartPosition + chunkSizeInBytes;
    if (proposedEndPosition > _fileStream.Length) return _fileStream.Length;
    _fileStream.Seek(proposedEndPosition, SeekOrigin.Begin);
    while (_fileStream.Position < _fileStream.Length && _fileStream.ReadByte() != '\n') ;
    return _fileStream.Position;
  }

  private void ReadChunk(Chunk dataChunk, Dictionary<ByteArray, Stats> stationStats) {
    int optimalBufferSize = Math.Min(BUFFER_SIZE, (int)dataChunk.Size);
    byte[] rentedBuffer = _bytePool.Rent(optimalBufferSize);
    long currentOffset = 0L;

    try {
      using MemoryMappedViewStream stream = _mmf.CreateViewStream(dataChunk.Position, dataChunk.Size, MemoryMappedFileAccess.Read);
      while (currentOffset < dataChunk.Size) {
        int bytesReadCount = stream.Read(rentedBuffer, 0, optimalBufferSize);
        Span<byte> bufferToProcess = new(rentedBuffer, 0, bytesReadCount);
        int unprocessedDataCount = ProcessBuffer(bufferToProcess, stationStats);
        if (unprocessedDataCount > 0) stream.Position -= unprocessedDataCount;
        currentOffset += bytesReadCount - unprocessedDataCount;
      }
    }
    finally {
      _bytePool.Return(rentedBuffer);
    }
  }

  private static int ProcessBuffer(Span<byte> buffer, Dictionary<ByteArray, Stats> stationStats) {
    int newlineIndex;
    Span<byte> remainingBuffer = buffer;
    while ((newlineIndex = remainingBuffer.IndexOf((byte)'\n')) != -1) {
      Span<byte> lineBuffer = remainingBuffer.Slice(0, newlineIndex);
      ProcessBufferLine(lineBuffer, stationStats);
      remainingBuffer = remainingBuffer.Slice(newlineIndex + 1);
    }
    return remainingBuffer.Length;
  }

  private static void ProcessBufferLine(Span<byte> line, Dictionary<ByteArray, Stats> stationStats) {
    int semicolonIndex = line.IndexOf((byte)';');
    Span<byte> stationNameSpan = line.Slice(0, semicolonIndex);
    Span<byte> valueSpan = line.Slice(semicolonIndex + 1);

    ByteArray stationName = new(stationNameSpan.ToArray());
    if (!Utf8Parser.TryParse(valueSpan, out double value, out _)) throw new FormatException("Invalid double value.");

    if (stationStats.TryGetValue(stationName, out Stats? stats)) {
      stats.Min = Math.Min(stats.Min, value);
      stats.Max = Math.Max(stats.Max, value);
      stats.Sum += value;
      stats.Count += 1;
    }
    else {
      stats = new Stats {
        Min = value,
        Max = value,
        Sum = value,
        Count = 1
      };
      stationStats.TryAdd(stationName, stats);
    }
  }

  public class Chunk(long position, long length) {
    public long Position { get; } = position;
    public long Size { get; } = length;
  }

  public class Stats {
    public double Min;
    public double Max;
    public double Sum;
    public long Count;
  }

  public readonly struct ByteArray(byte[] data) : IComparable<ByteArray> {
    private readonly byte[] _data = data;

    public override bool Equals(object? obj) {
      if (obj is not ByteArray other) return false;

      int diff = 0;
      for (int i = 0; i < _data.Length; i++) {
        diff |= _data[i] ^ other._data[i];
        if (diff != 0)
          break;
      }

      return diff == 0;
    }

    public override int GetHashCode() {
      unchecked {
        const int prime = 16777619;
        int hash = (int)2166136261;

        int i = 0;
        for (; i < _data.Length; i++) {
          hash ^= _data[i];
          hash *= prime;
        }

        return hash;
      }
    }

    public string GetString() {
      return Encoding.UTF8.GetString(_data);
    }

    public int CompareTo(ByteArray other) {
      int minLength = Math.Min(_data.Length, other._data.Length);
      int comparison = 0;
      for (int i = 0; i < minLength; i++) {
        comparison |= _data[i] - other._data[i] << i * 8;
        if (comparison != 0)
          break;
      }
      return comparison != 0 ? comparison : _data.Length.CompareTo(other._data.Length);
    }
  }
}
