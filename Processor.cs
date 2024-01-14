using System.Buffers;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace _1brc;

internal class Processor {
  private const int BUFFER_SIZE = 4096;
  private readonly ArrayPool<byte> _bytePool = ArrayPool<byte>.Shared;
  private readonly FileStream _fileStream;
  private readonly MemoryMappedFile _mmf;
  private readonly int _processorCount;
  private readonly UTF8Encoding _utf8 = new();
  public readonly ConcurrentDictionary<string, Stats> StationStats = new();

  public Processor(FileStream fileStream, int processorCount = 1) {
    _fileStream = fileStream;
    _processorCount = processorCount;
    _mmf = MemoryMappedFile.CreateFromFile(
      _fileStream.SafeFileHandle,
      access: MemoryMappedFileAccess.Read,
      inheritability: HandleInheritability.None,
      mapName: null,
      leaveOpen: true,
      capacity: 0);
  }

  public void ProcessChunks(IEnumerable<Chunk> chunks) {
    var partitioner = Partitioner.Create(chunks);
    Parallel.ForEach(partitioner,
                     InitDict,
                     (chunk, loopState, loopIndex) => ReadChunkBody(InitDict(), chunk, loopState),
                     LocalMerge);
    return;

    Dictionary<string, Stats> InitDict() {
      return new Dictionary<string, Stats>();
    }

    Dictionary<string, Stats> ReadChunkBody(Dictionary<string, Stats> localDict, Chunk chunk,
                                            ParallelLoopState loopState) {
      ReadChunk(chunk, localDict);
      return localDict;
    }

    void LocalMerge(Dictionary<string, Stats> localDict) {
      foreach (var (station, value) in localDict)
        StationStats.AddOrUpdate(station, value, (_, existingValue) => {
          existingValue.Min = Math.Min(existingValue.Min, value.Min);
          existingValue.Max = Math.Max(existingValue.Max, value.Max);
          existingValue.Sum += value.Sum;
          existingValue.Count += value.Count;
          return existingValue;
        });
    }
  }

  public Chunk[] Chunks() {
    var chunks = new Chunk[_processorCount];
    var chunkSizeInBytes = _fileStream.Length / _processorCount;
    long chunkStartPosition = 0;

    for (var chunkIndex = 0; chunkIndex < _processorCount; chunkIndex++) {
      var chunkEndPosition = GetChunkEndPosition(chunkStartPosition, chunkSizeInBytes);
      chunks[chunkIndex] = new Chunk(chunkStartPosition, chunkEndPosition - chunkStartPosition);
      chunkStartPosition = chunkEndPosition;
    }

    return chunks.Where(chunk => chunk.Size > 0).ToArray();
  }

  private long GetChunkEndPosition(long chunkStartPosition, long chunkSizeInBytes) {
    var proposedEndPosition = chunkStartPosition + chunkSizeInBytes;
    if (proposedEndPosition > _fileStream.Length) return _fileStream.Length;
    _fileStream.Seek(proposedEndPosition, SeekOrigin.Begin);
    while (_fileStream.Position < _fileStream.Length && _fileStream.ReadByte() != '\n') ;
    return _fileStream.Position;
  }

  private void ReadChunk(Chunk dataChunk, Dictionary<string, Stats> stationStatistics) {
    var optimalBufferSize = Math.Min(BUFFER_SIZE, (int)dataChunk.Size);
    var rentedBuffer = _bytePool.Rent(optimalBufferSize);
    var currentOffset = 0L;
    var remainingBytes = 0;

    try {
      using var memoryAccessor = _mmf.CreateViewAccessor(
        dataChunk.Position,
        dataChunk.Size,
        MemoryMappedFileAccess.Read);

      while (currentOffset < dataChunk.Size) {
        var bytesReadCount = memoryAccessor.ReadArray(currentOffset, rentedBuffer, remainingBytes, optimalBufferSize - remainingBytes);
        var bufferToProcess = new Span<byte>(rentedBuffer, 0, bytesReadCount + remainingBytes);
        var unprocessedData = ProcessBuffer(bufferToProcess, stationStatistics);
        remainingBytes = unprocessedData.Length;
        if (remainingBytes > 0) unprocessedData.CopyTo(new Span<byte>(rentedBuffer, 0, remainingBytes));
        currentOffset += bytesReadCount;
      }
    }
    finally {
      _bytePool.Return(rentedBuffer);
    }
  }

  private Span<byte> ProcessBuffer(Span<byte> buffer, Dictionary<string, Stats> stationStats) {
    var bufferStart = 0;
    var newlineIndex = buffer.IndexOf((byte)'\n');
    while (newlineIndex != -1) {
      var lineBuffer = buffer.Slice(bufferStart, newlineIndex - bufferStart);
      if (lineBuffer.Length > 0) ProcessBufferLine(lineBuffer, stationStats);
      buffer = buffer.Slice(newlineIndex + 1);
      bufferStart = 0;
      newlineIndex = buffer.IndexOf((byte)'\n');
    }
    return buffer;
  }

  private void ProcessBufferLine(Span<byte> line, Dictionary<string, Stats> stationStats) {
    var semicolonIndex = line.IndexOf((byte)';');
    var stationNameSpan = line.Slice(0, semicolonIndex);
    var valueSpan = line.Slice(semicolonIndex + 1);

    var stationName = _utf8.GetString(stationNameSpan);
    if (!Utf8Parser.TryParse(valueSpan, out double value, out _)) throw new FormatException("Invalid double value.");

    if (stationStats.TryGetValue(stationName, out var stats)) {
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

  public struct Stats {
    public double Min;
    public double Max;
    public double Sum;
    public long Count;
  }
}

