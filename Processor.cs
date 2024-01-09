using System.Buffers;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace _1brc;

internal class Processor {
  private const int BufferSize = 4096;
  private readonly ArrayPool<byte> _bytePool = ArrayPool<byte>.Shared;
  private readonly ArrayPool<char> _charPool = ArrayPool<char>.Shared;
  private readonly FileStream _fileStream;
  private readonly MemoryMappedFile _mmf;
  private readonly int _processorCount;
  public Dictionary<string, Stats> StationStats = [];

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
      lock (StationStats) {
        foreach (var pair in localDict)
          if (StationStats.TryGetValue(pair.Key, out var value))
            StationStats[pair.Key] = new Stats {
              Min = Math.Min(value.Min, pair.Value.Min),
              Max = Math.Max(value.Max, pair.Value.Max),
              Sum = value.Sum + pair.Value.Sum,
              Count = value.Count + pair.Value.Count
            };
          else
            StationStats[pair.Key] = pair.Value;
      }
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
    if (chunkStartPosition + chunkSizeInBytes > _fileStream.Length) return _fileStream.Length;

    _fileStream.Seek(chunkStartPosition + chunkSizeInBytes, SeekOrigin.Begin);
    while (_fileStream.ReadByte() != '\n' && _fileStream.Position < _fileStream.Length) ;

    return _fileStream.Position;
  }

  private void ReadChunk(Chunk chunk, Dictionary<string, Stats> stationStats) {
    var bufferSize = Math.Min(BufferSize, (int)chunk.Size);
    var buffer = _bytePool.Rent(bufferSize);
    var offset = 0;
    var remainingBytes = Memory<byte>.Empty;

    try {
      while (offset < chunk.Size) {
        using var viewStream = _mmf.CreateViewStream(
          access: MemoryMappedFileAccess.Read,
          offset: chunk.Position + offset,
          size: Math.Min(bufferSize, chunk.Size - offset));

        var bytesRead = viewStream.Read(buffer, 0, bufferSize);
        var memory = new Memory<byte>(buffer, 0, bytesRead);

        var totalBuffer = new byte[remainingBytes.Length + memory.Length];
        remainingBytes.CopyTo(totalBuffer);
        memory.CopyTo(totalBuffer.AsMemory(remainingBytes.Length));

        var lastNewline = Array.LastIndexOf(totalBuffer, (byte)'\n');
        if (lastNewline != totalBuffer.Length - 1) {
          remainingBytes = totalBuffer.AsMemory(lastNewline + 1);
          totalBuffer = totalBuffer.AsSpan(0, lastNewline + 1).ToArray();
        }
        else {
          remainingBytes = Memory<byte>.Empty;
        }

        ProcessBuffer(totalBuffer, stationStats);
        offset += bytesRead;
      }
    }
    finally {
      _bytePool.Return(buffer);
    }
  }

  private void ProcessBuffer(Span<byte> buffer, Dictionary<string, Stats> stationStats) {
    var bufferStart = 0;
    for (var i = 0; i < buffer.Length; i++)
      if (buffer[i] == (byte)'\n') {
        var lineBuffer = buffer.Slice(bufferStart, i - bufferStart);
        if (lineBuffer.Length > 0) ProcessBufferLine(lineBuffer, stationStats);
        bufferStart = i + 1;
      }
  }

  private void ProcessBufferLine(Span<byte> line, Dictionary<string, Stats> stationStats) {
    var semicolonIndex = line.IndexOf((byte)';');
    var stationName = Encoding.UTF8.GetString(line.Slice(0, semicolonIndex));
    var value = double.Parse(Encoding.UTF8.GetString(line.Slice(semicolonIndex + 1)));

    if (stationStats.ContainsKey(stationName)) {
      var oldStats = stationStats[stationName];
      stationStats[stationName] = new Stats {
        Min = Math.Min(oldStats.Min, value),
        Max = Math.Max(oldStats.Max, value),
        Sum = oldStats.Sum + value,
        Count = oldStats.Count + 1
      };
    }
    else {
      stationStats[stationName] = new Stats {
        Min = value,
        Max = value,
        Sum = value,
        Count = 1
      };
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