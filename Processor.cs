using System.Text;
using System.IO.MemoryMappedFiles;
using System.Buffers;
using System.Collections.Concurrent;

class Processor
{
    private const int BufferSize = 4096;
    private readonly FileStream _fileStream;
    private readonly int _processorCount;
    private readonly MemoryMappedFile _mmf;
    private readonly ArrayPool<char> charPool = ArrayPool<char>.Shared;
    private readonly ArrayPool<byte> bytePool = ArrayPool<byte>.Shared;

    public Processor(FileStream fileStream, int processorCount = 1)
    {
        _fileStream = fileStream;

        _processorCount = processorCount;

        _mmf = MemoryMappedFile.CreateFromFile(
            fileHandle: _fileStream.SafeFileHandle,
            access: MemoryMappedFileAccess.Read,
            inheritability: HandleInheritability.None,
            mapName: null,
            leaveOpen: true,
            capacity: 0);
    }

    public class Chunk(long position, long length)
    {
        public long Position { get; private set; } = position;
        public long Size { get; private set; } = length;
    }

    public Dictionary<string, Stats> stationStats = [];

    public struct Stats
    {
        public double Min;
        public double Max;
        public double Sum;
        public long Count;
    }

    public Chunk[] DivideFileIntoChunks()
    {
        var chunks = new Chunk[_processorCount];
        var chunkSizeInBytes = _fileStream.Length / _processorCount;

        long chunkStartPosition = 0;
        for (var chunkIndex = 0; chunkIndex < chunks.Length; chunkIndex++)
        {
            long chunkEndPosition;
            if (chunkStartPosition + chunkSizeInBytes <= _fileStream.Length)
            {
                chunkEndPosition = chunkStartPosition + chunkSizeInBytes;

                _fileStream.Seek(chunkEndPosition, SeekOrigin.Begin);
                var _ = 0; while ((_ = _fileStream.ReadByte()) != '\n' && _ >= 0) ;

                chunkEndPosition = _fileStream.Position;
            }
            else
            {
                chunkEndPosition = _fileStream.Length;
            }

            var chunkLengthInBytes = chunkEndPosition - chunkStartPosition;
            chunks[chunkIndex] = new Chunk(chunkStartPosition, chunkLengthInBytes);
            chunkStartPosition = chunkEndPosition;
        }

        return chunks.Where(chunk => chunk.Size > 0).ToArray();
    }


    public void ReadChunk(Chunk chunk, Dictionary<string, Stats> stationStats)
    {
        var buffer = bytePool.Rent(BufferSize);

        try
        {
            using var viewStream = _mmf.CreateViewStream(
                access: MemoryMappedFileAccess.Read,
                offset: chunk.Position,
                size: chunk.Size);

            var bytesRead = 0;
            while (bytesRead < chunk.Size)
            {
                var bufferIndex = 0;
                var lastNewlineIndex = -1;

                // Read a full page at a time
                var bytesToRead = Math.Min(BufferSize, chunk.Size - bytesRead);
                viewStream.Read(buffer, 0, (int)bytesToRead);
                bytesRead += (int)bytesToRead;

                // Process each byte in the buffer
                for (bufferIndex = 0; bufferIndex < bytesToRead; bufferIndex++)
                {
                    var byteRead = buffer[bufferIndex];

                    // If the byte is '\n', remember the position
                    if (byteRead == '\n')
                    {
                        lastNewlineIndex = bufferIndex;

                        if (lastNewlineIndex > 0)
                        {
                            var bufferSpan = new Span<byte>(buffer, 0, lastNewlineIndex);
                            var lastNewLineIdx = bufferSpan.LastIndexOf((byte)'\n') + 1;
                            var lastLine = bufferSpan.Slice(lastNewLineIdx);
                            ProcessBufferLine(lastLine, stationStats);
                        }
                    }
                }

                // If the end of the segment doesn't end with a '\n', process the remaining bytes
                if (lastNewlineIndex != bufferIndex - 1)
                {
                    var remainingBytes = new Span<byte>(buffer, lastNewlineIndex + 1, bufferIndex - lastNewlineIndex - 1);
                    ProcessBufferLine(remainingBytes, stationStats);
                }
            }
        }
        finally
        {
            bytePool.Return(buffer);
        }
    }


    public void ProcessData(IEnumerable<Chunk> chunks)
    {
        var partitioner = Partitioner.Create(chunks);

        Dictionary<string, Stats> localInit()
        {
            return new Dictionary<string, Stats>();
        }

        Dictionary<string, Stats> body(Dictionary<string, Stats> localDict, Chunk chunk, ParallelLoopState loopState)
        {
            ReadChunk(chunk, localDict);
            return localDict;
        }

        void localFinally(Dictionary<string, Stats> localDict)
        {
            lock (stationStats)
            {
                foreach (var pair in localDict)
                {
                    if (stationStats.TryGetValue(pair.Key, out Stats value))
                    {
                        var oldStats = value;
                        stationStats[pair.Key] = new Stats
                        {
                            Min = Math.Min(oldStats.Min, pair.Value.Min),
                            Max = Math.Max(oldStats.Max, pair.Value.Max),
                            Sum = oldStats.Sum + pair.Value.Sum,
                            Count = oldStats.Count + pair.Value.Count
                        };
                    }
                    else
                    {
                        stationStats[pair.Key] = pair.Value;
                    }
                }
            }
        }

        Parallel.ForEach(source: partitioner,
                         localInit: localInit,
                         body: (chunk, loopState, loopIndex) => body(localInit(), chunk, loopState),
                         localFinally: localFinally);
    }

    public void ProcessBufferLine(Span<byte> buffer, Dictionary<string, Stats> stationStats)
    {
        char[] chars = charPool.Rent(buffer.Length);
        try
        {
            Encoding.UTF8.GetChars(buffer, chars);

            var line = new string(chars, 0, buffer.Length);
            var parts = line.Split(';');

            var stationName = parts[0];
            var value = double.Parse(parts[1]);

            if (stationStats.ContainsKey(stationName))
            {
                var oldStats = stationStats[stationName];
                stationStats[stationName] = new Stats
                {
                    Min = Math.Min(oldStats.Min, value),
                    Max = Math.Max(oldStats.Max, value),
                    Sum = oldStats.Sum + value,
                    Count = oldStats.Count + 1
                };
            }
            else
            {
                stationStats[stationName] = new Stats { Min = value, Max = value, Sum = value, Count = 1 };
            }
        }
        finally
        {
            charPool.Return(chars);
        }
    }
}
