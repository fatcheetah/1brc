using System.Text;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Diagnostics;
using System.Buffers;
using System.Collections.Concurrent;

/// <summary>
/// Represents the main entry point of the program.
/// </summary>
class Program
{
    private const string FILEPATH = "./measurements.txt";

    /// <summary>
    /// The main method that is executed when the program starts.
    /// </summary>
    /// <param name="args">The command-line arguments.</param>
    public static void Main(string[] args)
    {
        var fileStream = new FileStream(
            path: FILEPATH,
            mode: FileMode.Open,
            access: FileAccess.Read,
            share: FileShare.ReadWrite,
            options: FileOptions.RandomAccess,
            bufferSize: 1
        );

        var processor = new Processor(
            fileStream: fileStream,
            processorCount: Environment.ProcessorCount
        );

        var chunks = processor.DivideFileIntoChunks().Dump();

        Debug.Assert(fileStream.Length == chunks.Sum(chunk => chunk.Size), "File length does not match sum of chunk lengths.");

        // processor.ReadChunk(chunks[0]);
        Parallel.ForEach(chunks, chunk => processor.ReadChunk(chunk));
        foreach (var stationName in processor.stationNames)
        {
            Console.WriteLine($"{stationName.Key}");
        }
    }
}

/// <summary>
/// Represents a processor that reads chunks of a file.
/// </summary>
class Processor(FileStream fileStream, int processorCount = 1)
{
    private readonly FileStream _fileStream = fileStream;
    private readonly int _processorCount = processorCount;

    /// <summary>
    /// Represents a chunk of a file.
    /// </summary>
    public class Chunk(long position, long length)
    {
        /// <summary>
        /// Gets the starting position of the chunk in the file.
        /// </summary>
        public long Position { get; private set; } = position;

        /// <summary>
        /// Gets the length of the chunk in bytes.
        /// </summary>
        public long Size { get; private set; } = length;
    }

    /// <summary>
    /// Divides the file into chunks based on the number of processors.
    /// </summary>
    /// <returns>An array of chunks representing different portions of the file.</returns>
    public Chunk[] DivideFileIntoChunks()
    {
        var chunks = new Chunk[_processorCount];
        var chunkSizeInBytes = _fileStream.Length / _processorCount;

        long chunkStartPosition = 0;
        for (var chunkIndex = 0; chunkIndex < chunks.Length; chunkIndex++)
        {
            if (chunkStartPosition + chunkSizeInBytes <= _fileStream.Length)
            {
                var chunkEndPosition = chunkStartPosition + chunkSizeInBytes;
                _fileStream.Seek(chunkEndPosition, SeekOrigin.Begin);

                var _ = 0; while ((_ = _fileStream.ReadByte()) != '\n' && _ >= 0) ;

                chunkEndPosition = _fileStream.Position;
                var chunkLengthInBytes = chunkEndPosition - chunkStartPosition;

                chunks[chunkIndex] = new Chunk(chunkStartPosition, chunkLengthInBytes);
                chunkStartPosition = chunkEndPosition;
            }
            else
            {
                chunks[chunkIndex] = new Chunk(chunkStartPosition, _fileStream.Length - chunkStartPosition);
                break;
            }
        }

        _fileStream.Seek(0, SeekOrigin.Begin);
        return chunks;
    }

    /// <summary>
    /// Reading files in segments of the system's page size can be beneficial for performance reasons.  The page size is the unit of data that the operating system's memory manager uses to transfer data between the disk and the system's main memory (RAM). When you read a file, the operating system loads it into memory one page at a time.  If you read a file in chunks that align with the system's page size, you can reduce the number of disk operations and make better use of the system's memory cache. This is because each read operation will fill exactly one page of memory, without any wasted space.  Moreover, many file systems and storage devices are optimized for reading data in multiples of the page size. Reading data in smaller chunks can result in additional overhead, as the system needs to perform more read operations and manage more memory pages.  However, it's important to note that the benefits of reading files in page-sized chunks can depend on the specific characteristics of your system and workload. In some cases, other factors may be more important for performance, such as the layout of the data on disk or the pattern of read and write operations.
    /// </summary>
    /// <param name="chunk">The chunk to read.</param>
    public void ReadChunk(Chunk chunk)
    {
        var bufferSize = 4096;
        var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);

        try
        {
            using var mmf = MemoryMappedFile.CreateFromFile(
                fileHandle: _fileStream.SafeFileHandle,
                access: MemoryMappedFileAccess.Read,
                inheritability: HandleInheritability.None,
                mapName: null,
                leaveOpen: true,
                capacity: 0
            );

            using var viewStream = mmf.CreateViewStream(
                access: MemoryMappedFileAccess.Read,
                offset: chunk.Position,
                size: chunk.Size
            );

            var bytesRead = 0;
            while (bytesRead < chunk.Size)
            {
                var bufferIndex = 0;
                var lastNewlineIndex = -1;

                // Read a full page at a time
                var bytesToRead = Math.Min(bufferSize, chunk.Size - bytesRead);
                viewStream.Position = bytesRead;
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
                            ProcessBufferLine(new Span<byte>(buffer, 0, lastNewlineIndex));
                        }
                    }
                }

                // If the end of the segment doesn't end with a '\n', move the file stream back
                if (lastNewlineIndex != bufferIndex - 1)
                {
                    var adjustment = bufferIndex - lastNewlineIndex;
                    bytesRead -= adjustment;
                    viewStream.Position -= adjustment;
                    bufferIndex = lastNewlineIndex;
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }


    public ConcurrentDictionary<string, byte> stationNames = new();

    public int i = 0;
    public void ProcessBufferLine(Span<byte> buffer)
    {
        var lastNewlineIndex = buffer.LastIndexOf((byte)'\n') + 1;
        var lastLine = buffer.Slice(lastNewlineIndex);
        var firstSemicolonIndex = lastLine.IndexOf((byte)';');
        var stationNameBytes = lastLine.Slice(0, firstSemicolonIndex);

        stationNameBytes.ToArray().Dump();


        // increment the counter
        Interlocked.Increment(ref i);

    }
}