using System.Diagnostics;

class Program
{
    private const string FilePath = "./measurements.txt";

    public static void Main(string[] args)
    {
        var fileStream = new FileStream(
            path: FilePath,
            mode: FileMode.Open,
            access: FileAccess.Read,
            share: FileShare.ReadWrite,
            options: FileOptions.RandomAccess,
            bufferSize: 4096);

        var processor = new Processor(
            fileStream: fileStream,
            processorCount: Environment.ProcessorCount);

        var chunks = processor.DivideFileIntoChunks();
        Debug.Assert(fileStream.Length == chunks.Sum(chunk => chunk.Size), "File length does not match sum of chunk lengths.");

        processor.ProcessData(chunks);

        foreach (var pair in processor.stationStats)
        {
            var mean = pair.Value.Sum / pair.Value.Count;
            Console.WriteLine($"Station: {pair.Key}, Min: {pair.Value.Min}, Max: {pair.Value.Max}, Mean: {mean}");
        }
    }
}
