using System.Diagnostics;

namespace _1brc;

public static class Program {
  public static void Main(string[] args) {
    string filePath = args[0];
    FileStream fileStream = new(
      filePath,
      FileMode.Open,
      FileAccess.Read,
      FileShare.ReadWrite,
      options: FileOptions.RandomAccess,
      bufferSize: 0);

    Processor processor = new(
      fileStream,
      Environment.ProcessorCount);

    Processor.Chunk[] chunks = processor.Chunks();

    Debug.Assert(fileStream.Length == chunks.Sum(chunk => chunk.Size),
                 "File length does not match sum of chunk lengths.");

    processor.ProcessChunks(chunks);
    foreach (KeyValuePair<string, Processor.Stats> pair in processor.StationStats.OrderBy(a => a.Key)) {
      double mean = pair.Value.Sum / pair.Value.Count;
      Console.WriteLine($"Station: {pair.Key}, Min: {pair.Value.Min}, Max: {pair.Value.Max}, Mean: {mean}");
    }
  }
}