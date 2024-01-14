using System.Diagnostics;

namespace _1brc;

public static class Program {
  public static void Main(string[] args) {
    var filePath = args[0];
    var fileStream = new FileStream(
      filePath,
      FileMode.Open,
      FileAccess.Read,
      FileShare.ReadWrite,
      options: FileOptions.RandomAccess,
      bufferSize: 0);

    var processor = new Processor(
      fileStream,
      Environment.ProcessorCount);

    var chunks = processor.Chunks();

    Debug.Assert(fileStream.Length == chunks.Sum(chunk => chunk.Size),
                 "File length does not match sum of chunk lengths.");

    processor.ProcessChunks(chunks);
    foreach (var pair in processor.StationStats.OrderBy(a => a.Key)) {
      var mean = pair.Value.Sum / pair.Value.Count;
      Console.WriteLine($"Station: {pair.Key}, Min: {pair.Value.Min}, Max: {pair.Value.Max}, Mean: {mean}");
    }
  }
}