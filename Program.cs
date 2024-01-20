using System.Diagnostics;
using System.Text;

namespace _1brc;

public static class Program {
  public static void Main(string[] args) {
    string filePath = args[0];
    FileStream fileStream = new(
      filePath,
      FileMode.Open,
      FileAccess.Read,
      FileShare.ReadWrite,
      options: FileOptions.SequentialScan,
      bufferSize: 4096);

    Processor processor = new(fileStream);
    Processor.Chunk[] chunks = processor.Chunks();

    Debug.Assert(fileStream.Length == chunks.Sum(chunk => chunk.Size),
                 "File length does not match sum of chunk lengths.");

    processor.ProcessChunks(chunks);
    
    StringBuilder outputJson = new();
    outputJson.Append("{");
    foreach (KeyValuePair<Processor.ByteArray, Processor.Stats> pair in processor.StationStats.OrderBy(a => a.Key)) {
      double mean = pair.Value.Sum / pair.Value.Count;
      outputJson.Append($"{pair.Key.GetString()}={pair.Value.Min:F1}/{mean:F1}/{pair.Value.Max:F1}, ");
    }
    outputJson.Length -= 2;
    outputJson.Append("}");
    Console.WriteLine(outputJson.ToString());
  }
}
