using System.Buffers;
using System.Diagnostics;
using System.Text;

public static class ExtensionMethods {
  /// <summary>
  ///   Dumps the specified object to the console, displaying its contents.
  /// </summary>
  /// <typeparam name="T">The type of the object to dump this is inferred.</typeparam>
  /// <param name="obj">The object to dump.</param>
  /// <param name="depth">The depth of indent used in the cli output.</param>
  /// <returns>The dumped object.</returns>
  public static T Dump<T>(this T obj, int depth = 0, int MAX_DEPTH = 5) {
    if (obj == null) {
      "null".Dump(depth);
      return obj;
    }

    if (obj.GetType() == typeof(byte[])) {
      var bytes = (byte[])(object)obj;
      var output = new StringBuilder(bytes.Length * 4);

      var arrayPool = ArrayPool<char>.Shared;
      var asciiLine = arrayPool.Rent(16);

      for (var i = 0; i < bytes.Length; i += 16) {
        var lineLength = Math.Min(16, bytes.Length - i);
        for (var byteIndex = 0; byteIndex < 16; byteIndex++)
          if (byteIndex < lineLength) {
            var currentByte = bytes[i + byteIndex];
            output.Append($"{currentByte:X2} ");
            asciiLine[byteIndex] = currentByte >= 32 && currentByte <= 126
              ? (char)currentByte
              : '\u00b7';
          }
          else {
            output.Append(' ', 3);
          }

        output.AppendFormat(" {0}{1}",
                            new string(asciiLine, 0, lineLength),
                            i + 16 < bytes.Length ? Environment.NewLine : "");
      }

      arrayPool.Return(asciiLine);
      Console.WriteLine(output.ToString());

      return obj;
    }

    if (obj is string) {
      Console.WriteLine(obj);
      return obj;
    }

    if (obj.GetType().IsPrimitive) {
      Console.WriteLine(obj);
      return obj;
    }

    if (obj is DateTime) {
      var date = (DateTime)(object)obj;
      Console.WriteLine(date.ToString("dd/MM/yyyy HH:mm"));
      return obj;
    }

    if (depth == MAX_DEPTH) {
      Console.WriteLine("...");
      return obj;
    }

    var properties = obj.GetType().GetProperties();
    var values = properties.Select(p => {
      var value = p.GetIndexParameters().Length == 0
        ? p.GetValue(obj, null)
        : p.GetValue(obj, new object[] { 0 });
      return (value, p.PropertyType, p.Name);
    }).ToArray();

    var longestName = values.Max(v => v.Name.Length);

    foreach (var (value, type, name) in values) {
      if (value == null) continue;

      var indentation = new StringBuilder().Insert(0, " ", depth * 2);

      if (value is IEnumerable<object> collection) {
        Console.WriteLine($"{indentation}\u001b[1m{type.Name}::{name}\u001b[0m");
        foreach (var item in collection) item.Dump(depth + 1);
      }
      else {
        Console.Write($"{indentation}\u001b[1m{name.PadRight(longestName)}\u001b[0m: ");
        var isComplexType = !type.IsPrimitive
                            && !type.IsEnum
                            && type != typeof(string)
                            && type != typeof(decimal);
        if (isComplexType) value.Dump(depth + 1);
        else value.Dump(depth + 1);
      }
    }
    return obj;
  }

  public static void IterTimer(Action action, int iterations = 100) {
    var sb = new StringBuilder();
    sb.AppendLine($"\u001b[1mRunning {iterations} iterations of: {action.Method.Name}\u001b[0m");

    var times = new long[iterations];
    for (var i = 0; i < iterations; i++) {
      var stopwatch = Stopwatch.StartNew();
      action();
      stopwatch.Stop();
      times[i] = stopwatch.ElapsedMilliseconds;
    }

    var maxTime = times.Max();
    for (var i = 0; i < iterations; i++) {
      var barLength = (int)(times[i] / (double)maxTime * 50);
      var bar = new string('#', barLength);
      sb.AppendLine($"Run {i + 1,3} : {bar}{times[i]}ms");
    }

    sb.AppendLine($"* Total : {times.Sum()}ms");
    Console.Write(sb.ToString());
  }
}