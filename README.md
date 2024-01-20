# 1brc

### what ? 
The One Billion Row Challenge (1BRC) is a fun exploration of how far modern [~~Java~~](https://github.com/gunnarmorling/1brc) C# can be pushed for aggregating one billion rows from a text file. Grab all your (virtual) threads, reach out to SIMD, optimize your GC, or pull any other trick, and create the fastest implementation for solving this task!

The task is to write a ~~Java~~ C# program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like this (i.e. sorted alphabetically by station name, and the result values per station in the format <min>/<mean>/<max>, rounded to one fractional digit):


### usage 
```
dotnet build -c release && time ./bin/release/net8.0/1brc <MEASUREMENTS_PATH>
```

### results

```md
# this
real    0m8.475s
user    1m56.857s
sys     0m1.876s

# https://github.com/buybackoff/1brc
real    0m1.679s
user    0m14.619s
sys     0m2.074s
```
