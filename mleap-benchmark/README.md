# MLeap Benchmarks

This module is meant to make benchmarking MLeap easy. We include two
pipelines for benchmarking, that you can create yourself by following
along in our [AirBnB price prediction demo](https://github.com/combust/mleap-demo/tree/master/notebooks).

What we are trying to determine is the raw execution speed of MLeap on a
leap frame. In order to do this, we execute the MLeap transformer
pipeline thousands of times over a single leap frame. This is meant to
simulate an API server that is taking in many requests and servicing
them in real time.

## Executing the Benchmarks

It is easy to run the benchmarks that come with MLeap or to run
benchmarks against your own models. We offer three different benchmarks:

1. Default MLeap transformer - this is the out-of-the box, simplest way
   to transform data with MLeap
2. Row-based MLeap transformer - this is that fastest way to execute
   your pipelines, but restricts the format of incoming rows to a
predetermined schema
3. Spark transformer - for comparison, we show how Spark would perform
   with a LocalRelation-backed DataFrame and Spark transformers

For each test-type we will provide our results when running on:
Mac OS X 2.5 GHz Intel Core i7

java version "1.8.0\_65"
Java(TM) SE Runtime Environment (build 1.8.0\_65-b17)
Java HotSpot(TM) 64-Bit Server VM (build 25.65-b01, mixed mode)

### Default MLeap Transformer

Use SBT to run the benchmarks, we fork separate JVM processes so it
does not interfere with test results.

```scala
// For random forest pipeline
sbt "mleap-benchmark/run mleap-transform --model-path ./mleap-benchmark/src/main/resources/models/airbnb.model.rf.zip --frame-path ./mleap-benchmark/src/main/resources/leap_frame/frame.airbnb.json"

// For linear regression pipeline
sbt "mleap-benchmark/run mleap-transform --model-path ./mleap-benchmark/src/main/resources/models/airbnb.model.lr.zip --frame-path ./mleap-benchmark/src/main/resources/leap_frame/frame.airbnb.json"
```

#### Results

Random Forest: ~24.6 microseconds (246/10000)

```
Parameters(size -> 1000): 23.990439
Parameters(size -> 2000): 47.991208
Parameters(size -> 3000): 72.809849
Parameters(size -> 4000): 96.966391
Parameters(size -> 5000): 121.449282
Parameters(size -> 6000): 146.344077
Parameters(size -> 7000): 170.942028
Parameters(size -> 8000): 195.383286
Parameters(size -> 9000): 222.241228
Parameters(size -> 10000): 246.348788
```

Linear Regression: ~23.7 microseconds (237/10000)

```
Parameters(size -> 1000): 23.348875
Parameters(size -> 2000): 46.789762
Parameters(size -> 3000): 70.088292
Parameters(size -> 4000): 92.938216
Parameters(size -> 5000): 116.56363
Parameters(size -> 6000): 141.166164
Parameters(size -> 7000): 162.03061
Parameters(size -> 8000): 187.489697
Parameters(size -> 9000): 208.484832
Parameters(size -> 10000): 237.864215
```

### Spark Transformer

For Spark tests, we have to run a much lower volume of transformers
because of how long it takes to execute one-off transforms. Note that
transform speed in this case is dictated more by how long it takes Spark
to optimize your pipelines, rather than how long it takes to execute it.

```scala
// For random forest pipeline
sbt "mleap-benchmark/run spark-transform --model-path ./mleap-benchmark/src/main/resources/models/airbnb.model.rf.zip --frame-path ./mleap-benchmark/src/main/resources/leap_frame/frame.airbnb.avro"

// For linear regression pipeline
sbt "mleap-benchmark/run spark-transform --model-path ./mleap-benchmark/src/main/resources/models/airbnb.model.lr.zip --frame-path ./mleap-benchmark/src/main/resources/leap_frame/frame.airbnb.avro"
```

#### Results

Random Forest: ~101 milliseconds (10100/100)

```
Parameters(size -> 10): 3003.134541
Parameters(size -> 20): 2667.17666
Parameters(size -> 30): 3625.830997
Parameters(size -> 40): 4670.878447
Parameters(size -> 50): 5595.42647
Parameters(size -> 60): 6363.986371
Parameters(size -> 70): 7157.167454
Parameters(size -> 80): 8142.149347
Parameters(size -> 90): 9035.291511
Parameters(size -> 100): 10104.72628
```

Linear Regression: ~ 116 milliseconds (11600/100)

```
Parameters(size -> 10): 2889.342366
Parameters(size -> 20): 2728.041945
Parameters(size -> 30): 4160.168527
Parameters(size -> 40): 4985.649625
Parameters(size -> 50): 5566.344499
Parameters(size -> 60): 6595.621628
Parameters(size -> 70): 7965.271514
Parameters(size -> 80): 8762.643043
Parameters(size -> 90): 10896.841689
Parameters(size -> 100): 11646.638617
```

### MLeap Row-Transformer

MLeap row transformers take a lot of overhead out of transforming data
by predefining the input and output schema of the rows you are
transforming. This is the fastest execution mode MLeap provides, and it
roughly 4x faster than the default transorms for our example pipelines.

```scala
// For random forest pipeline
sbt "mleap-benchmark/run mleap-row --model-path ./mleap-benchmark/src/main/resources/models/airbnb.model.rf.zip --frame-path ./mleap-benchmark/src/main/resources/leap_frame/frame.airbnb.json"

// For linear regression pipeline
sbt "mleap-benchmark/run mleap-row --model-path ./mleap-benchmark/src/main/resources/models/airbnb.model.lr.zip --frame-path ./mleap-benchmark/src/main/resources/leap_frame/frame.airbnb.json"
```

#### Results

Random Forest: ~6.8 microseconds (68/10000)

```
Parameters(size -> 1000): 6.956204
Parameters(size -> 2000): 13.717578
Parameters(size -> 3000): 20.424697
Parameters(size -> 4000): 27.160537
Parameters(size -> 5000): 34.025757
Parameters(size -> 6000): 41.017156
Parameters(size -> 7000): 48.140102
Parameters(size -> 8000): 54.724859
Parameters(size -> 9000): 61.769202
Parameters(size -> 10000): 68.646654
```

Linear Regression: ~6.2 microseconds (62/10000)

```
Parameters(size -> 1000): 6.535936
Parameters(size -> 2000): 12.432958
Parameters(size -> 3000): 18.78803
Parameters(size -> 4000): 25.055208
Parameters(size -> 5000): 31.42303
Parameters(size -> 6000): 37.868067
Parameters(size -> 7000): 44.034418
Parameters(size -> 8000): 51.660834
Parameters(size -> 9000): 56.598257
Parameters(size -> 10000): 62.713341
```

