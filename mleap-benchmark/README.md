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

## Included Benchmarks

Our included benchmarks work off a pipeline that has several continuous and several categorical features. We run continuous features through a standard scaler and we run categorical features through string indexing and one hot encoding. We then combine the one hot vectors with the scaled continuous features and perform scoring with either a random forest or a linear regression.

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

| # of transforms | Total time (milliseconds) | Transform time (microseconds) |
|:---:|:---:|:---:|
| 1000 | 23.990439 | 24 |
| 2000 | 47.991208 | 24 |
| 3000 | 72.809849 | 24 |
| 4000 | 96.966391 | 24 |
| 5000 | 121.449282 | 24 |
| 6000 | 146.344077 | 24 |
| 7000 | 170.942028 | 24 |
| 8000 | 195.383286 | 24 |
| 9000 | 222.241228 | 25 |
| 10000 | 246.348788 | 25 |


Linear Regression: ~23.7 microseconds (237/10000)

| # of transforms | Total time (milliseconds) | Transform time (microseconds) |
|:---:|:---:|:---:|
| 1000 | 23.348875 | 23 |
| 2000 | 46.789762 | 23 |
| 3000 | 70.088292 | 23 |
| 4000 | 92.938216 | 23 |
| 5000 | 116.56363 | 23 |
| 6000 | 141.166164 | 23 |
| 7000 | 162.03061 | 23 |
| 8000 | 187.489697 | 23 |
| 9000 | 208.484832 | 23 |
| 10000 | 237.864215 | 24 |

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

| # of transforms | Total time (milliseconds) | Transform time (microseconds) |
|:---:|:---:|:---:|
| 1000 | 6.956204 | 7 |
| 2000 | 13.717578 | 7 |
| 3000 | 20.424697 | 7 |
| 4000 | 27.160537 | 7 |
| 5000 | 34.025757 | 7 |
| 6000 | 41.017156 | 7 |
| 7000 | 48.140102 | 7 |
| 8000 | 54.724859 | 7 |
| 9000 | 61.769202 | 7 |
| 10000 | 68.646654 | 7 |

Linear Regression: ~6.2 microseconds (62/10000)

| # of transforms | Total time (milliseconds) | Transform time (microseconds) |
|:---:|:---:|:---:|
| 1000 | 6.535936 | 7 |
| 2000 | 12.432958 | 6 |
| 3000 | 18.78803 | 6 |
| 4000 | 25.055208 | 6 |
| 5000 | 31.42303 | 6 |
| 6000 | 37.868067 | 6 |
| 7000 | 44.034418 | 6 |
| 8000 | 51.660834 | 6 |
| 9000 | 56.598257 | 6 |
| 10000 | 62.713341 | 6 |

### Spark Transformer

For Spark tests, we have to run a much lower volume of transformers
because of how long it takes to execute one-off transforms. Note that
transform speed in this case is dictated more by how long it takes Spark
to optimize your pipelines, rather than how long it takes to execute it.

NOTE: We do not fork Spark benchmarks, as it causes issues with running
Spark. This minimally affects transform time from what we have seen by
about 10 microseconds per transform.

```scala
// For random forest pipeline
sbt "mleap-benchmark/run spark-transform --model-path ./mleap-benchmark/src/main/resources/models/airbnb.model.rf.zip --frame-path ./mleap-benchmark/src/main/resources/leap_frame/frame.airbnb.avro"

// For linear regression pipeline
sbt "mleap-benchmark/run spark-transform --model-path ./mleap-benchmark/src/main/resources/models/airbnb.model.lr.zip --frame-path ./mleap-benchmark/src/main/resources/leap_frame/frame.airbnb.avro"
```

#### Results

Random Forest: ~101 milliseconds (10100/100)

| # of transforms | Total time (milliseconds) | Transform time (milliseconds) |
|:---:|:---:|:---:|
| 10 | 3003.134541 | 300 |
| 20 | 2667.17666 | 133 |
| 30 | 3625.830997 | 120 |
| 40 | 4670.878447 | 116 |
| 50 | 5595.42647 | 111 |
| 60 | 6363.986371 | 106 |
| 70 | 7157.167454 | 102 |
| 80 | 8142.149347 | 102 |
| 90 | 9035.291511 | 100 |
| 100 | 10104.72628 | 101 |

Linear Regression: ~ 116 milliseconds (11600/100)

| # of transforms | Total time (milliseconds) | Transform time (milliseconds) |
|:---:|:---:|:---:|
| 10 | 2889.342366 | 289 |
| 20 | 2728.041945 | 136.4 |
| 30 | 4160.168527 | 139 |
| 40 | 4985.649625 | 125 |
| 50 | 5566.344499 | 111 |
| 60 | 6595.621628 | 110 |
| 70 | 7965.271514 | 114 |
| 80 | 8762.643043 | 110 |
| 90 | 10896.841689 | 121 |
| 100 | 11646.638617 | 116 |

