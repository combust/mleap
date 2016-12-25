# MLeap

[![Join the chat at https://gitter.im/combust-ml/mleap](https://badges.gitter.im/combust-ml/mleap.svg)](https://gitter.im/combust-ml/mleap?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/combust-ml/mleap.svg?branch=master)](https://travis-ci.org/combust-ml/mleap)

Deploying machine learning pipelines should not be a time-consuming or difficult task. MLeap allows data scientists and engineers to deploy machine learning pipelines from Spark and Scikit-learn to a portable format and execution engine. Most of the time this process takes many developer hours and may not result in performant systems, we are here to change that.

Using the MLeap execution engine and serialization format, we provide a performant, portable and easy-to-integrate production library for machine learning pipelines.

For portability, we build our software on the JVM and only use serialization formats that are widely-adopted.

We also provide a high level of integration with existing technologies. Instead of trying to rebuild the wheel, we are adding the body and engine.

## Overview

1. implemented in Scala
2. full [Spark](http://spark.apache.org/) and new [Scikit-learn](http://scikit-learn.org/stable/) support
3. supports 3 portable serialization formats (JSON, Protobuf, and Mixed)
4. export a model with Scikit-learn or Spark and execute it on the MLeap engine anywhere in the JVM
5. implement custom data types and transformers for use with MLeap data frames and transformer pipelines
6. extensive test coverage with full parity tests for Spark and MLeap pipelines
7. optional Spark transformer extension to extend Spark's default transformer offerings
8. serialize MLeap data frames to multiple formats, including JSON, Avro and a binary format

## Setup

### Link with Maven or SBT

MLeap is cross-compiled for Scala 2.10 and 2.11, so just replace 2.10 with 2.11 wherever you see it if you are running Scala version 2.11 and using a POM file for dependency management. Otherwise, use the `%%` operator if you are using SBT and the correct Scala version will be used.

#### SBT

```sbt
libraryDependencies += "ml.combust.mleap" %% "mleap-runtime" % "0.5.0"
```

#### Maven

```pom
<dependency>
    <groupId>ml.combust.mleap</groupId>
    <artifactId>mleap-runtime_2.10</artifactId>
    <version>0.5.0</version>
</dependency>
```

### For Spark Integration

#### SBT

```sbt
libraryDependencies += "ml.combust.mleap" %% "mleap-spark" % "0.5.0"
```

#### Maven

```pom
<dependency>
    <groupId>ml.combust.mleap</groupId>
    <artifactId>mleap-spark_2.10</artifactId>
    <version>0.5.0</version>
</dependency>
```

### Spark Packages

```bash
$ bin/spark-shell --packages ml.combust.mleap:mleap-spark_2.11:0.5.0
```

## Using the Library

For more complete examples, see our other Git repository: [MLeap Demos](https://github.com/combust-ml/mleap-demo)

### Create and Export a Spark Pipeline

The first step is to create our pipeline in Spark. Normally we would use real data to train a complete pipeline, but for our example we will manually build a simple Spark ML pipeline.

```scala
import org.apache.spark.ml.{StringIndexerModel, Binarizer}

// User out-of-the-box Spark transformers like you normally would
val si = new StringIndexerModel(uid = "si", labels = Array("hello", "MLeap")).
  setInputCol("test_string").
  setOutputCol("test_index")
val bin = new Binarizer(uid = "bin").
  setThreshold(0.34).
  setInputCol("test_double").
  setOutputCol("test_bin")
  
// Use the MLeap utility method to directly create an org.apache.spark.ml.PipelineModel

import org.apache.spark.ml.mleap.SparkUtil

// Without needing to fit an org.apache.spark.ml.Pipeline
val pipeline = SparkUtil.createPipelineModel(uid = "pipeline", Array(si, bin))

import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._

val modelFile = BundleFile("/tmp/simple-pipeline.zip")
pipeline.write.
  // delete the file if it already exists
  overwrite.
  // name our pipeline
  name("simple-pipeline").
  // save our pipeline to a zip file
  // we can save a file to any supported java.nio.FileSystem
  save(dest)
  
modelFile.close()
```

### Import and Transform Using Spark Pipeline

Spark pipelines are not meant to be run outside of Spark. They require a DataFrame and therefore a SparkContext to run as well. These are very expensive data structures and libraries to include in a project. With MLeap, there is no dependency on Spark to execute a pipeline. MLeap dependencies are very lightweight and we use very fast data structures for executing your ML pipelines.

```scala
import ml.combust.bundle.BundleFile
import ml.combust.mleap.MleapSupport._

// load the Spark pipeline we saved in the previous section
val bundle = BundleFile("/tmp/simple-pipeline.zip").load().get

// create a simple LeapFrame to transform
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset}
import ml.combust.mleap.runtime.types._

// MLeap makes extensive use of monadic types like Try
val schema = StructType(StructField("test_string", StringType),
  StructField("")).get
val data = LocalDataset(Row("hello", 0.6),
  Row("MLeap", 0.2))
val frame = LeapFrame.create(schema, data)

// transform the dataframe using our pipeline
val mleapPipeline = bundle.root
val frame2 = mleapPipeline.transform(frame).get
val data = frame2.dataset

// get data from the transformed rows and make some assertions
assert(data(0).getDouble(2) == 0.0) // string indexer output
assert(data(0).getDouble(3) == 1.0) // binarizer output

// the second row
assert(data(1).getDouble(2) == 1.0)
assert(data(1).getDouble(3) == 0.0)
```

### Create and Export a Scikit-learn Pipeline

TODO: Mikhail

### Import and Transform Using Scikit-learn Pipeline

TODO: Mikhail

## Documentation

TODO: add links to the wiki here

For more documentation, please see our github wiki, where you can learn to:

1. implement custom transformers that will work with Spark, MLeap and Scikit-learn
2. implement custom data types to transform with Spark and MLeap pipelines
3. transform with blazing fast speeds using optimized row-based transformers
4. serialize MLeap data frames to various formats like avro, json, and a custom binary format
5. implement new serialization formats for MLeap data frames
6. work through several demonstration pipelines which use real-world data to create predictive pipelines

## Contributing

* Write documentation. As you can see looking through the source code, there is very little.
* Contribute an Estimator/Transformer from Spark.
* Use MLeap at your company and tell us what you think.
* Make a feature request or report a bug in github.
* Make a pull request for an existing feature request or bug report.
* Join the discussion of how to get MLeap into Spark as a dependency. Talk with us on Gitter (see link at top of README.md).

## Contact Information

* Hollin Wilkins (hollin@combust.ml)
* Mikhail Semeniuk (mikhail@combust.ml)
* Ram Sriharsha (ram@databricks.com)

## License

See LICENSE and NOTICE file in this repository.

Copyright 2016 Combust, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
