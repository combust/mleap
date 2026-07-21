<a href="https://combust.github.io/mleap-docs/"><img src="logo.png" alt="MLeap Logo" width="176" height="70" /></a>

[![Build Status](https://github.com/combust/mleap/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/combust/mleap/actions/workflows/test.yml)
[![Maven Central](https://img.shields.io/maven-central/v/ml.combust.mleap/mleap-base_2.13)](https://central.sonatype.com/artifact/ml.combust.mleap/mleap-base_2.13)
[![PyPI](https://img.shields.io/pypi/v/mleap)](https://pypi.org/project/mleap/)

Deploying machine learning data pipelines and algorithms should not be a time-consuming or difficult task. MLeap allows data scientists and engineers to deploy machine learning pipelines from Spark and Scikit-learn to a portable format and execution engine.

## Documentation

Documentation is available at [https://combust.github.io/mleap-docs/](https://combust.github.io/mleap-docs/).

Read [Serializing a Spark ML Pipeline and Scoring with MLeap](https://github.com/combust/mleap/wiki/Serializing-a-Spark-ML-Pipeline-and-Scoring-with-MLeap) to gain a full sense of what is possible.

## Introduction

MLeap provides a performant, portable execution engine and serialization format (Bundle.ML) for machine learning pipelines. Train a pipeline with Spark or Scikit-learn, export it once, and execute it anywhere on the JVM without a dependency on Spark, sklearn, numpy, or pandas.

Key features:

1. Core execution engine implemented in Scala, with [Spark](http://spark.apache.org/), PySpark, and Scikit-learn support.
2. Export a Spark or Scikit-learn pipeline and run it on the lightweight MLeap Runtime.
3. Two portable serialization formats: JSON and Protobuf.
4. Custom data types and transformers for MLeap data frames and pipelines.
5. Full parity tests between Spark and MLeap pipelines.
6. Optional Spark transformer extension that adds to Spark's built-in transformers.

## Dependency Compatibility Matrix

Other versions besides those listed below may also work (especially more recent Java versions for the JRE), 
but these are the configurations which are tested by mleap.

| MLeap Version | Spark Version | Scala Version    | Java Version | Python Version | XGBoost Version | Tensorflow Version |
|---------------|---------------|------------------|--------------|----------------|-----------------|--------------------|
| 0.25.2        | 4.1.2         | 2.13.18          | 17           | 3.10 - 3.13    | 2.0.3           | 2.16.2             |
| 0.25.1        | 4.1.1         | 2.13.17          | 17           | 3.10 - 3.13    | 2.0.3           | 2.16.2             |
| 0.25.0        | 4.1.0         | 2.13.17          | 17           | 3.10 - 3.13    | 2.0.3           | 2.10.1             |
| 0.24.2        | 4.0.3         | 2.13.16          | 17           | 3.9 - 3.13     | 2.0.3           | 2.10.1             |
| 0.24.1        | 4.0.2         | 2.13.16          | 17           | 3.9 - 3.13     | 2.0.3           | 2.10.1             |
| 0.24.0        | 4.0.1         | 2.13.16          | 17           | 3.9 - 3.13     | 2.0.3           | 2.10.1             |
| 0.23.4        | 3.4.4         | 2.12.18          | 11           | 3.7 - 3.12     | 1.7.6           | 2.10.1             |
| 0.23.3        | 3.4.0         | 2.12.18          | 11           | 3.7, 3.8       | 1.7.6           | 2.10.1             |
| 0.23.2        | 3.4.0         | 2.12.18          | 11           | 3.7, 3.8       | 1.7.6           | 2.10.1             |
| 0.23.1        | 3.4.0         | 2.12.18          | 11           | 3.7, 3.8       | 1.7.6           | 2.10.1             |
| 0.23.0        | 3.4.0         | 2.12.13          | 11           | 3.7, 3.8       | 1.7.3           | 2.10.1             |
| 0.22.0        | 3.3.0         | 2.12.13          | 11           | 3.7, 3.8       | 1.6.1           | 2.7.0              |
| 0.21.1        | 3.2.0         | 2.12.13          | 11           | 3.7            | 1.6.1           | 2.7.0              |
| 0.21.0        | 3.2.0         | 2.12.13          | 11           | 3.6, 3.7       | 1.6.1           | 2.7.0              |
| 0.20.0        | 3.2.0         | 2.12.13          | 8            | 3.6, 3.7       | 1.5.2           | 2.7.0              |
| 0.19.0        | 3.0.2         | 2.12.13          | 8            | 3.6, 3.7       | 1.3.1           | 2.4.1              |
| 0.18.1        | 3.0.2         | 2.12.13          | 8            | 3.6, 3.7       | 1.0.0           | 2.4.1              |
| 0.18.0        | 3.0.2         | 2.12.13          | 8            | 3.6, 3.7       | 1.0.0           | 2.4.1              |
| 0.17.0        | 2.4.5         | 2.11.12, 2.12.10 | 8            | 3.6, 3.7       | 1.0.0           | 1.11.0             |

## Setup

### Link with Maven or SBT

#### SBT

```sbt
libraryDependencies += "ml.combust.mleap" %% "mleap-runtime" % "0.25.2"
```

#### Maven

```pom
<dependency>
    <groupId>ml.combust.mleap</groupId>
    <artifactId>mleap-runtime_2.13</artifactId>
    <version>0.25.2</version>
</dependency>
```

### For Spark Integration

#### SBT

```sbt
libraryDependencies += "ml.combust.mleap" %% "mleap-spark" % "0.25.2"
```

#### Maven

```pom
<dependency>
    <groupId>ml.combust.mleap</groupId>
    <artifactId>mleap-spark_2.13</artifactId>
    <version>0.25.2</version>
</dependency>
```

### PySpark Integration

Install MLeap from [PyPI](https://pypi.org/project/mleap/)
```bash
$ pip install mleap
```

## Using the Library

For more complete examples, see our other Git repository: [MLeap Demos](https://github.com/combust/mleap-demo)

### Create and Export a Spark Pipeline

The first step is to create our pipeline in Spark. For our example we will manually build a simple Spark ML pipeline.


```scala
import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.{Binarizer, StringIndexer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.util.Using

  val datasetName = "./examples/spark-demo.csv"

  val dataframe: DataFrame = spark.sqlContext.read.format("csv")
    .option("header", true)
    .load(datasetName)
    .withColumn("test_double", col("test_double").cast("double"))

  // Use out-of-the-box Spark transformers like you normally would
  val stringIndexer = new StringIndexer().
    setInputCol("test_string").
    setOutputCol("test_index")

  val binarizer = new Binarizer().
    setThreshold(0.5).
    setInputCol("test_double").
    setOutputCol("test_bin")

  val pipelineEstimator = new Pipeline()
    .setStages(Array(stringIndexer, binarizer))

  val pipeline = pipelineEstimator.fit(dataframe)

  // then serialize pipeline
  val sbc = SparkBundleContext().withDataset(pipeline.transform(dataframe))
  Using(BundleFile("jar:file:/tmp/simple-spark-pipeline.zip")) { bf =>
    pipeline.writeBundle.save(bf)(sbc).get
  }
```

The dataset used for training can be found [here](https://github.com/combust/mleap/tree/master/examples/spark-demo.csv)

Spark pipelines are not meant to be run outside of Spark. They require a DataFrame and therefore a SparkContext to run. These are expensive data structures and libraries to include in a project. With MLeap, there is no dependency on Spark to execute a pipeline. MLeap dependencies are lightweight and we use fast data structures to execute your ML pipelines.

### PySpark Integration

Import the MLeap library in your PySpark job

```python
import mleap.pyspark
from mleap.pyspark.spark_support import SimpleSparkSerializer
```

See [PySpark Integration of python/README.md](python/README.md#pyspark-integration) for more.

### Create and Export a Scikit-Learn Pipeline

```python
import pandas as pd

from mleap.sklearn.pipeline import Pipeline
from mleap.sklearn.preprocessing.data import FeatureExtractor, LabelEncoder, ReshapeArrayToN1
from sklearn.preprocessing import OneHotEncoder

data = pd.DataFrame(['a', 'b', 'c'], columns=['col_a'])

categorical_features = ['col_a']

feature_extractor_tf = FeatureExtractor(input_scalars=categorical_features, 
                                         output_vector='imputed_features', 
                                         output_vector_items=categorical_features)

# Label Encoder for x1 Label 
label_encoder_tf = LabelEncoder(input_features=feature_extractor_tf.output_vector_items,
                               output_features='{}_label_le'.format(categorical_features[0]))

# Reshape the output of the LabelEncoder to N-by-1 array
reshape_le_tf = ReshapeArrayToN1()

# Vector Assembler for x1 One Hot Encoder
one_hot_encoder_tf = OneHotEncoder(sparse_output=False)
one_hot_encoder_tf.mlinit(prior_tf = label_encoder_tf, 
                          output_features = '{}_label_one_hot_encoded'.format(categorical_features[0]))

one_hot_encoder_pipeline_x0 = Pipeline([
                                         (feature_extractor_tf.name, feature_extractor_tf),
                                         (label_encoder_tf.name, label_encoder_tf),
                                         (reshape_le_tf.name, reshape_le_tf),
                                         (one_hot_encoder_tf.name, one_hot_encoder_tf)
                                        ])

one_hot_encoder_pipeline_x0.mlinit()
one_hot_encoder_pipeline_x0.fit_transform(data)
one_hot_encoder_pipeline_x0.serialize_to_bundle('/tmp', 'mleap-scikit-test-pipeline', init=True)

# array([[ 1.,  0.,  0.],
#        [ 0.,  1.,  0.],
#        [ 0.,  0.,  1.]])
```

### Load and Transform Using MLeap

Because we export Spark and Scikit-learn pipelines to a standard format, we can use either our Spark-trained pipeline or our Scikit-learn pipeline from the previous steps to demonstrate usage of MLeap in this section. The choice is yours!

```scala
import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport._
import scala.util.Using
// load the Spark pipeline we saved in the previous section
val bundle = Using(BundleFile("jar:file:/tmp/simple-spark-pipeline.zip")) { bundleFile =>
  bundleFile.loadMleapBundle().get
}.get

// create a simple LeapFrame to transform
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.core.types._

// MLeap makes extensive use of monadic types like Try
val schema = StructType(StructField("test_string", ScalarType.String),
  StructField("test_double", ScalarType.Double)).get
val data = Seq(Row("hello", 0.6), Row("MLeap", 0.2))
val frame = DefaultLeapFrame(schema, data)

// transform the dataframe using our pipeline
val mleapPipeline = bundle.root
val frame2 = mleapPipeline.transform(frame).get
val data2 = frame2.dataset

// get data from the transformed rows and make some assertions
assert(data2(0).getDouble(2) == 1.0) // string indexer output
assert(data2(0).getDouble(3) == 1.0) // binarizer output

// the second row
assert(data2(1).getDouble(2) == 2.0)
assert(data2(1).getDouble(3) == 0.0)
```

## Documentation

For more documentation, please see our [documentation](https://combust.github.io/mleap-docs/), where you can learn to:

1. Implement custom transformers that will work with Spark, MLeap and Scikit-learn
2. Implement custom data types to transform with Spark and MLeap pipelines
3. Transform with blazing fast speeds using optimized row-based transformers
4. Serialize MLeap data frames to various formats like avro, json, and a custom binary format
5. Implement new serialization formats for MLeap data frames
6. Work through several demonstration pipelines which use real-world data to create predictive pipelines
7. Supported Spark transformers
8. Supported Scikit-learn transformers
9. Custom transformers provided by MLeap

## Contributing

* Write documentation.
* Write a tutorial/walkthrough for an interesting ML problem
* Contribute an Estimator/Transformer from Spark
* Use MLeap at your company and tell us what you think
* Make a feature request or report a bug in github
* Make a pull request for an existing feature request or bug report
* Open an issue or pull request on [GitHub](https://github.com/combust/mleap) to start a discussion

## Building

The recommended way to build and test is inside the devcontainer image, which
pins the exact sbt, Java, Scala, and Python versions used by CI so you don't have
to install or track them yourself.

1. Initialize the git submodules: `git submodule update --init --recursive`
2. Build the image: `docker build -t mleap:ci .devcontainer`
3. Run a build or the test suite inside it, with the repo mounted:

```bash
# compile
docker run --rm -v "$PWD:/workspace" -w /workspace mleap:ci sbt compile

# run the full test suite (same as CI)
docker run --rm -v "$PWD:/workspace" -w /workspace mleap:ci make test
```

If you prefer to build directly on your host instead, install the sbt, Java, and
Scala versions listed for the latest release in the compatibility matrix above,
then run `sbt compile`.

## Thank You

Thank you to [Swoop](https://www.swoop.com/) for supporting the XGboost
integration.

## Contributors Information

* Jason Sleight ([jsleight](https://github.com/jsleight))
* Talal Riaz ([talalryz](https://github.com/talalryz))
* Weichen Xu ([WeichenXu123](https://github.com/WeichenXu123))
* Ludovic Trottier ([ltrottier-yelp](https://github.com/ltrottier-yelp))

## Past contributors
* Hollin Wilkins (hollin@combust.ml)
* Mikhail Semeniuk (mikhail@combust.ml)
* Anca Sarb (sarb.anca@gmail.com)
* Ryan Vogan (rvogan@yelp.com)


## License

See LICENSE and NOTICE file in this repository.

Copyright Combust, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
