# MLeap

[![Join the chat at https://gitter.im/combust-ml/mleap](https://badges.gitter.im/combust-ml/mleap.svg)](https://gitter.im/combust-ml/mleap?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/combust-ml/mleap.svg?branch=master)](https://travis-ci.org/combust-ml/mleap)

Easily put your Spark ML Pipelines into action with MLeap. Train your feature and regression/classification pipeline with Spark or Scikit-Learn, then easily convert them to MLeap pipelines to deploy them anywhere. Take your pipelines to an API server, Hadoop, or even back to Spark to execute on a DataFrame.

MLeap allows for easy serialization of your estimator and transformer pipelines so you can save them for reuse later. Executing an MLeap pipeline does not require a SparkContext or DataFrame so there is very little overhead for realtime one-off predictions. You don't have to worry about Spark dependencies for executing your models, just add the lightweight MLeap runtime library instead.

MLeap makes deploying your Spark and Scikit-Learn ML pipelines with 3 core functions:

1. Release: Deploy your entire ML pipeline without a SparkContext or any dependency on Spark libraries.
2. Reuse: Export your ML pipeline to easy-to-read JSON or Protobuf files so you can reuse pipelines.
3. Recycle: Export your training pipelines to easy-to-read JSON files so you can easily modify your training pipelines.
4. Don't Recode: Prototype quickly in Scikit-Learn and export your pipeline and models to Spark (without having rewrite any transformers)

## Setup

### Link with Maven or SBT

MLeap is cross-compiled for Scala 2.10 and 2.11, so just replace 2.10 with 2.11 wherever you see it if you are running Scala version 2.11 and using a POM file for dependency management. Otherwise, use the `%%` operator if you are using SBT and the correct Scala version will be used.

#### SBT

```
libraryDependencies += "ml.combust.mleap" %% "mleap-runtime" % "0.3.0"
```

#### Maven

```
<dependency>
    <groupId>ml.combust.mleap</groupId>
    <artifactId>mleap-runtime_2.10</artifactId>
    <version>0.3.0</version>
</dependency>
```


#### Python

```
git clone
cd mleap/python

python setup.py install
```

### For Spark Integration

#### SBT

```
libraryDependencies += "ml.combust.mleap" %% "mleap-spark" % "0.3.0"
```

#### Maven

```
<dependency>
    <groupId>ml.combust.mleap</groupId>
    <artifactId>mleap-spark_2.10</artifactId>
    <version>0.3.0</version>
</dependency>
```

### Spark Packages

MLeap is now a [Spark Package](http://spark-packages.org/package/combust-ml/mleap).

```bash
$ bin/spark-shell --packages ml.combust.mleap:mleap-spark_2.11:0.5.0
```

## Modules

MLeap is broken into 4 modules:

1. mleap-core - Core execution building blocks, includes runtime for executing linear regressions, random forest models, logisitic regressions, assembling feature vectors, string indexing, one hot encoding, etc.
2. mleap-runtime - Provides LeapFrame data structure, MLeap transformers and Bundle.ML serialization for MLeap.
3. mleap-spark - Provides Spark Bundle.ML serialization.
4. python/mleap - Provides PySpark and Scikit learn extensions.


## Demos

We have demo notebooks as well as instructions on how to configure Toree to work with [Spark 2.x](https://github.com/combust-ml/mleap/wiki/Setting-up-a-Spark-2.0-notebook-with-MLeap-and-Toree), [PySpark](https://github.com/combust-ml/mleap/wiki/Setting-up-a-Spark-2.0-notebook-with-MLeap-and-Toree) and Scikit-Learn [here](https://github.com/combust-ml/mleap-demo/tree/master/notebooks).

## Future of MLeap

1. Provide TensorFlow bindings
2. Unify linear algebra and core ML models library with Spark
3. Deploy outside of the JVM to embedded systems
4. Easy serialization/deserialization of custom tranformers accross Spark/Scikit/TensorFlow

## Contributing

There are a few ways to contribute to MLeap.

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
