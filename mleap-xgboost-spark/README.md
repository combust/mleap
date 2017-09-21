# MLeap XGBoost Spark

This is the XGBoost Spark integration for MLeap. It provides Bundle Ops for serializing/deserialize XGBoostClassificationModel and XGBoostRegressionModel to a Bundle.ML file.

## Installation

The first thing to do is follow the [xgboost4j-spark installation](http://xgboost.readthedocs.io/en/latest/jvm/) documents to get a copy of the `xgboost4j-spark` jars in your local maven repository. At the time of writing this document, `xgboost4j` libraries are not provided on any public repositories such as maven central or bintray.

**IMPORTANT NOTES**

1. If you see an issue during `mvn install` about "Training failed", add your hostname to `/etc/hosts`: `127.0.0.1 your-computers-hostname`

After you have installed the local `xgboost4j-spark` library, test to make sure everything is working by running the unit tests.

```
# Use --recursive to get the submodule with MLeap protobuf definitions
git clone --recursive https://github.com/combust/mleap.git
cd mleap

# Run XGBoost Spark tests
sbt mleap-xgboost-spark/test
```

In order to use `mleap-xgboost-spark` you will have to install it as a local library in either your local maven repository or local ivy2 repository.

### Install to Local Ivy2

```
sbt mleap-xgboost-spark/publishLocal
```

### Install to Local Maven

```
sbt mleap-xgboost-spark/publishM2
```

## Usage

Once you have installed to either ivy2 or maven, the next step is to include `mleap-xgboost-spark` as a dependency in your training job.

### SBT Dependency

```
// Make sure to set mleapVersion, at the time of this writing
// mleapVersion should be "0.8.0"
libraryDependencies += "ml.combust.mleap" %% "mleap-xgboost-spark" % mleapVersion
```

### Maven Dependency

Make sure to set `ml.combust.mleap.version` to the desired MLeap version.
As of the time of this writing, the value should be `0.8.0`.

```
<dependency>
  <groupId>ml.combust.mleap</groupId>
  <artifactId>mleap-xgboost-spark_${scala.binary.version}</artifactId>
  <version>${ml.combust.mleap.version}</version>
</dependency>
```

Once you have added `mleap-xgboost-spark` as a dependency to your project, you should be able to train an `XGBoostClassificationModel` or `XGBoostRegressionModel` using the `XGBoostEstimator`. You will then be able to export the classifier or regression to MLeap along with the rest of your Spark pipeline.

[Exporting Spark pipelines with MLeap](http://mleap-docs.combust.ml/spark/)

## Usage in Notebook

To setup the xgboost integration for usage with a notebook like Zeppelin or Jupyter, you need to make sure that `mleap-xgboost-spark` *as well as its dependencies* are included in the class path. A common way to do this is to use an assembly jar, also known as a "fat" or "shaded" jar.

### Create Assembly Jar

```
sbt mleap-xgboost-spark/assembly
```

Once you have created the assembly jar, point your notebook to it as a dependency.