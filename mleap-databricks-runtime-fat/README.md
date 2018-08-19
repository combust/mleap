# MLeap Databricks ML Runtime Fat JAR

This is the fat jar that has all of the MLeap and dependencies of MLeap included. All 3rd-party
dependencies are shaded into the jar. This JAR is not the one published to maven central,
as the pom file would still contain all of the 3rd-party dependencies. Instead, see the
`mleap-databricks-runtime` submodule, which provides a facade to this JAR for publishing
to maven repos.

## Installation

Requirements:

1. `xgboost4j-spark` must be in the local M2 repository to build. [xgboost4j-spark installation](http://xgboost.readthedocs.io/en/latest/jvm/)

After making sure `xgboost` is available to compilation, get a local copy of MLeap and build the Databricks ML Runtime Fat JAR.

```
# Use --recursive to get the submodule with MLeap protobuf definitions
git clone --recursive https://github.com/combust/mleap.git
cd mleap

# Compile the Databrics Runtime Assembly JAR
sbt mleap-databricks-runtime-fat/assembly

# Verify that it worked, you should see a file like: mleap-databricks-runtime-fat-assembly-<mleap version>-SNAPSHOT.jar
ls mleap-databricks-runtime-fat/target/scala-2.11
```

## Usage

See the README.md file in `mleap-databricks-runtime-testkit` for sample usage of the JAR and how to run basic tests.

## Namespaces

There are 3 namespaces allowed in this JAR, all other namespaces are shaded if required for execution.

1. `ml.combust`
2. `org.apache.spark`
3. `ml.dmlc.xgboost`

Allowed namespaces may be added in the future with more integrations with 3rd-party ML libraries.

## Shading

All shaded dependencies are transformed into the `ml.combust.mleap.shaded` namespace and can be found there.

## Provided

Certain dependencies are expected to be provided. These include:

1. Spark ML and its dependencies
2. TensorFlow JAR and dynamic library configured on classpath
3. XGBoost Spark and dynamic library configured on classpath
