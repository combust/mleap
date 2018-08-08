# MLeap Databricks ML Runtime Fat JAR

This is the fat jar that has all of the MLeap and dependencies of MLeap included. All 3rd-party
dependencies are shaded into the jar. This JAR is not the one published to maven central,
as the pom file would still contain all of the 3rd-party dependencies. Instead, see the
`mleap-databricks-runtime` submodule, which provides a facade to this JAR for publishing
to maven repos.

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
