# Submitting Tests to Spark

Build the fat JAR and the testkit assembly, then submit the testkit to Spark.
Replace the version and dependency versions to match the release you are
building (see the compatibility matrix in the top-level README).

```
sbt mleap-databricks-runtime-fat/assembly mleap-databricks-runtime-testkit/assembly

spark-submit --jars $PWD/mleap-databricks-runtime-fat/target/scala-2.13/mleap-databricks-runtime-fat-assembly-0.25.1.jar \
  --packages org.tensorflow:tensorflow-core-api:1.0.0,ml.dmlc:xgboost4j-spark_2.13:2.0.3 \
  mleap-databricks-runtime-testkit/target/scala-2.13/mleap-databricks-runtime-testkit-assembly-0.25.1.jar
```
