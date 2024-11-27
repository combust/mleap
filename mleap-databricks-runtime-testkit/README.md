# Submitting Tests to Spark

```
sbt mleap-databricks-runtime-fat/assembly mleap-databricks-runtime-testkit/assembly

spark-submit --jars $PWD/mleap-databricks-runtime-fat/target/scala-2.12/mleap-databricks-runtime-fat-assembly-0.23.3.jar \
  --packages org.tensorflow:tensorflow-core-api:0.5.0,ml.dmlc:xgboost4j-spark:1.7.6 \
  mleap-databricks-runtime-testkit/target/scala-2.12/mleap-databricks-runtime-testkit-assembly-0.23.3.jar
```
