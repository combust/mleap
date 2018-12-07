# Submitting Tests to Spark

```
sbt mleap-databricks-runtime-fat/assembly mleap-databricks-runtime-testkit/assembly

spark-submit --jars $PWD/mleap-databricks-runtime-fat/target/scala-2.11/mleap-databricks-runtime-fat-assembly-0.13.0-SNAPSHOT.jar \
  --packages org.tensorflow:tensorflow:1.9.0,org.tensorflow:libtensorflow_jni:1.9.0,ml.dmlc:xgboost4j-spark:0.80 \
  mleap-databricks-runtime-testkit/target/scala-2.11/mleap-databricks-runtime-testkit-assembly-0.13.0-SNAPSHOT.jar
```