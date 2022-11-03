# Submitting Tests to Spark

```
sbt mleap-databricks-runtime-fat/assembly mleap-databricks-runtime-testkit/assembly

spark-submit --jars $PWD/mleap-databricks-runtime-fat/target/scala-2.12/mleap-databricks-runtime-fat-assembly-0.21.0.jar \
  --packages org.tensorflow:tensorflow:1.11.0,org.tensorflow:libtensorflow_jni:1.11.0,ml.dmlc:xgboost4j-spark:1.6.1 \
  mleap-databricks-runtime-testkit/target/scala-2.12/mleap-databricks-runtime-testkit-assembly-0.21.0.jar
```
