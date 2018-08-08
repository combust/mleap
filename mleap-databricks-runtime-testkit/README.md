# Submitting Tests to Spark

```
sbt mleap-databricks-runtime-fat/assembly mleap-databricks-runtime-testkit/assembly

spark-submit --jars $PWD/mleap-databricks-runtime-fat/target/scala-2.11/mleap-databricks-runtime-fat-assembly-0.11.0-SNAPSHOT.jar \
  mleap-databricks-runtime-testkit/target/scala-2.11/mleap-databricks-runtime-testkit-assembly-0.11.0-SNAPSHOT.jar
```