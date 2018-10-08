package ml.combust.mleap.databricks.runtime.testkit

import org.apache.spark.sql.SparkSession

object Boot extends App {
  val session = SparkSession.builder().
    appName("TestMleapDatabricksRuntime").
    master("local[2]").
    getOrCreate()
  val sqlContext = session.sqlContext

  new TestSparkMl(session).run()
  new TestTensorflow(session).run()

  session.close()
}
