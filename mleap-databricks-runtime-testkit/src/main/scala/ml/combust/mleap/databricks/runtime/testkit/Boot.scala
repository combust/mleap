package ml.combust.mleap.databricks.runtime.testkit

import org.apache.spark.sql.SparkSession

object Boot extends App {
  val session = SparkSession.builder()
    .appName("TestMleapDatabricksRuntime")
    .config("spark.ui.enabled", "false")
    .master("local[2]")
    .getOrCreate()
  val sqlContext = session.sqlContext

  new TestSparkMl(session).run()
  new TestTensorflow(session).run()
  new TestXgboost(session).run()

  session.close()
}
