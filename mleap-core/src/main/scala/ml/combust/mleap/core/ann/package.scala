package ml.combust.mleap.core

import ml.combust.mleap.core.annotation.SparkCode

/** Most of the contents of this package were taken from Spark
  * and adapter for use without training or a Spark Context
  */
package object ann {
  @SparkCode(uri = "https://github.com/apache/spark/tree/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/ann")
  case object SparkCodeHolder
}
