package org.apache.spark.ml.linalg.mleap

import ml.combust.mleap.core.annotation.SparkCode

/**
  * Created by hollinwilkins on 11/17/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/branch-2.0/mllib-local/src/main/scala/org/apache/spark/ml/impl/Utils.scala")
object Utils {
  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }
}
