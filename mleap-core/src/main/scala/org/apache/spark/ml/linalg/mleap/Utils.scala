package org.apache.spark.ml.linalg.mleap

/**
  * Created by hollinwilkins on 11/17/16.
  */
object Utils {
  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }
}
