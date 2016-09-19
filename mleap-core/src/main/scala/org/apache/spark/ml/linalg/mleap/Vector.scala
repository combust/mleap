package org.apache.spark.ml.linalg.mleap

import breeze.linalg.{Vector => BreezeVector}
import org.apache.spark.ml.linalg.Vector

/**
  * Created by mikhail on 9/18/16.
  */
object Vector {
  implicit class VectorOps(vector: Vector) {
    def toBreeze: BreezeVector[Double] = vector.asBreeze
  }
}