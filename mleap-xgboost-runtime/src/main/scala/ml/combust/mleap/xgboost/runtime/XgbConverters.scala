package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.tensor.Tensor
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.DMatrix
import org.apache.spark.ml.linalg.Vector

trait XgbConverters {
  implicit class VectorOps(vector: Vector) {
    def asXGB: DMatrix = {
      new DMatrix(Iterator(new LabeledPoint(0.0f, null, vector.toDense.values.map(_.toFloat))))
    }
  }

  implicit class DoubleTensorOps(tensor: Tensor[Double]) {
    def asXGB: DMatrix = {
      new DMatrix(Iterator(new LabeledPoint(0.0f, null, tensor.toDense.rawValues.map(_.toFloat))))
    }
  }
}

object XgbConverters extends XgbConverters
