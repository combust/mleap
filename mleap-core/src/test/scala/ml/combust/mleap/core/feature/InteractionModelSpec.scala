package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{DoubleType, ScalarShape, TensorShape, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 4/26/17.
  */
class InteractionModelSpec extends FunSpec {
  describe("with all numeric inputs") {
    it("produces the expected interaction vector") {
      val encoderSpec: Array[Array[Int]] = Array(Array(1), Array(1, 1))
      val model = InteractionModel(encoderSpec, base = DoubleType(), inputShapes = Seq(ScalarShape, TensorShape(2)))
      val features = Seq(2.toDouble, Vectors.dense(3, 4))

      assert(model(features).toArray.toSeq == Seq(6, 8))
    }
  }

  describe("with one nominal input") {
    it("produce the expected interaction vector") {
      val encoderSpec: Array[Array[Int]] = Array(Array(4), Array(1, 1))
      val model = InteractionModel(encoderSpec, base = DoubleType(), inputShapes = Seq(ScalarShape, TensorShape(2)))
      val features = Seq(2.toDouble, Vectors.dense(3, 4))

      assert(model(features).toArray.toSeq == Seq(0, 0, 0, 0, 3, 4, 0, 0))
    }
  }
}
