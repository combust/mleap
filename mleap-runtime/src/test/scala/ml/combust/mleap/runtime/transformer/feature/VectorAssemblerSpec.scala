package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/15/16.
  */
class VectorAssemblerSpec extends FunSpec {
  val schema = StructType(Seq(StructField("feature1", TensorType(BasicType.Double)),
    StructField("feature2", ScalarType.Double),
    StructField("feature3", ScalarType.Double))).get
  val dataset = LocalDataset(Seq(Row(Tensor.denseVector(Array(0.5, -0.5, 1.0)), 42.0, 13.0)))
  val frame = LeapFrame(schema, dataset)
  val vectorAssembler = VectorAssembler(inputCols = Array("feature1", "feature2", "feature3"),
    outputCol = "features",
    model = VectorAssemblerModel(BasicType.Double, Seq(TensorShape(3), ScalarShape(), ScalarShape())))

  describe("#transform") {
    it("assembles its inputs into a new vector") {
      val frame2 = vectorAssembler.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getTensor[Double](3).toArray sameElements Array(0.5, -0.5, 1.0, 42.0, 13.0))
    }

    describe("with invalid input") {
      val vectorAssembler2 = vectorAssembler.copy(inputCols = vectorAssembler.inputCols :+ "bad_feature")

      it("returns a Failure") { assert(vectorAssembler2.transform(frame).isFailure) }
    }
  }

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      assert(vectorAssembler.getFields().get ==
        Seq(StructField("feature1", TensorType(BasicType.Double, Some(Seq(3)))),
          StructField("feature2", ScalarType.Double),
          StructField("feature3", ScalarType.Double),
          StructField("features", TensorType(BasicType.Double, Some(Seq(5))))))
    }
  }
}
