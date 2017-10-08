package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/15/16.
  */
class VectorAssemblerSpec extends FunSpec {
  val schema = StructType(Seq(StructField("feature1", TensorType(BasicType.Double)),
    StructField("feature2", ScalarType.Double),
    StructField("feature3", ScalarType.Int))).get
  val dataset = Seq(Row(Tensor.denseVector(Array(0.5, -0.5, 1.0)), 42.0, 13))
  val frame = DefaultLeapFrame(schema, dataset)
  val vectorAssembler = VectorAssembler(
    shape = NodeShape().withInput("input0", "feature1").
              withInput("input1", "feature2").
              withInput("input2", "feature3").
          withStandardOutput("features"),
    model = VectorAssemblerModel(Seq(TensorShape(3), ScalarShape(), ScalarShape())))

  describe("#transform") {
    it("assembles its inputs into a new vector") {
      val frame2 = vectorAssembler.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getTensor[Double](3).toArray sameElements Array(0.5, -0.5, 1.0, 42.0, 13.0))
    }

    describe("with invalid input") {
      val vectorAssembler2 = vectorAssembler.copy(shape = NodeShape().withInput("input0", "bad_input").
              withStandardOutput("features"),
      model = VectorAssemblerModel(Seq(ScalarShape())))

      it("returns a Failure") { assert(vectorAssembler2.transform(frame).isFailure) }
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(vectorAssembler.schema.fields ==
        Seq(StructField("feature1", TensorType(BasicType.Double, Seq(3))),
          StructField("feature2", ScalarType.Double),
          StructField("feature3", ScalarType.Double),
          StructField("features", TensorType(BasicType.Double, Seq(5)))))
    }
  }
}
