package ml.combust.mleap.core.feature

import java.math.BigDecimal

import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class VectorAssemblerModelSpec extends org.scalatest.funspec.AnyFunSpec {
  val assembler = VectorAssemblerModel(Seq(
    ScalarShape(), ScalarShape(),
    TensorShape(2),
    TensorShape(5)))

  describe("#apply") {
    it("assembles doubles and vectors into a new vector") {
      val expectedArray = Array(45.0, 76.8, 23.0, 45.6, 0.0, 22.3, 45.6, 0.0, 99.3)

      assert(assembler(Array(45.0,
        new BigDecimal(76.8),
        Vectors.dense(Array(23.0, 45.6)),
        Vectors.sparse(5, Array(1, 2, 4), Array(22.3, 45.6, 99.3)))).toArray.sameElements(expectedArray))
    }
  }

  describe("input/output schema") {
    it("has the right input schema") {
      assert(assembler.inputSchema.fields == Seq(
        StructField("input0", ScalarType.Double),
        StructField("input1", ScalarType.Double),
        StructField("input2", TensorType.Double(2)),
        StructField("input3", TensorType.Double(5))))
    }

    it("has the right output schema") {
      assert(assembler.outputSchema.fields == Seq(StructField("output", TensorType.Double(9))))
    }
  }
}
