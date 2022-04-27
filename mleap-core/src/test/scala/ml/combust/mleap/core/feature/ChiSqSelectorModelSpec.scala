package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.scalatest.FunSpec
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

class ChiSqSelectorModelSpec extends FunSpec {

  describe("input/output schema"){
    val model = new ChiSqSelectorModel(Seq(2,3, 1), 3)

    it("Dense vector work with unsorted indices") {
      val vector = Vectors.dense(1.0,2.0,3.0,4.0)
      assert(model(vector) == Vectors.dense(2.0, 3.0, 4.0))
    }

    it("Sparse vector work with unsorted indices") {
      val vector = Vectors.sparse(size = 4, indices=Array(0,1,2,3), values = Array(1.0,2.0,3.0,4.0))
      assert(model(vector) == Vectors.sparse(size=3, indices=Array(0,1,2), values=Array(2.0,3.0,4.0)))
    }

    it("Has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", TensorType.Double(3))))
    }

    it("Has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double(3))))
    }
  }
}
