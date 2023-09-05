package ml.combust.mleap.core.clustering

import breeze.linalg.{DenseMatrix, Matrix}
import ml.combust.mleap.core.types.{StructField, TensorType}
import org.scalatest.funspec.AnyFunSpec

class LDAModelSpec extends org.scalatest.funspec.AnyFunSpec {


  describe("lda model") {
    val topics: Matrix[Double] = DenseMatrix.zeros[Double](3,3)
    val model = new LocalLDAModel(topics, null, 2, 100)

    it("Has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features", TensorType.Double(3))))
    }

    it("Has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("prediction", TensorType.Double(3))))
    }
  }
}
