package ml.combust.mleap.runtime.transformer.clustering

import breeze.linalg.{DenseMatrix, Matrix}
import ml.combust.mleap.core.clustering.LocalLDAModel
import ml.combust.mleap.core.types._
import org.scalatest.funspec.AnyFunSpec

class LDASpec extends AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val topics: Matrix[Double] = DenseMatrix.zeros[Double](3,3)

      val transformer = LDA(shape =
        NodeShape.basicCluster(),
        model = new LocalLDAModel(topics, null, 2, 100))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", TensorType.Double(3))))
    }
  }
}