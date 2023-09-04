package ml.combust.mleap.core.clustering

import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.mleap.VectorWithNorm
import org.scalatest.funspec.AnyFunSpec

class BisectingKMeansModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("bisecting kmeans model") {
    val model = new BisectingKMeansModel(ClusteringTreeNode(23,
      VectorWithNorm(Vectors.dense(1, 2, 3)) , Array()))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features", TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("prediction", ScalarType.Int.nonNullable)))
    }
  }
}