package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.{BisectingKMeansModel, ClusteringTreeNode}
import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.mleap.VectorWithNorm
import org.scalatest.FunSpec

class BisectingKMeansSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = BisectingKMeans(shape = NodeShape.basicCluster(),
        model = new BisectingKMeansModel(ClusteringTreeNode(23,
          VectorWithNorm(Vectors.dense(1, 2, 3)) , Array())))

      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Int.nonNullable)))
    }
  }
}