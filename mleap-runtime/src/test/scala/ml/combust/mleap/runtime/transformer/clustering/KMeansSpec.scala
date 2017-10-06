package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.KMeansModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.DenseTensor
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/30/16.
  */
class KMeansSpec extends FunSpec {
  val v1 = Vectors.dense(Array(1.0, 2.0, 55.0))
  val v2 = Vectors.dense(Array(11.0, 200.0, 55.0))
  val v3 = Vectors.dense(Array(100.0, 22.0, 55.0))

  val schema = StructType(Seq(StructField("features", TensorType(BasicType.Double)))).get
  val dataset = Seq(Row(DenseTensor(Array(2.0, 5.0, 34.0), Seq(3))),
    Row(DenseTensor(Array(20.0, 230.0, 34.0), Seq(3))),
    Row(DenseTensor(Array(111.0, 20.0, 56.0), Seq(3))))
  val frame = DefaultLeapFrame(schema, dataset)
  val km = KMeans(shape = NodeShape.basicCluster(), model = KMeansModel(Seq(v1, v2, v3), 3))

  describe("#transform") {
    it("uses the k-means to find closest cluster") {
      val frame2 = km.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getInt(1) == 0)
      assert(data(1).getInt(1) == 1)
      assert(data(2).getInt(1) == 2)
    }

    describe("with invalid features column") {
      val km2 = km.copy(shape = NodeShape.basicCluster(featuresCol = "bad_features"))

      it("returns a Failure") { assert(km2.transform(frame).isFailure) }
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(km.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Int.nonNullable)))
    }
  }
}
