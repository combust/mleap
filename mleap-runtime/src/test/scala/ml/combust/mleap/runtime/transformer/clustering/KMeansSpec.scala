package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.KMeansModel
import ml.combust.mleap.core.regression.GBTRegressionModel
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.runtime.types.{StructField, StructType, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/30/16.
  */
class KMeansSpec extends FunSpec {
  val v1 = Vectors.dense(Array(1.0, 2.0, 55.0))
  val v2 = Vectors.dense(Array(11.0, 200.0, 55.0))
  val v3 = Vectors.dense(Array(100.0, 22.0, 55.0))

  val schema = StructType(Seq(StructField("features", TensorType.doubleVector()))).get
  val dataset = LocalDataset(Seq(Row(Vectors.dense(Array(2.0, 5.0, 34.0))),
    Row(Vectors.dense(Array(20.0, 230.0, 34.0))),
    Row(Vectors.dense(Array(111.0, 20.0, 56.0)))))
  val frame = LeapFrame(schema, dataset)
  val km = KMeans(featuresCol = "features",
    predictionCol = "prediction",
    model = KMeansModel(Array(v1, v2, v3)))

  describe("#transform") {
    it("uses the k-means to find closest cluster") {
      val frame2 = km.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getDouble(1) == 0.0)
      assert(data(1).getDouble(1) == 1.0)
      assert(data(2).getDouble(1) == 2.0)
    }

    describe("with invalid features column") {
      val km2 = km.copy(featuresCol = "bad_features")

      it("returns a Failure") { assert(km2.transform(frame).isFailure) }
    }
  }
}
