package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.PolynomialExpansionModel
import ml.combust.mleap.runtime.{LeapFrame, Row, LocalDataset}
import ml.combust.mleap.runtime.types.{TensorType, StructField, StructType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by mikhail on 10/16/16.
  */
class PolynomialExpansionSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_vec", TensorType.doubleVector()))).get
  val dataset = LocalDataset(Seq(Row(Vectors.dense(Array(2.0, 3.0)))))
  val frame = LeapFrame(schema, dataset)

  val transformer = PolynomialExpansion(inputCol = "test_vec",
    outputCol = "test_expanded",
    model = PolynomialExpansionModel(2))

  describe("#transform") {
    it("Takes 2+ dimensional vector and runs polynomial expansion") {
      val frame2 = transformer.transform(frame).get
      val data = frame2.dataset.toArray
      val expanded = data(0).getVector(1)
      val expectedVector = Array(2.0, 4.0, 3.0, 6.0, 9.0)

      assert(expanded.toArray.sameElements(expectedVector))
    }
  }
}
