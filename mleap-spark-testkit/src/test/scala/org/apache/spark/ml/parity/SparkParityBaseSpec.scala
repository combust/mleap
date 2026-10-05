package org.apache.spark.ml.parity

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

class SparkParityBaseSpec extends SparkParityBase {
  override val dataset: DataFrame = null
  override val sparkTransformer: Transformer = null

  override def parityTransformer(): Unit = ()

  describe("checkRowWithRelTol") {
    it("compares array fields element-wise, so NaN elements are equal") {
      val row = Row(mutable.ArraySeq(0.1, 0.2, 0.0, Double.NaN))
      checkRowWithRelTol(row, Row(mutable.ArraySeq(0.1, 0.2, 0.0, Double.NaN)), 1e-6)
    }
  }
}
