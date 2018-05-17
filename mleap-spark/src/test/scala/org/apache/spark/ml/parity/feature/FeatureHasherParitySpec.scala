package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.DataFrame

class FeatureHasherParitySpec extends SparkParityBase {
  import spark.implicits._

  override val dataset: DataFrame = Seq(
    (2.0, true, "1", "foo"),
    (3.0, false, "2", "bar")
  ).toDF("real", "bool", "stringNum", "string")

  override val sparkTransformer: Transformer = new FeatureHasher()
    .setInputCols("real", "bool", "stringNum", "string")
    .setOutputCol("features")
}
