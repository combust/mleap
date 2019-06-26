package org.apache.spark.ml.classification.parity

import org.apache.spark.ml.classification.LinearSVCModel
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.DataFrame

class LinearSVCParitySpec extends SparkParityBase
{
    override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti")
    override val sparkTransformer: Transformer = new Pipeline()
      .setStages(Array(
        new StringIndexer().
            setInputCol("fico_score_group_fnl").
            setOutputCol("fico_index"),
        new VectorAssembler().
                setInputCols(Array("fico_index", "dti")).
                setOutputCol("features"),
        new LinearSVCModel("linear_svc",
            Vectors.dense(0.44, 0.77),
            0.66).setThreshold(0.5).setFeaturesCol("features")))
      .fit(dataset)

    // The string order type is ignored, because once the transformer is built based on some order type, we need to serialize only the string to index map
    // but not the order in which it has to index. This value we can ignore while we check the transformer values.
    override val unserializedParams: Set[String] = Set("stringOrderType")
}

