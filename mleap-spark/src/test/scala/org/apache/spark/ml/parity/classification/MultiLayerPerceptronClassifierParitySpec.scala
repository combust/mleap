package org.apache.spark.ml.parity.classification

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 12/25/16.
  */
class MultiLayerPerceptronClassifierParitySpec extends SparkParityBase {
  override val dataset: DataFrame = multiClassClassificationDataset

  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(
    new MultilayerPerceptronClassifier(uid = "mlp").
      setThresholds(Array(0.1, 0.2, 0.3)).
      // specify layers for the neural network:
      // input layer of size 4 (features), two intermediate of size 5 and 4
      // and output of size 3 (classes)
      setLayers(Array(4, 5, 4, 3)).
      setFeaturesCol("features").
      setPredictionCol("prediction"))).fit(dataset)
}
