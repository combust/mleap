package ml.combust.mleap.core.classification

import ml.combust.mleap.core.ann.FeedForwardTopology
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Created by hollinwilkins on 12/25/16.
  */

object MultiLayerPerceptronClassifierModel {

  /** Label to vector converter. */
  @SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/classification/MultilayerPerceptronClassifier.scala")
  object LabelConverter {
    // TODO: Use OneHotEncoder instead
    /**
      * Encodes a label as a vector.
      * Returns a vector of given length with zeroes at all positions
      * and value 1.0 at the position that corresponds to the label.
      *
      * @param labeledPoint labeled point
      * @param labelCount   total number of labels
      * @return pair of features and vector encoding of a label
      */
    def encodeLabeledPoint(labeledPoint: LabeledPoint, labelCount: Int): (Vector, Vector) = {
      val output = Array.fill(labelCount)(0.0)
      output(labeledPoint.label.toInt) = 1.0
      (labeledPoint.features, Vectors.dense(output))
    }

    /**
      * Converts a vector to a label.
      * Returns the position of the maximal element of a vector.
      *
      * @param output label encoded with a vector
      * @return label
      */
    def decodeLabel(output: Vector): Double = {
      output.argmax.toDouble
    }
  }
}

@SparkCode(uri = "https://github.com/apache/spark/blob/v2.3.0/mllib/src/main/scala/org/apache/spark/ml/classification/MultilayerPerceptronClassifier.scala")
case class MultiLayerPerceptronClassifierModel(layers: Seq[Int],
                                               weights: Vector,
                                               override val thresholds: Option[Array[Double]] = None) extends ProbabilisticClassificationModel {
  val numFeatures: Int = layers.head

  private val mlpModel = FeedForwardTopology
    .multiLayerPerceptron(layers.toArray)
    .model(weights)

  override def predictRaw(features: Vector): Vector = {
    mlpModel.predictRaw(features)
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    mlpModel.raw2ProbabilityInPlace(raw)
  }

  override val numClasses: Int = layers.last

}
