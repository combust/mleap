package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.classification.LightGBMClassifierModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.{Vector, Vectors}

@SparkCode(uri = "https://github.com/Azure/mmlspark/blob/f07e5584459e909223a470e6d2e11135b292f3ea/" +
  "src/main/scala/com/microsoft/ml/spark/lightgbm/LightGBMClassifier.scala")
case class LightGBMClassifier(override val uid: String = Transformer.uniqueName("lightgbm_classifier"),
                                override val shape: NodeShape,
                                override val model: LightGBMClassifierModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val f = (features: Tensor[Double]) => {

      if (model.thresholdValues.isDefined) {
        require(model.thresholdValues.get.length == model.numClasses, this.getClass.getSimpleName +
          ".transform() called with non-matching numClasses and thresholds.length." +
          s" numClasses=$model.numClasses, but thresholds has length ${model.thresholdValues.get.length}")
      }

      val rawPrediction: Vector =
        if (shape.getOutput("raw_prediction").nonEmpty)
          model.predictRaw(features)
        else
          Vectors.dense(Array.empty[Double])

      val probability: Vector =
        if (shape.getOutput("probability").nonEmpty)
          model.predictProbabilities(features)
        else
          Vectors.dense(Array.empty[Double])

      val prediction =
        if (shape.getOutput("prediction").isDefined) {
          if (shape.getOutput("raw_prediction").nonEmpty && model.thresholdValues.isEmpty) {
            // Note: Only call raw2prediction if thresholds not defined
            model.rawToPrediction(rawPrediction)
          } else if (shape.getOutput("prediction").nonEmpty) {
            model.probabilityToPrediction(probability)
          } else {
            model.predict(features)
          }
        }
        else
          Double.NaN

      Row(rawPrediction: Tensor[Double], probability: Tensor[Double], prediction)
    }
    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}
