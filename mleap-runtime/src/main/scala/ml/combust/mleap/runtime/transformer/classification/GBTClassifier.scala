package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.GBTClassifierModel
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}

import scala.util.Try

/**
  * Created by hollinwilkins on 9/24/16.
  */
case class GBTClassifier(override val uid: String = Transformer.uniqueName("gbt_classifier"),
                         featuresCol: String,
                         predictionCol: String,
                         probabilityCol: Option[String] = None,
                         model: GBTClassifierModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(featuresCol, TensorType.doubleVector()).flatMap {
      case(b, featuresIndex) =>
        probabilityCol match {
          case Some(probability) =>
            b.withOutputs(Seq(StructField(predictionCol, DoubleType),
              StructField(probability, DoubleType))) {
              row =>
                val (prediction, probability) = model.predictWithProbability(row.getVector(featuresIndex))
                Row(prediction, probability)
            }
          case None =>
            b.withOutput(predictionCol, DoubleType)(row => model(row.getVector(featuresIndex)))
        }
    }
  }
}
