package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.OneVsRestModel
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class OneVsRest(uid: String = Transformer.uniqueName("one_vs_rest"),
                     featuresCol: String,
                     predictionCol: String,
                     probabilityCol: Option[String] = None,
                     model: OneVsRestModel) extends Transformer {
  override def build[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(featuresCol, TensorType.doubleVector()).flatMap {
      case(b, featuresIndex) =>
        probabilityCol match {
          case Some(pc) =>
            b.withOutputs(Seq(StructField(predictionCol, DoubleType),
              StructField(pc, DoubleType))) {
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
