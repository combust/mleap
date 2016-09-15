package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.LogisticRegressionModel
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class LogisticRegression(uid: String = Transformer.uniqueName("logistic_regression"),
                              featuresCol: String,
                              predictionCol: String,
                              probabilityCol: Option[String] = None,
                              model: LogisticRegressionModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(featuresCol, TensorType.doubleVector()).flatMap {
      case(b, featuresIndex) =>
        probabilityCol match {
          case Some(probability) =>
            b.withOutputs(Seq(StructField(predictionCol, DoubleType),
              StructField(probability, DoubleType))) {
              row =>
                val r = model.predictWithProbability(row.getVector(featuresIndex))
                Row(r._1, r._2)
            }
          case None =>
            b.withOutput(predictionCol, DoubleType)(row => model(row.getVector(featuresIndex)))
        }
    }
  }
}
