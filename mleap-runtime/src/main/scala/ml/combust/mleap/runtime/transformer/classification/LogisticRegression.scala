package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.LogisticRegressionModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, TensorType}

import scala.util.Try

/**
  * Created by hwilkins on 10/22/15.
  */
case class LogisticRegression(uid: String = Transformer.uniqueName("logistic_regression"),
                              featuresCol: String,
                              predictionCol: String,
                              model: LogisticRegressionModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(featuresCol, TensorType.doubleVector()).flatMap {
      case(b, featuresIndex) =>
        b.withOutput(predictionCol, DoubleType)(row => model(row.getVector(featuresIndex)))
    }
  }
}
