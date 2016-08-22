package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, TensorType}

import scala.util.Try

/**
  * Created by hwilkins on 11/8/15.
  */
case class DecisionTreeRegression(uid: String = Transformer.uniqueName("decision_tree_regression"),
                                  featuresCol: String,
                                  predictionCol: String,
                                  model: DecisionTreeRegressionModel) extends Transformer {
  override def build[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(featuresCol, TensorType.doubleVector()).flatMap {
      case (b, featuresIndex) =>
        b.withOutput(predictionCol, DoubleType)(row => model(row.getVector(featuresIndex)))
    }
  }
}
