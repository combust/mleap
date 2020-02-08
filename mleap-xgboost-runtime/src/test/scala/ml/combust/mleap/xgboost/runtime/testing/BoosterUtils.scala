package ml.combust.mleap.xgboost.runtime.testing

import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}

trait BoosterUtils {

  final val xgboostParams: Map[String, Any] = Map(
    "eta" -> 0.3,
    "max_depth" -> 2,
    "objective" -> "binary:logistic",
    "num_round" -> 15,
    "num_classes" -> 2
  )

  def trainBooster(xgboostParams: Map[String, Any], dataset: DMatrix): Booster =
    XGBoost.train(dataset, xgboostParams, xgboostParams("num_round").asInstanceOf[Int])

}
