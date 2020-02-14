package ml.combust.mleap.xgboost.runtime.testing

import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}


trait BoosterUtils {

  val commonXGboostParams: Map[String, Any] = Map(
    "eta" -> 0.3,
    "max_depth" -> 2,
    "num_round" -> 15
  )

  val xgboostBinaryParams: Map[String, Any] = commonXGboostParams ++ Map("objective" -> "binary:logistic")

  val xgboostMultinomialParams: Map[String, Any] = commonXGboostParams ++ Map(
    "num_class" -> 3,
    "objective" -> "multi:softprob"
  )

  def trainBooster(dataset: DMatrix): Booster =
    XGBoost.train(dataset, xgboostBinaryParams, xgboostBinaryParams("num_round").asInstanceOf[Int])

  def trainMultinomialBooster(dataset: DMatrix): Booster =
    XGBoost.train(dataset, xgboostMultinomialParams, xgboostMultinomialParams("num_round").asInstanceOf[Int])

}
