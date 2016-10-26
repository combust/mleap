package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.RandomForestClassifierModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.spark.ml.linalg.Vector

import scala.util.Try

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class RandomForestClassifier(override val uid: String = Transformer.uniqueName("random_forest_classification"),
                                  featuresCol: String,
                                  predictionCol: String,
                                  model: RandomForestClassifierModel) extends Transformer {
  val exec: UserDefinedFunction = (features: Vector) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(predictionCol, featuresCol)(exec)
  }
}
