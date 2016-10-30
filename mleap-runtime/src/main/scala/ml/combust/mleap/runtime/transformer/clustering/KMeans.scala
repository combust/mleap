package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.KMeansModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.spark.ml.linalg.Vector

import scala.util.Try

/**
  * Created by hollinwilkins on 9/30/16.
  */
case class KMeans(override val uid: String = Transformer.uniqueName("k_means"),
                  featuresCol: String,
                  predictionCol: String,
                  model: KMeansModel) extends Transformer {
  val exec: UserDefinedFunction = (features: Vector) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(predictionCol, featuresCol)(exec)
  }
}
