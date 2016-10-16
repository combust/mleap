package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.KMeansModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, TensorType}

import scala.util.Try

/**
  * Created by hollinwilkins on 9/30/16.
  */
case class KMeans(override val uid: String = Transformer.uniqueName("k_means"),
                  featuresCol: String,
                  predictionCol: String,
                  model: KMeansModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(featuresCol, TensorType.doubleVector()).flatMap {
      case(b, featuresIndex) =>
        b.withOutput(predictionCol, DoubleType)(row => model(row.getVector(featuresIndex)).toDouble)
    }
  }
}
