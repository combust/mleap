package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ElementwiseProductModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import org.apache.spark.ml.linalg.Vector

import scala.util.Try

/**
  * Created by mikhail on 9/23/16.
  */
case class ElementwiseProduct(override val uid: String = Transformer.uniqueName("elmentwise_product"),
                              override val inputCol: String,
                              override val  outputCol: String,
                              model: ElementwiseProductModel) extends FeatureTransformer {

  override val exec: UserDefinedFunction = (value: Vector) => model(value)
}
