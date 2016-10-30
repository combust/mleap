package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.PolynomialExpansionModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import org.apache.spark.ml.linalg.Vector

/**
  * Created by mikhail on 10/16/16.
  */
case class PolynomialExpansion(override val uid: String = Transformer.uniqueName("polynomial_expansion"),
                               override val inputCol: String,
                               override val outputCol: String,
                               model: PolynomialExpansionModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Vector) => model(value)
}
