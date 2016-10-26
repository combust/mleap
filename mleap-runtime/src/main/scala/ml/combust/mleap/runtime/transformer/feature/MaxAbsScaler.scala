package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MaxAbsScalerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import org.apache.spark.ml.linalg.Vector

import scala.util.Try

/**
  * Created by mikhail on 9/18/16.
  */
case class MaxAbsScaler(override val uid: String = Transformer.uniqueName("max_abs_scaler"),
                        override val inputCol: String,
                        override val outputCol: String,
                       model: MaxAbsScalerModel) extends FeatureTransformer {

  override val exec: UserDefinedFunction = (value: Vector) => model(value)
}
