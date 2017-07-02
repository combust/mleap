package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MaxAbsScalerModel
import ml.combust.mleap.core.types.{BasicType, StructField, TensorType}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.{Success, Try}

/**
  * Created by mikhail on 9/18/16.
  */
case class MaxAbsScaler(override val uid: String = Transformer.uniqueName("max_abs_scaler"),
                        override val inputCol: String,
                        override val outputCol: String,
                       model: MaxAbsScalerModel) extends FeatureTransformer {

  override val exec: UserDefinedFunction = (value: Tensor[Double]) => model(value): Tensor[Double]

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, TensorType(BasicType.Double)),
    StructField(outputCol, TensorType(BasicType.Double))))
}
