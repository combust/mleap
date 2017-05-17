package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StandardScalerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}

import scala.util.{Success, Try}

/**
  * Created by hwilkins on 10/23/15.
  */
case class StandardScaler(override val uid: String = Transformer.uniqueName("standard_scaler"),
                          override val inputCol: String,
                          override val outputCol: String,
                          model: StandardScalerModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Tensor[Double]) => model(value): Tensor[Double]

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, TensorType(DoubleType())),
    StructField(outputCol, TensorType(DoubleType()))))
}
