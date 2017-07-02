package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorSlicerModel
import ml.combust.mleap.core.types.{BasicType, StructField, TensorType}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.FeatureTransformer
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class VectorSlicer(override val uid: String,
                        override val inputCol: String,
                        override val outputCol: String,
                        model: VectorSlicerModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features): Tensor[Double]

  override def getFields(): Try[Seq[StructField]] = Success(
    Seq(StructField(inputCol, TensorType(BasicType.Double)),
      StructField(outputCol, TensorType(BasicType.Double))))
}
