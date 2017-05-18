package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.PcaModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 10/12/16.
  */
case class Pca(override val uid: String = Transformer.uniqueName("pca"),
               override val inputCol: String,
               override val outputCol: String,
               model: PcaModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Tensor[Double]) => model(value): Tensor[Double]

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, TensorType(DoubleType())),
    StructField(outputCol, TensorType(DoubleType()))))
}
