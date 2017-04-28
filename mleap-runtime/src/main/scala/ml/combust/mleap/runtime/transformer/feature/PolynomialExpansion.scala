package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.PolynomialExpansionModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}

import scala.util.{Success, Try}

/**
  * Created by mikhail on 10/16/16.
  */
case class PolynomialExpansion(override val uid: String = Transformer.uniqueName("polynomial_expansion"),
                               override val inputCol: String,
                               override val outputCol: String,
                               model: PolynomialExpansionModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Tensor[Double]) => model(value): Tensor[Double]

  override def getSchema(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, TensorType(DoubleType())),
    StructField(outputCol, TensorType(DoubleType()))))
}
