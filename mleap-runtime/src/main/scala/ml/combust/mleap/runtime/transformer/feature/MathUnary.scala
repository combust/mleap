package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MathUnaryModel
import ml.combust.mleap.core.types.{DoubleType, StructField}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 12/27/16.
  */
case class MathUnary(override val uid: String = Transformer.uniqueName("math_unary"),
                     override val inputCol: String,
                     override val outputCol: String,
                     model: MathUnaryModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (a: Double) => model(a)

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, DoubleType()),
    StructField(outputCol, DoubleType())))
}
