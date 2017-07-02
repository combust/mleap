package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.GBTClassifierModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 9/24/16.
  */
case class GBTClassifier(override val uid: String = Transformer.uniqueName("gbt_classifier"),
                         featuresCol: String,
                         predictionCol: String,
                         model: GBTClassifierModel) extends Transformer {
  val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(predictionCol, featuresCol)(exec)
  }

  override def getFields(): Try[Seq[StructField]] = Success(
                Seq(StructField(featuresCol, TensorType(BasicType.Double)),
                    StructField(predictionCol, ScalarType.Double)))
}
