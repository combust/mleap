package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BucketedRandomProjectionLSHModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.FeatureTransformer
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.types._

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class BucketedRandomProjectionLSH(override val uid: String,
                                       override val inputCol: String,
                                       override val outputCol: String,
                                       model: BucketedRandomProjectionLSHModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (features: Tensor[Double]) => model(features).map(v => v: Tensor[Double])

  override def getSchema(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, TensorType(DoubleType())),
    StructField(outputCol, ListType(TensorType(DoubleType())))
  ))
}
