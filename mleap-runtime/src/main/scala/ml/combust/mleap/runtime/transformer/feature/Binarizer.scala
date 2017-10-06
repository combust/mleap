package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BinarizerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

/**
  * Created by fshabbir on 12/1/16.
  */
case class Binarizer(override val uid: String = Transformer.uniqueName("binarizer"),
                     override val shape: NodeShape,
                     override val model: BinarizerModel) extends SimpleTransformer {
  val execTensor: UserDefinedFunction = (value: Tensor[Double]) => model(value): Tensor[Double]
  val execDouble: UserDefinedFunction = (value: Double) => model(value): Double

  override val exec: UserDefinedFunction = if(model.inputShape.isScalar) {
    execDouble
  } else {
    execTensor
  }
}
