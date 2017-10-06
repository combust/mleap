package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BucketizerModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{SimpleTransformer, Transformer}

/**
  * Created by mikhail on 9/19/16.
  */
case class Bucketizer(override val uid: String = Transformer.uniqueName("bucketizer"),
                      override val shape: NodeShape,
                      override val model: BucketizerModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (value: Double) => model(value)
}

object BucketizerUtil {
  def restoreSplits(splits : Array[Double]): Array[Double] = {
    splits.update(0, update(splits.head, Double.NegativeInfinity))
    splits.update(splits.length - 1, update(splits.last, Double.PositiveInfinity))
    splits
  }

  private def update(orig: Double, updated: Double) = if (orig.isNaN) updated else orig
}