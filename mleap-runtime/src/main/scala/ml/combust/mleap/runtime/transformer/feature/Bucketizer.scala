package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.{BucketizerModel, HandleInvalid}
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.{FieldSelector, UserDefinedFunction}
import ml.combust.mleap.runtime.frame.{FrameBuilder, SimpleTransformer, Transformer}

import scala.util.Try

/**
  * Created by mikhail on 9/19/16.
  */
case class Bucketizer(override val uid: String = Transformer.uniqueName("bucketizer"),
                      override val shape: NodeShape,
                      override val model: BucketizerModel) extends SimpleTransformer {
  val input: String = inputSchema.fields.head.name
  val inputSelector: FieldSelector = input
  val exec: UserDefinedFunction = (value: Double) => model(value)

  override def transform[FB <: FrameBuilder[FB]](builder: FB): Try[FB] = {
    if(model.handleInvalid == HandleInvalid.Skip) {
      builder.filter(input) {
        value: Double => !value.isNaN
      }.flatMap(_.withColumn(output, inputSelector)(exec))
    } else {
      builder.withColumn(output, inputSelector)(exec)
    }
  }
}

object BucketizerUtil {
  def restoreSplits(splits : Array[Double]): Array[Double] = {
    splits.update(0, update(splits.head, Double.NegativeInfinity))
    splits.update(splits.length - 1, update(splits.last, Double.PositiveInfinity))
    splits
  }

  private def update(orig: Double, updated: Double) = if (orig.isNaN) updated else orig
}