package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BucketizerModel
import ml.combust.mleap.core.types.{DoubleType, StructField}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

import scala.util.{Success, Try}

/**
  * Created by mikhail on 9/19/16.
  */
case class Bucketizer(override val uid: String = Transformer.uniqueName("bucketizer"),
                      override val inputCol: String,
                      override val outputCol: String,
                      model: BucketizerModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Double) => model(value)

  override def getFields(): Try[Seq[StructField]] = Success(
    Seq(StructField(inputCol, DoubleType()),
      StructField(outputCol, DoubleType())))
}

object BucketizerUtil {
  def restoreSplits(splits : Array[Double]): Array[Double] = {
    splits.update(0, update(splits.head, Double.NegativeInfinity))
    splits.update(splits.length - 1, update(splits.last, Double.PositiveInfinity))
    splits
  }

  private def update(orig: Double, updated: Double) = if (orig.isNaN) updated else orig
}