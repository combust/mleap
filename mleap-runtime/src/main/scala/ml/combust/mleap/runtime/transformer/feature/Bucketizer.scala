package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BucketizerModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.DoubleType

import scala.util.Try

/**
  * Created by mikhail on 9/19/16.
  */
case class Bucketizer(uid: String = Transformer.uniqueName("bucketizer"),
                      inputCol: String,
                      outputCol: String,
                      model: BucketizerModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol, DoubleType).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, DoubleType)(row => model(row.getDouble(inputIndex)))
    }
  }
}
