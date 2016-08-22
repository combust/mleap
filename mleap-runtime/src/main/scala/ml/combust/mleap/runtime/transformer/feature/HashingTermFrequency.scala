package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.HashingTermFrequencyModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{StringType, TensorType}

import scala.util.Try

/**
  * Created by hwilkins on 12/30/15.
  */
case class HashingTermFrequency(uid: String = Transformer.uniqueName("hashing_term_frequency"),
                                inputCol: String,
                                outputCol: String,
                                hashingTermFrequency: HashingTermFrequencyModel) extends Transformer {
  override def build[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(inputCol, StringType).flatMap {
      case (b, inputIndex) =>
        b.withOutput(outputCol, TensorType.doubleVector())(row => hashingTermFrequency(row.getString(inputIndex)))
    }
  }
}
