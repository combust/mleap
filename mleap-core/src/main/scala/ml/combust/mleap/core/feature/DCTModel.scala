package ml.combust.mleap.core.feature

import edu.emory.mathcs.jtransforms.dct.DoubleDCT_1D
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class DCTModel(inverse: Boolean) {
  def apply(features: Vector): Vector = {
    val result = features.toArray
    val jTransformer = new DoubleDCT_1D(result.length)
    if (inverse) jTransformer.inverse(result, true) else jTransformer.forward(result, true)
    Vectors.dense(result)
  }
}
