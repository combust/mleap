package ml.combust.mleap.core.feature

import edu.emory.mathcs.jtransforms.dct.DoubleDCT_1D
import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Created by hollinwilkins on 12/28/16.
  */
case class DCTModel(inverse: Boolean, inputSize: Int) extends Model {

  def apply(features: Vector): Vector = {
    val result = features.toArray.clone()
    val jTransformer = new DoubleDCT_1D(result.length)
    if (inverse) jTransformer.inverse(result, true) else jTransformer.forward(result, true)
    Vectors.dense(result)
  }

  override def inputSchema: StructType = StructType("input" -> TensorType.Double(inputSize)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(inputSize)).get
}
