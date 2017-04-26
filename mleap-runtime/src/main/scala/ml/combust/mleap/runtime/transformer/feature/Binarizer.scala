package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BinarizerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, TensorType}

import scala.util.{Failure, Try}

/**
  * Created by fshabbir on 12/1/16.
  */
case class Binarizer(override val uid: String = Transformer.uniqueName("binarizer"),
                     inputCol: String,
                     outputCol: String,
                     model: BinarizerModel) extends Transformer {
  val execTensor: UserDefinedFunction = (value: Tensor[Double]) => model(value): Tensor[Double]
  val execDouble: UserDefinedFunction = (value: Double) => model(value): Double

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.schema.getField(inputCol).map(_.dataType).map {
      case DoubleType(false) =>
        builder.withOutput(outputCol, inputCol)(execDouble)
      case tt: TensorType if tt.base == DoubleType() && !tt.isNullable =>
        builder.withOutput(outputCol, inputCol)(execTensor)
    }.getOrElse(Failure(new IllegalArgumentException("Input column must be double or double tensor")))
  }
}
