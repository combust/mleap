package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BinarizerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.{Failure, Success, Try}

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
      case ScalarType(BasicType.Double, false) =>
        builder.withOutput(outputCol, inputCol)(execDouble)
      case TensorType(BasicType.Double, _, false) =>
        builder.withOutput(outputCol, inputCol)(execTensor)
      case dt => Failure(new IllegalArgumentException(s"invalid input column type $dt"))
    }.getOrElse(Failure(new IllegalArgumentException("Input column must be double or double tensor")))
  }

  override def getFields(): Try[Seq[StructField]] = {
    Success(Seq(StructField(inputCol, DataType(model.base, model.inputShape)),
      StructField(outputCol, DataType(model.base, model.inputShape))))
  }
}
