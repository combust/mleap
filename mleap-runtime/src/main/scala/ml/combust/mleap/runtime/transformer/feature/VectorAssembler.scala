package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.core.types.{DataType, ScalarShape, StructField, TensorShape}
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.function.UserDefinedFunction

import scala.util.{Failure, Success, Try}

/**
  * Created by hwilkins on 10/23/15.
  */
case class VectorAssembler(override val uid: String = Transformer.uniqueName("vector_assembler"),
                           inputCols: Array[String],
                           outputCol: String,
                           model: VectorAssemblerModel) extends Transformer {
  val exec: UserDefinedFunction = (values: Seq[Any]) => model(values): Tensor[Double]

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, inputCols)(exec)
  }

  override def getFields(): Try[Seq[StructField]] = {
    val inputFields = inputCols.zip(model.inputShapes).map {
      case (name, shape) => StructField(name, DataType(model.base, shape))
    }
    val outputSize = model.inputShapes.foldLeft(0) {
      (acc, shape) => shape match {
        case ScalarShape(false) => acc + 1
        case TensorShape(Seq(size), false) => acc + size
        case _ => return Failure(new IllegalArgumentException(s"invalid shape for vector assembler $shape"))
      }
    }

    Success(inputFields :+ StructField(outputCol, DataType(model.base, TensorShape(outputSize))))
  }
}
