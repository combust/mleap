package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.InteractionModel
import ml.combust.mleap.core.types.{DataType, DoubleType, StructField, TensorType}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.tensor.Tensor

import scala.util.{Failure, Success, Try}

/**
  * Created by hollinwilkins on 4/26/17.
  */
case class Interaction(override val uid: String = Transformer.uniqueName("interaction"),
                       inputCols: Array[String],
                       inputDataTypes: Option[Array[DataType]],
                       outputCol: String,
                       model: InteractionModel) extends Transformer {
  val exec: UserDefinedFunction = (values: Seq[Any]) => model(values): Tensor[Double]

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, inputCols)(exec)
  }

  override def getFields(): Try[Seq[StructField]] = {
    inputDataTypes match {
      case None => Failure(new RuntimeException(s"Cannot determine schema for transformer ${this.uid}"))
      case Some(inputTypes) => val inputs : Seq[StructField] = (0 until inputCols.size).map(index => new StructField(inputCols(index), inputTypes(index)))
        Success(inputs :+ StructField(outputCol, TensorType(DoubleType())))

    }
  }
}
