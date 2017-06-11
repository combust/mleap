package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DataType, DoubleType, StructField}

import scala.util.{Failure, Success, Try}

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class Coalesce(override val uid: String = Transformer.uniqueName("coalesce"),
                    inputCols: Array[String],
                    inputDataTypes: Option[Array[DataType]],
                    outputCol: String,
                    model: CoalesceModel) extends Transformer {
  val exec: UserDefinedFunction = (values: Seq[Any]) => model(values: _*)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, inputCols)(exec)
  }

  override def getFields(): Try[Seq[StructField]] = {
    inputDataTypes match {
      case None => Failure(new RuntimeException(s"Cannot determine schema for transformer ${this.uid}"))
      case Some(inputTypes) => val inputs : Seq[StructField] = (0 until inputCols.length).map(index => StructField(inputCols(index), inputTypes(index)))
                                Success(inputs :+ StructField(outputCol, DoubleType(true)))
    }
  }
}
