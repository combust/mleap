package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class Coalesce(override val uid: String = Transformer.uniqueName("coalesce"),
                    inputCols: Array[String],
                    outputCol: String,
                    model: CoalesceModel) extends Transformer {
  private val f = (values: TupleData) => model(values.values: _*)
  val exec: UserDefinedFunction = UserDefinedFunction(f,
    ScalarType.Double,
    Seq(TupleType(model.nullableInputs.map(n => DataType(BasicType.Double, ScalarShape(n))): _*)))

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, inputCols)(exec)
  }

  override def getFields(): Try[Seq[StructField]] = {
    val inputFields = inputCols.zip(model.nullableInputs).map {
      case (name, isNullable) => StructField(name, DataType(BasicType.Double, ScalarShape(isNullable)))
    }

    Success(inputFields :+ StructField(outputCol, ScalarType(BasicType.Double, isNullable = true)))
  }
}
