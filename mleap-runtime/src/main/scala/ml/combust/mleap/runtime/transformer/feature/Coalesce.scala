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
    TensorType(model.base),
    Seq(TupleType(model.inputShapes.map(s => DataType(model.base, s)): _*)))

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, inputCols)(exec)
  }

  override def getFields(): Try[Seq[StructField]] = {
    val inputFields = inputCols.zip(model.inputShapes).map {
      case (name, shape) => StructField(name, DataType(model.base, shape))
    }

    Success(inputFields :+ StructField(outputCol, ScalarType(model.base, isNullable = true)))
  }
}
