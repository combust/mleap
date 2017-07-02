package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.InteractionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.tensor.Tensor

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 4/26/17.
  */
case class Interaction(override val uid: String = Transformer.uniqueName("interaction"),
                       inputCols: Array[String],
                       outputCol: String,
                       model: InteractionModel) extends Transformer {
  private val f = (values: TupleData) => model(values.values): Tensor[Double]
  val exec: UserDefinedFunction = UserDefinedFunction(f,
    TensorType(BasicType.Double),
    Seq(TupleType(model.inputShapes.map(s => DataType(BasicType.Double, s)): _*)))

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, inputCols)(exec)
  }

  override def getFields(): Try[Seq[StructField]] = {
    val inputFields = inputCols.zip(model.inputShapes).map {
      case (name, shape) => StructField(name, DataType(BasicType.Double, shape))
    }
    val outputSize = model.featuresSpec.map(_.sum).product

    Success(inputFields :+ StructField(outputCol, DataType(BasicType.Double, TensorShape(outputSize))))
  }
}
