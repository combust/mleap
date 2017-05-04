package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.types.{DataType, DoubleType, StructField, TensorType}

import scala.util.{Failure, Success, Try}

/**
  * Created by hwilkins on 10/23/15.
  */
case class VectorAssembler(override val uid: String = Transformer.uniqueName("vector_assembler"),
                           inputCols: Array[String],
                           inputDataTypes: Option[Array[DataType]],
                           outputCol: String) extends Transformer {
  val exec: UserDefinedFunction = (values: Seq[Any]) => VectorAssemblerModel.default(values): Tensor[Double]

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, inputCols)(exec)
  }

  override def getSchema(): Try[Seq[StructField]] = {
    if (inputDataTypes == None) {
      return Failure(new RuntimeException(s"Cannot determine schema for transformer ${this.uid}"))
    }

    val inputTypes = inputDataTypes.get
    val inputs : Seq[StructField] = (0 until inputCols.size).map(index => new StructField(inputCols(index), inputTypes(index)))
    Success(inputs :+ StructField(outputCol, TensorType(DoubleType())))
  }
}
