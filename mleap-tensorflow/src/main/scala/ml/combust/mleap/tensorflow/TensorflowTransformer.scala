package ml.combust.mleap.tensorflow

import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
  * Created by hollinwilkins on 1/12/17.
  */
case class TensorflowTransformer(override val uid: String = Transformer.uniqueName("tensorflow"),
                                 inputCols: Seq[String],
                                 outputCols: Seq[String],
                                 rawOutputCol: Option[String] = None,
                                 model: TensorflowModel) extends Transformer {
  val actualRawCol = rawOutputCol.getOrElse(uid)
  val exec: UserDefinedFunction = (tensors: Seq[Any]) => model(tensors: _*)
  val outputUdfs: Seq[UserDefinedFunction] = outputCols.zipWithIndex.map {
    case (output, index) =>
      val udf: UserDefinedFunction = (raw: Seq[Any]) => raw(index)
      udf.copy(returnType = model.outputs(index)._2)
  }
  private val outputs = outputCols.zip(outputUdfs)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    val builder2 = builder.withOutput(actualRawCol, inputCols.toArray)(exec)
    outputs.foldLeft(builder2) {
      case (b, (name, udf)) => b.flatMap(_.withOutput(name, actualRawCol)(udf))
    }
  }

  override def close(): Unit = { model.close() }
}
