package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.runtime.types.{DataType, DoubleType, StructField}

import scala.util.{Failure, Success, Try}

/**
  * Created by hwilkins on 10/22/15.
  */
case class StringIndexer(override val uid: String = Transformer.uniqueName("string_indexer"),
                         override val inputCol: String,
                         inputDataType: Option[DataType],
                         override val outputCol: String,
                         model: StringIndexerModel) extends FeatureTransformer {
  val exec: UserDefinedFunction = (value: Any) => model(value)

  def toReverse: ReverseStringIndexer = ReverseStringIndexer(inputCol = inputCol,
    outputCol = outputCol,
    model = model.toReverse)

  def toReverse(name: String): ReverseStringIndexer = ReverseStringIndexer(uid = name,
    inputCol = inputCol,
    outputCol = outputCol,
    model = model.toReverse)

  override def getSchema(): Try[Seq[StructField]] = {
    inputDataType match {
      case None => Failure(new RuntimeException(s"Cannot determine schema for transformer ${this.uid}"))
      case Some(inputType) =>  Success(Seq(StructField(inputCol, inputType),
        StructField(outputCol, DoubleType())))
    }
  }
}
