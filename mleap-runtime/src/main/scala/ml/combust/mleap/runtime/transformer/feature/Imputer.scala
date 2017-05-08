package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ImputerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.runtime.types.{DataType, DoubleType, StructField}

import scala.util.{Failure, Success, Try}

/**
  * Created by mikhail on 12/18/16.
  */
case class Imputer(override val uid: String = Transformer.uniqueName("imputer"),
                   override val inputCol: String,
                   inputDataType: Option[DataType],
                   override val outputCol: String,
                   model: ImputerModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Any) => model.predictAny(value)

  override def getSchema(): Try[Seq[StructField]] = {
    inputDataType match {
      case None => Failure(new RuntimeException(s"Cannot determine schema for transformer ${this.uid}"))
      case Some(inputType) =>  Success(Seq(
                                  StructField(inputCol, inputType),
                                  StructField(outputCol, DoubleType())))
    }
  }
}
