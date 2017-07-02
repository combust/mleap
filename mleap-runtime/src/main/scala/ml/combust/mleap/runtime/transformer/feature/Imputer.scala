package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ImputerModel
import ml.combust.mleap.core.types.{DataType, DoubleType, StructField}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

import scala.util.{Failure, Success, Try}

/**
  * Created by mikhail on 12/18/16.
  */
case class Imputer(override val uid: String = Transformer.uniqueName("imputer"),
                   override val inputCol: String,
                   override val outputCol: String,
                   model: ImputerModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Any) => model.predictAny(value)

  override def getFields(): Try[Seq[StructField]] = {
    Success(Seq(StructField(inputCol, DoubleType(model.inputNullable)),
      StructField(outputCol, DoubleType())))
  }
}
