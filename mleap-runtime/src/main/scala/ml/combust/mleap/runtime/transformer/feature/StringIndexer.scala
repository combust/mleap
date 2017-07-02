package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

import scala.util.{Success, Try}

/**
  * Created by hwilkins on 10/22/15.
  */
case class StringIndexer(override val uid: String = Transformer.uniqueName("string_indexer"),
                         override val inputCol: String,
                         override val outputCol: String,
                         model: StringIndexerModel) extends FeatureTransformer {
  val exec: UserDefinedFunction = if(model.nullableInput) {
    (value: Option[String]) => model(value).toDouble
  } else {
    (value: String) => model(value).toDouble
  }

  def toReverse: ReverseStringIndexer = ReverseStringIndexer(inputCol = inputCol,
    outputCol = outputCol,
    model = model.toReverse)

  def toReverse(name: String): ReverseStringIndexer = ReverseStringIndexer(uid = name,
    inputCol = inputCol,
    outputCol = outputCol,
    model = model.toReverse)

  override def getFields(): Try[Seq[StructField]] = {
    Success(Seq(StructField(inputCol, ScalarType(BasicType.String, model.nullableInput)),
      StructField(outputCol, ScalarType.Double)))
  }
}
