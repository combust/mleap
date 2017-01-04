package org.apache.spark.ml.mleap.feature

import ml.combust.mleap.core.feature.MathBinaryModel
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DoubleType, NumericType, StructField, StructType}
import org.apache.spark.sql.functions.udf

/**
  * Created by hollinwilkins on 12/27/16.
  */
trait MathBinaryParams extends HasOutputCol {
  final val inputA: Param[String] = new Param[String](this, "inputA", "input for left side of binary operation")
  final def getInputA: String = $(inputA)

  final val inputB: Param[String] = new Param[String](this, "inputB", "input for right side of binary operation")
  final def getInputB: String = $(inputB)
}

class MathBinary(override val uid: String = Identifiable.randomUID("math_binary"),
                 val model: MathBinaryModel) extends Transformer
with MathBinaryParams {
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setInputA(value: String): this.type = set(inputA, value)
  def setInputB(value: String): this.type = set(inputB, value)

  @org.apache.spark.annotation.Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val binaryUdfA = udf {
      (a: Double) => model(Some(a), None)
    }
    val binaryUdfB = udf {
      (b: Double) => model(None, Some(b))
    }
    val binaryUdfAB = udf {
      (a: Double, b: Double) => model(Some(a), Some(b))
    }
    val binaryUdfNone = udf {
      () => model(None, None)
    }

    (isSet(inputA), isSet(inputB)) match {
      case (true, true) => dataset.withColumn($(outputCol), binaryUdfAB(dataset($(inputA)).cast(DoubleType),
        dataset($(inputB)).cast(DoubleType)))
      case (true, false) => dataset.withColumn($(outputCol), binaryUdfA(dataset($(inputA)).cast(DoubleType)))
      case (false, true) => dataset.withColumn($(outputCol), binaryUdfB(dataset($(inputB)).cast(DoubleType)))
      case (false, false) => dataset.withColumn($(outputCol), binaryUdfNone())
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    if(isSet(inputA)) {
      require(schema($(inputA)).dataType.isInstanceOf[NumericType],
        s"Input column A must be of type NumericType but got ${schema($(inputA)).dataType}")
    }

    if(isSet(inputB)) {
      require(schema($(inputB)).dataType.isInstanceOf[NumericType],
        s"Input column B must be of type NumericType but got ${schema($(inputB)).dataType}")
    }

    val inputFields = schema.fields
    require(!inputFields.exists(_.name == $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")

    StructType(schema.fields :+ StructField($(outputCol), DoubleType))
  }
}
