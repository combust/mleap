package org.apache.spark.ml.bundle

import ml.bundle
import ml.bundle.{BasicType, DataShapeType}
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 7/4/17.
  */
trait BundleTypeConverters {
  implicit def sparkToBundleDataShape(field: StructField)
                                     (implicit dataset: DataFrame): bundle.DataShape = {
    field.dataType match {
      case BooleanType | ByteType | ShortType | IntegerType
        | LongType | FloatType | DoubleType | StringType | ArrayType(ByteType, false) =>
        bundle.DataShape(DataShapeType.SCALAR)
      case ArrayType(_, _) => bundle.DataShape(DataShapeType.LIST)
      case _: VectorUDT =>
        // collect size information from dataset if necessary
        bundle.DataShape(bundle.DataShapeType.TENSOR)
      case _ => throw new IllegalArgumentException(s"invalid shape for field $field")
    }
  }

  implicit def sparkToBundleBasicType(dataType: DataType)
                                     (implicit dataset: DataFrame): bundle.BasicType = {
    dataType match {
      case BooleanType => BasicType.BOOLEAN
      case ByteType => BasicType.BYTE
      case ShortType => BasicType.SHORT
      case IntegerType => BasicType.INT
      case LongType => BasicType.LONG
      case FloatType => BasicType.FLOAT
      case DoubleType => BasicType.DOUBLE
      case StringType => BasicType.STRING
      case ArrayType(ByteType, _) => BasicType.BYTE_STRING
      case ArrayType(dt, _) => sparkToBundleBasicType(dt)
      case _: VectorUDT => BasicType.DOUBLE
      case _ => throw new IllegalArgumentException(s"invalid spark basic type $dataType")
    }
  }

  implicit def sparkToBundleDataType(field: StructField)
                                    (implicit dataset: DataFrame): bundle.DataType = {
    bundle.DataType(field.dataType, Some(field))
  }
}
object BundleTypeConverters extends BundleTypeConverters
