package org.apache.spark.sql.mleap

import ml.combust.mleap.core.types
import ml.combust.mleap.core.types.{BasicType, ScalarType}
import org.apache.spark.sql.types._

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 10/22/16.
  */
trait TypeConverters {
  implicit def mleapToSparkBasicType(base: BasicType): DataType = base match {
    case BasicType.Boolean => BooleanType
    case BasicType.Byte => ByteType
    case BasicType.Short => ShortType
    case BasicType.Int => IntegerType
    case BasicType.Long => LongType
    case BasicType.Float => FloatType
    case BasicType.Double => DoubleType
    case BasicType.String => StringType
    case BasicType.ByteString => ArrayType(ByteType, containsNull = false)
  }

  implicit def mleapToSparkType(dataType: types.DataType): DataType = dataType match {
    case types.ScalarType(base, _) => base
    case types.ListType(base, _) => ArrayType(base, containsNull = false)
    case types.TensorType(base, _, _) => new TensorUDT(base)
    case types.TupleType(dts @ _*) =>
      val fields = dts.zipWithIndex.map {
        case (dt, index) => StructField(s"_$index", mleapToSparkType(dt))
      }

      StructType(fields)
  }

  implicit def sparkToMleapBasicType(dataType: DataType): BasicType = dataType match {
    case BooleanType => BasicType.Boolean
    case ByteType => BasicType.Byte
    case ShortType => BasicType.Short
    case IntegerType => BasicType.Int
    case LongType => BasicType.Long
    case FloatType => BasicType.Float
    case DoubleType => BasicType.Double
    case StringType => BasicType.String
    case ArrayType(ByteType, false) => BasicType.Boolean
    case _ => throw new IllegalArgumentException(s"invalid basic Spark type $dataType")
  }

  implicit def sparkToMleapStructField(field: StructField): types.StructField = {
    val dt = field.dataType match {
      case BooleanType => ScalarType.Boolean
      case ByteType => ScalarType.Byte
      case ShortType => ScalarType.Short
      case IntegerType => ScalarType.Int
      case LongType => ScalarType.Long
      case FloatType => ScalarType.Float
      case DoubleType => ScalarType.Double
      case StringType => ScalarType.String
      case ArrayType(ByteType, false) => ScalarType.ByteString // TODO: make a custom type for byte string
      case ArrayType(base, _) => types.ListType(base)
      case tu: TensorUDT => types.TensorType(tu.base)
    }

    types.StructField(field.name, dt.setNullable(field.nullable))
  }

  implicit def mleapStructType(schema: StructType): types.StructType = {
    val fields = schema.fields.map {
      field => field: types.StructField
    }.toSeq

    types.StructType(fields).get
  }
}
object TypeConverters extends TypeConverters
