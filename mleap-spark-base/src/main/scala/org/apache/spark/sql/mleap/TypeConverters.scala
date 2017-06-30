package org.apache.spark.sql.mleap

import ml.combust.mleap.runtime
import ml.combust.mleap.core.types
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.types._

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 10/22/16.
  */
trait TypeConverters {
  implicit def sparkType(dataType: types.DataType): Option[DataType] = dataType match {
    case types.BooleanType(_) => Some(BooleanType)
    case types.StringType(_) => Some(StringType)
    case types.ByteType(_) => Some(ByteType)
    case types.ShortType(_) => Some(ShortType)
    case types.IntegerType(_) => Some(IntegerType)
    case types.LongType(_) => Some(LongType)
    case types.FloatType(_) => Some(FloatType)
    case types.DoubleType(_) => Some(DoubleType)
    case tt: types.TupleType =>
      val fields = tt.dts.zipWithIndex.map {
        case (dt, index) =>
          sparkType(dt).map(d => StructField(s"_$index", d)).get
      }
      Some(StructType(fields))
    case lt: types.ListType => sparkType(lt.base).map(t => ArrayType(t, containsNull = false))
    case _: types.TensorType => Some(new TensorUDT)
    case types.AnyType(_) => None
    case _ => throw new RuntimeException(s"unsupported data type: $dataType")
  }

  implicit def mleapType(dataType: DataType): types.DataType = dataType match {
    case BooleanType => types.BooleanType()
    case StringType => types.StringType()
    case ByteType => types.ByteType()
    case ShortType => types.ShortType()
    case IntegerType => types.IntegerType()
    case LongType => types.LongType()
    case FloatType => types.FloatType()
    case DoubleType => types.DoubleType()
    case at: ArrayType => types.ListType(base = mleapType(at.elementType))
    case _: VectorUDT => types.TensorType(base = types.DoubleType())
    case tt: TensorUDT =>
      // This is not correct to assume it has a Double base type
      // But this is the best we can do for now
      types.TensorType(base = types.DoubleType())
    case _ => throw new RuntimeException(s"unsupported data type: $dataType")
  }

  implicit def mleapStructType(schema: StructType): types.StructType = {
    val fields = schema.fields.map {
      field =>
        val dt = mleapType(field.dataType)
        types.StructField(field.name, dt)
    }.toSeq

    types.StructType(fields).get
  }
}
object TypeConverters extends TypeConverters
