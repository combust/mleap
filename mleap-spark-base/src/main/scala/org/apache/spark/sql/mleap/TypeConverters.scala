package org.apache.spark.sql.mleap

import ml.combust.mleap.runtime.types
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
    case lt: types.ListType => sparkType(lt.base).map(t => ArrayType(t, containsNull = false))
    case tt: types.TensorType => Some(new TensorUDT)
    case ct: types.CustomType => UDTRegistration.getUDTFor(ct.klazz.getCanonicalName).
      map(_.newInstance().asInstanceOf[UserDefinedType[_]])
    case types.AnyType(_) => None
    case _ => throw new RuntimeException(s"unsupported data type: $dataType")
  }
}
object TypeConverters extends TypeConverters
