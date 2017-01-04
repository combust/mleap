package org.apache.spark.sql.mleap

import ml.combust.mleap.runtime.types
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.VectorUDT

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 10/22/16.
  */
trait TypeConverters {
  implicit def sparkType(dataType: types.DataType): Option[DataType] = dataType match {
    case types.BooleanType(_) => Some(BooleanType)
    case types.StringType(_) => Some(StringType)
    case types.IntegerType(_) => Some(IntegerType)
    case types.LongType(_) => Some(LongType)
    case types.DoubleType(_) => Some(DoubleType)
    case lt: types.ListType => sparkType(lt.base).map(t => ArrayType(t, containsNull = false))
    case tt: types.TensorType if tt.base == types.DoubleType(false) && tt.dimensions.length == 1 => Some(new VectorUDT())
    case ct: types.CustomType => UDTRegistration.getUDTFor(ct.klazz.getCanonicalName).
      map(_.newInstance().asInstanceOf[UserDefinedType[_]])
    case types.AnyType(_) => None
  }
}
object TypeConverters extends TypeConverters
