package org.apache.spark.sql.mleap

import ml.bundle
import ml.combust.mleap.runtime.types
import ml.combust.mleap.runtime.types.BundleTypeConverters.mleapTypeToBundleType
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.DataFrame
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

  implicit def mleapType(dataType: DataType): Option[types.DataType] = dataType match {
    case BooleanType => Some(types.BooleanType())
    case StringType => Some(types.StringType())
    case ByteType => Some(types.ByteType())
    case ShortType => Some(types.ShortType())
    case IntegerType => Some(types.IntegerType())
    case LongType => Some(types.LongType())
    case FloatType => Some(types.FloatType())
    case DoubleType => Some(types.DoubleType())
    case vt:VectorUDT => Some(types.ListType(types.DoubleType()))
    case at: ArrayType =>  Some(types.ListType(mleapType(at.elementType).get))
    case _ => throw new RuntimeException(s"unsupported data type: $dataType")
  }

  implicit def fieldType(name: String, dataset: Option[DataFrame]): Option[bundle.DataType.DataType] = {
    dataset match {
      case Some(_) => {
          val schema = dataset.get.schema
          val index = schema.fieldIndex(name)
          Some(mleapTypeToBundleType(mleapType(schema.fields.apply(index).dataType).get))
      }
      case None => None
    }
  }
}
object TypeConverters extends TypeConverters
