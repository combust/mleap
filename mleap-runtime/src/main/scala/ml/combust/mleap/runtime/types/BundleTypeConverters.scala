package ml.combust.mleap.runtime.types

import ml.bundle
import ml.bundle.{DataShapeType, TensorDimension}
import ml.combust.mleap.core.types._

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 1/15/17.
  */
trait BundleTypeConverters {
  implicit def bundleToMleapBasicType(b: bundle.BasicType): BasicType = {
    b match {
      case bundle.BasicType.BOOLEAN => BooleanType()
      case bundle.BasicType.BYTE => ByteType()
      case bundle.BasicType.SHORT => ShortType()
      case bundle.BasicType.INT => IntegerType()
      case bundle.BasicType.LONG => LongType()
      case bundle.BasicType.FLOAT => FloatType()
      case bundle.BasicType.DOUBLE => DoubleType()
      case bundle.BasicType.STRING => StringType()
      case bundle.BasicType.BYTE_STRING => ByteStringType()
      case _ => throw new IllegalArgumentException(s"unsupported data type $b")
    }
  }

  implicit def mleapToBundleBasicType(b: BasicType): bundle.BasicType = b match {
    case BooleanType(false) => bundle.BasicType.BOOLEAN
    case ByteType(false) => bundle.BasicType.BYTE
    case ShortType(false) => bundle.BasicType.SHORT
    case IntegerType(false) => bundle.BasicType.INT
    case LongType(false) => bundle.BasicType.LONG
    case FloatType(false) => bundle.BasicType.FLOAT
    case DoubleType(false) => bundle.BasicType.DOUBLE
    case StringType(false) => bundle.BasicType.STRING
    case ByteStringType(false) => bundle.BasicType.BYTE_STRING
    case _ => throw new IllegalArgumentException(s"unsupported type $b")
  }

  implicit def bundleToMleapShape(s: bundle.DataShape): DataShape = {
    s.base match {
      case DataShapeType.SCALAR => ScalarShape(isNullable = s.isNullable)
      case DataShapeType.LIST => ListShape(isNullable = s.isNullable)
      case DataShapeType.TENSOR => TensorShape(s.tensorShape.get.dimensions.map(_.size), isNullable = s.isNullable)
      case _ => throw new IllegalArgumentException(s"unsupported shape $s")
    }
  }

  implicit def mleapToBundleShape(s: DataShape): bundle.DataShape = {
    s match {
      case ScalarShape(isNullable) => bundle.DataShape(base = DataShapeType.SCALAR, isNullable = isNullable)
      case ListShape(isNullable) => bundle.DataShape(base = DataShapeType.LIST, isNullable = isNullable)
      case TensorShape(dimensions, isNullable) =>
        bundle.DataShape(base = DataShapeType.TENSOR,
          isNullable = isNullable,
          tensorShape = Some(ml.bundle.TensorShape(dimensions.map(s => TensorDimension(s)))))
    }
  }
}
object BundleTypeConverters extends BundleTypeConverters
