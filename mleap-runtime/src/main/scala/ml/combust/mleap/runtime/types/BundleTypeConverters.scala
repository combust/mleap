package ml.combust.mleap.runtime.types

import ml.bundle

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 1/15/17.
  */
trait BundleTypeConverters {
  implicit def bundleBasicTypeToMleapBasicType(b: bundle.BasicType.BasicType): BasicType = {
    if(b.isBoolean) {
      BooleanType()
    } else if(b.isString) {
      StringType()
    } else if(b.isByte) {
      ByteType()
    } else if(b.isShort) {
      ShortType()
    } else if(b.isInt) {
      IntegerType()
    } else if(b.isLong) {
      LongType()
    } else if(b.isFloat) {
      FloatType()
    } else if(b.isDouble) {
      DoubleType()
    } else { throw new IllegalArgumentException(s"unsupported data type $b") }
  }

  implicit def bundleTypeToMleapType(bt: bundle.DataType.DataType): DataType = {
    val u = bt.underlying
    if(u.isBasic) {
      bundleBasicTypeToMleapBasicType(bt.getBasic)
    } else if(u.isList) {
      ListType(base = bundleTypeToMleapType(bt.getList.getBase))
    } else if(u.isTensor) {
      TensorType(bundleBasicTypeToMleapBasicType(bt.getTensor.base))
    } else { throw new IllegalArgumentException(s"unsupported data type $bt") }
  }

  implicit def mleapBasicTypeToBundleBasicType(b: BasicType): bundle.BasicType.BasicType = b match {
    case BooleanType(false) => bundle.BasicType.BasicType.BOOLEAN
    case StringType(false) => bundle.BasicType.BasicType.STRING
    case ByteType(false) => bundle.BasicType.BasicType.BYTE
    case ShortType(false) => bundle.BasicType.BasicType.SHORT
    case IntegerType(false) => bundle.BasicType.BasicType.INT
    case LongType(false) => bundle.BasicType.BasicType.LONG
    case FloatType(false) => bundle.BasicType.BasicType.FLOAT
    case DoubleType(false) => bundle.BasicType.BasicType.DOUBLE
  }

  implicit def mleapTypeToBundleType(bt: DataType): bundle.DataType.DataType = bt match {
    case basic: BasicType =>
      bundle.DataType.DataType(bundle.DataType.DataType.Underlying.Basic(mleapBasicTypeToBundleBasicType(basic)))
    case lt: ListType =>
      bundle.DataType.DataType(
        bundle.DataType.DataType.Underlying.List(
          bundle.DataType.DataType.ListType(base = Some(mleapTypeToBundleType(lt.base)))))
    case tt: TensorType =>
      bundle.DataType.DataType(
        bundle.DataType.DataType.Underlying.Tensor(
          bundle.TensorType.TensorType(mleapBasicTypeToBundleBasicType(tt.base))))
    case _ => throw new IllegalArgumentException(s"unsupported mleap type $bt")
  }
}
object BundleTypeConverters extends BundleTypeConverters
