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
      case bundle.BasicType.BOOLEAN => BasicType.Boolean
      case bundle.BasicType.BYTE => BasicType.Byte
      case bundle.BasicType.SHORT => BasicType.Short
      case bundle.BasicType.INT => BasicType.Int
      case bundle.BasicType.LONG => BasicType.Long
      case bundle.BasicType.FLOAT => BasicType.Float
      case bundle.BasicType.DOUBLE => BasicType.Double
      case bundle.BasicType.STRING => BasicType.String
      case bundle.BasicType.BYTE_STRING => BasicType.ByteString
      case _ => throw new IllegalArgumentException(s"unsupported data type $b")
    }
  }

  implicit def mleapToBundleBasicType(b: BasicType): bundle.BasicType = b match {
    case BasicType.Boolean => bundle.BasicType.BOOLEAN
    case BasicType.Byte => bundle.BasicType.BYTE
    case BasicType.Short => bundle.BasicType.SHORT
    case BasicType.Int => bundle.BasicType.INT
    case BasicType.Long => bundle.BasicType.LONG
    case BasicType.Float => bundle.BasicType.FLOAT
    case BasicType.Double => bundle.BasicType.DOUBLE
    case BasicType.String => bundle.BasicType.STRING
    case BasicType.ByteString => bundle.BasicType.BYTE_STRING
    case _ => throw new IllegalArgumentException(s"unsupported type $b")
  }

  implicit def bundleToMleapShape(s: bundle.DataShape): DataShape = {
    s.base match {
      case DataShapeType.SCALAR => ScalarShape(isNullable = s.isNullable)
      case DataShapeType.LIST => ListShape(isNullable = s.isNullable)
      case DataShapeType.TENSOR => TensorShape(dimensions = s.tensorShape.map(_.dimensions.map(_.size)),
        isNullable = s.isNullable)
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
          tensorShape = dimensions.map(_.map(s => TensorDimension(s))).map(ml.bundle.TensorShape.apply))
    }
  }

  implicit def mleapToBundleDataType(dt: DataType): bundle.DataType = bundle.DataType(dt.base, Some(dt.shape))
  implicit def bundleToMleapDataType(dt: bundle.DataType): DataType = DataType(dt.base, dt.shape.get)

  implicit def mleapToBundleField(field: StructField): bundle.Field = bundle.Field(field.name, Some(field.dataType))
  implicit def bundleToMleapField(field: bundle.Field): StructField = StructField(field.name, field.dataType.get)

  implicit def mleapToBundleSocket(socket: Socket): bundle.Socket = bundle.Socket(socket.port, socket.name, Some(socket.dataType))
  implicit def bundleToMleapSocket(socket: bundle.Socket): Socket = Socket(socket.port, socket.name, socket.dataType.get)

  implicit def mleapToBundleNodeShape(shape: NodeShape): bundle.NodeShape = bundle.NodeShape(shape.inputs.values.map(mleapToBundleSocket).toSeq,
    shape.outputs.values.map(mleapToBundleSocket).toSeq)
  implicit def bundleToMleapNodeShape(shape: bundle.NodeShape): NodeShape = NodeShape(shape.inputs.map(bundleToMleapSocket),
    shape.outputs.map(bundleToMleapSocket))
}
object BundleTypeConverters extends BundleTypeConverters
