package ml.combust.bundle.v07.converter

import com.google.protobuf.ByteString
import ml.combust.bundle.dsl._
import ml.bundle.{List, Scalar, Value => BValue, v07 => bv07}
import ml.combust.bundle.v07

/**
  * Created by hollinwilkins on 1/27/18.
  */
trait ModelConverter {
  def convert(model: v07.dsl.Model): Model

  def convertAttributes(attrs: Option[v07.dsl.AttributeList]): Attributes = attrs match {
    case Some(attributes) =>
      val newAttributes = attributes.lookup.map {
        case (name, attr) => (name, convertAttr(attr))
      }

      Attributes(newAttributes)
    case None => Attributes()
  }

  def convertAttr(attr: v07.dsl.Attribute): Value = {
    val dt = attr.value.bundleDataType

    if (dt.underlying.isBasic) {
      dt.getBasic match {
        case bv07.BasicType.BOOLEAN => Value(BValue(BValue.V.S(Scalar(b = attr.value.getBoolean))))
        case bv07.BasicType.BYTE => Value(BValue(BValue.V.S(Scalar(i = attr.value.getByte))))
        case bv07.BasicType.SHORT => Value(BValue(BValue.V.S(Scalar(i = attr.value.getShort))))
        case bv07.BasicType.INT => Value(BValue(BValue.V.S(Scalar(i = attr.value.getInt))))
        case bv07.BasicType.LONG => Value(BValue(BValue.V.S(Scalar(l = attr.value.getLong))))
        case bv07.BasicType.FLOAT => Value(BValue(BValue.V.S(Scalar(f = attr.value.getFloat))))
        case bv07.BasicType.DOUBLE => Value(BValue(BValue.V.S(Scalar(d = attr.value.getDouble))))
        case bv07.BasicType.STRING => Value(BValue(BValue.V.S(Scalar(s = attr.value.getString))))
        case bv07.BasicType.BYTE_STRING => Value(BValue(BValue.V.S(Scalar(bs = ByteString.copyFrom(attr.value.getByteString.bytes)))))
        case _ => throw new IllegalArgumentException(s"invalid basic value, cannot convert with value ${attr.value}")
      }
    } else if (dt.underlying.isList) {
      val base = dt.getList.base.get
      if (!base.underlying.isBasic) { throw new IllegalArgumentException("can only convert 1-d lists") }

      base.getBasic match {
        case bv07.BasicType.BOOLEAN => Value(BValue(BValue.V.L(List(b = attr.value.getBooleanList))))
        case bv07.BasicType.BYTE => Value(BValue(BValue.V.L(List(i = attr.value.getByteList.map(_.toInt)))))
        case bv07.BasicType.SHORT => Value(BValue(BValue.V.L(List(i = attr.value.getShortList.map(_.toInt)))))
        case bv07.BasicType.INT => Value(BValue(BValue.V.L(List(i = attr.value.getIntList))))
        case bv07.BasicType.LONG => Value(BValue(BValue.V.L(List(l = attr.value.getLongList))))
        case bv07.BasicType.FLOAT => Value(BValue(BValue.V.L(List(f = attr.value.getFloatList))))
        case bv07.BasicType.DOUBLE => Value(BValue(BValue.V.L(List(d = attr.value.getDoubleList))))
        case bv07.BasicType.STRING => Value(BValue(BValue.V.L(List(s = attr.value.getStringList))))
        case bv07.BasicType.BYTE_STRING => Value(BValue(BValue.V.L(List(bs = attr.value.getByteStringList.map(bs => ByteString.copyFrom(bs.bytes))))))
        case _ => throw new IllegalArgumentException(s"invalid list value, cannot convert with value ${attr.value}")
      }
    } else if (dt.underlying.isTensor) {
      dt.getTensor.base match {
        case bv07.BasicType.BOOLEAN => Value.tensor(attr.value.getTensor[Boolean])
        case bv07.BasicType.BYTE => Value.tensor(attr.value.getTensor[Byte])
        case bv07.BasicType.SHORT => Value.tensor(attr.value.getTensor[Short])
        case bv07.BasicType.INT => Value.tensor(attr.value.getTensor[Int])
        case bv07.BasicType.LONG => Value.tensor(attr.value.getTensor[Long])
        case bv07.BasicType.FLOAT => Value.tensor(attr.value.getTensor[Float])
        case bv07.BasicType.DOUBLE => Value.tensor(attr.value.getTensor[Double])
        case bv07.BasicType.STRING => Value.tensor(attr.value.getTensor[String])
        case _ => throw new IllegalArgumentException(s"invalid tensor value, cannot convert with value ${attr.value}")
      }
    } else {
      throw new IllegalArgumentException(s"invalid attribute, cannot convert with data type $dt")
    }
  }
}

class BaseModelConverter extends ModelConverter {
  override def convert(model: v07.dsl.Model): Model = {
    Model(model.op, convertAttributes(model.attributes))
  }
}
