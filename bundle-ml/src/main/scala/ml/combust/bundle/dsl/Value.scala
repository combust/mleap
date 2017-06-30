package ml.combust.bundle.dsl

import ml.bundle.BasicType.BasicType
import ml.bundle.DataShape.{BaseDataShape, DataShape}
import ml.bundle.DataType.DataType
import ml.bundle.Tensor.Tensor
import ml.bundle.Value.Value.ListValue
import ml.bundle.Value.{Value => BValue}
import ml.combust.bundle.tensor.TensorSerializer
import ml.combust.{bundle, mleap}

import scala.reflect.ClassTag

/** Provides a set of helper methods for easily creating
  * [[ml.combust.bundle.dsl.Value]] objects.
  *
  * Easily create [[ml.combust.bundle.dsl.Value]] objects of any
  * type using the helper methods provided here. The helper
  * methods will wrap a Scala value for later serializing
  * into Bundle.ML.
  *
  * Also provides several helper methods for converting
  * from Bundle.ML protobuf objects back into wrapped
  * Scala objects.
  */
object Value {
  /** Convert from a [[ml.bundle.Tensor.Tensor]] to a [[ml.combust.mleap.tensor.Tensor]].
    *
    * @param base base type of the tensor
    * @param tensor bundle tensor
    * @return mleap tensor
    */
  def fromTensorValue(base: BasicType, tensor: Tensor): mleap.tensor.Tensor[_] = {
    TensorSerializer.fromProto(base, tensor)
  }

  /** Convert a [[ml.bundle.Value.Value.ListValue]] into a Scala Seq.
    *
    * Lists can be only of depth 1.
    *
    * @param base base type of the list
    * @param list protobuf list to extract to Scala value
    * @return Scala value of protobuf object
    */
  def fromListValue(base: BasicType, list: ListValue): Any = {
    base match {
      case BasicType.BOOLEAN => list.b
      case BasicType.STRING => list.s
      case BasicType.BYTE => list.i.map(_.toByte)
      case BasicType.SHORT => list.i.map(_.toShort)
      case BasicType.INT => list.i.map(_.toInt)
      case BasicType.LONG => list.i
      case BasicType.FLOAT => list.f.map(_.toFloat)
      case BasicType.DOUBLE => list.f
      case BasicType.BYTE_STRING => list.bs
      case BasicType.DATA_TYPE => list.`type`
      case _ => throw new IllegalArgumentException("unsupported list type")
    }
  }

  /** Convert a [[ml.bundle.Value.Value]] into a [[ml.combust.bundle.dsl.Value]].
    *
    * @param dataType data type of the protobuf value
    * @param value protobuf value to convert
    * @return wrapped Scala value of protobuf object
    */
  def fromBundle(dataType: DataType, value: ml.bundle.Value.Value): Value = {
    val v = if(dataType.shape.get.base.isTensor) {
      fromTensorValue(dataType.base, value.getTensor)
    } else if(dataType.shape.get.base.isScalar) {
      dataType.base match {
        case BasicType.STRING => value.getS
        case BasicType.BOOLEAN => value.getB
        case BasicType.BYTE => value.getI.toByte
        case BasicType.SHORT => value.getI.toShort
        case BasicType.INT => value.getI.toInt
        case BasicType.LONG => value.getI
        case BasicType.FLOAT => value.getF.toFloat
        case BasicType.DOUBLE => value.getF
        case BasicType.BYTE_STRING => value.getBs
        case BasicType.DATA_TYPE => value.getType
        case _ => throw new IllegalArgumentException("unsupported basic type")
      }
    } else if(dataType.shape.get.base.isList) {
      fromListValue(dataType.base, value.getList)
    } else { throw new IllegalArgumentException("unsupported value type") }

    Value(dataType, v)
  }

  /** Convert [[ml.combust.mleap.tensor.Tensor]] to [[ml.bundle.Tensor.Tensor]].
    *
    * @param tensor mleap tensor
    * @return bundle tensor
    */
  def tensorValue(tensor: mleap.tensor.Tensor[_]): Tensor = {
    TensorSerializer.toProto(tensor)
  }

  /** Create a list value.
    *
    * value can be a nested list, as long as lt is also nested.
    * For example, lt can be a list of list of double then value
    * should be a Seq[Seq[Double]].
    *
    * @param base base type of list
    * @param value Scala list
    * @return list protobuf object of the Scala list
    */
  def listValue(base: BasicType, value: Seq[_]): ListValue = {
    base match {
      case BasicType.STRING => ListValue(s = value.asInstanceOf[Seq[String]])
      case BasicType.BOOLEAN => ListValue(b = value.asInstanceOf[Seq[Boolean]])
      case BasicType.BYTE => ListValue(i = value.asInstanceOf[Seq[Byte]].map(_.toLong))
      case BasicType.SHORT => ListValue(i = value.asInstanceOf[Seq[Short]].map(_.toLong))
      case BasicType.INT => ListValue(i = value.asInstanceOf[Seq[Int]].map(_.toLong))
      case BasicType.LONG => ListValue(i = value.asInstanceOf[Seq[Long]])
      case BasicType.FLOAT => ListValue(f = value.asInstanceOf[Seq[Float]].map(_.toDouble))
      case BasicType.DOUBLE => ListValue(f = value.asInstanceOf[Seq[Double]])
      case BasicType.BYTE_STRING => ListValue(bs = value.asInstanceOf[Seq[bundle.ByteString]].map(_.toProto))
      case BasicType.DATA_TYPE => ListValue(`type` = value.asInstanceOf[Seq[DataType]])
      case _ => throw new IllegalArgumentException("unsupported basic type")
    }
  }

  /** Converts a Scala value to a protobuf value.
    *
    * @param dataType type of Scala value
    * @param value Scala value to convert
    * @return protobuf value of the Scala value
    */
  def bundleValue(dataType: DataType, value: Any): BValue = {
    val v = if(dataType.shape.get.base.isTensor) {
      BValue.V.Tensor(tensorValue(value.asInstanceOf[mleap.tensor.Tensor[_]]))
    } else if(dataType.shape.get.base.isScalar) {
      dataType.base match {
        case BasicType.BOOLEAN => BValue.V.B(value.asInstanceOf[Boolean])
        case BasicType.STRING => BValue.V.S(value.asInstanceOf[String])
        case BasicType.BYTE => BValue.V.I(value.asInstanceOf[Long].toByte)
        case BasicType.SHORT => BValue.V.I(value.asInstanceOf[Long].toShort)
        case BasicType.INT => BValue.V.I(value.asInstanceOf[Long].toInt)
        case BasicType.LONG => BValue.V.I(value.asInstanceOf[Long])
        case BasicType.FLOAT => BValue.V.F(value.asInstanceOf[Double].toFloat)
        case BasicType.DOUBLE => BValue.V.F(value.asInstanceOf[Double])
        case BasicType.BYTE_STRING => BValue.V.Bs(value.asInstanceOf[bundle.ByteString].toProto)
        case BasicType.DATA_TYPE => BValue.V.Type(value.asInstanceOf[DataType])
        case _ => throw new IllegalArgumentException(s"unsupported basic type ${dataType.base}")
      }
    } else if(dataType.shape.get.base.isList) {
      BValue.V.List(listValue(dataType.base, value.asInstanceOf[Seq[_]]))
    } else { throw new IllegalArgumentException("unsupported data type") }

    BValue(v)
  }

  /** Create a basic data type.
    *
    * Basic data types are string, long, double, boolean.
    *
    * @param basic string, long, double, or boolean
    * @return data type for the basic type
    */
  def basicDataType(basic: BasicType): DataType = {
    DataType(base = basic,
      shape = Some(DataShape(base = BaseDataShape.SCALAR)))
  }

  /** Create a list data type.
    *
    * @param base type of values held within list
    * @return list data type for the base type
    */
  def listDataType(base: BasicType): DataType = {
    DataType(base = base,
      shape = Some(DataShape(base = BaseDataShape.LIST)))
  }

  /** Create a tensor data type.
    *
    * @param basic basic data type of the tensor
    * @return tensor data type
    */
  def tensorDataType(basic: BasicType): DataType = {
    DataType(base = basic,
      shape = Some(DataShape(base = BaseDataShape.TENSOR)))
  }

  val booleanDataType: DataType = basicDataType(BasicType.BOOLEAN)
  val stringDataType: DataType = basicDataType(BasicType.STRING)
  val byteDataType: DataType = basicDataType(BasicType.BYTE)
  val shortDataType: DataType = basicDataType(BasicType.SHORT)
  val intDataType: DataType = basicDataType(BasicType.INT)
  val longDataType: DataType = basicDataType(BasicType.LONG)
  val floatDataType: DataType = basicDataType(BasicType.FLOAT)
  val doubleDataType: DataType = basicDataType(BasicType.DOUBLE)
  val byteStringDataType: DataType = basicDataType(BasicType.BYTE_STRING)
  val dataTypeDataType: DataType = basicDataType(BasicType.DATA_TYPE)
  val dataShapeDataType: DataType = basicDataType(BasicType.DATA_SHAPE)
  val basicTypeDataType: DataType = basicDataType(BasicType.BASIC_TYPE)

  val stringListDataType: DataType = listDataType(BasicType.STRING)
  val booleanListDataType: DataType = listDataType(BasicType.BOOLEAN)
  val byteListDataType: DataType = listDataType(BasicType.BYTE)
  val shortListDataType: DataType = listDataType(BasicType.SHORT)
  val intListDataType: DataType = listDataType(BasicType.INT)
  val longListDataType: DataType = listDataType(BasicType.LONG)
  val floatListDataType: DataType = listDataType(BasicType.FLOAT)
  val doubleListDataType: DataType = listDataType(BasicType.DOUBLE)
  val byteStringListDataType: DataType = listDataType(BasicType.BYTE_STRING)
  val listDataTypeDataType: DataType = listDataType(BasicType.DATA_TYPE)
  val listDataShapeDataType: DataType = listDataType(BasicType.DATA_SHAPE)
  val listBasicTypeDataType: DataType = listDataType(BasicType.BASIC_TYPE)

  /** Create a string value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def string(value: String): Value = Value(stringDataType, value)

  /** Create a boolean value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def boolean(value: Boolean): Value = Value(booleanDataType, value)

  /** Create a byte value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def byte(value: Byte): Value = Value(byteDataType, value)

  /** Create a short value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def short(value: Short): Value = Value(shortDataType, value)

  /** Create an int value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def int(value: Int): Value = Value(intDataType, value)

  /** Create a long value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def long(value: Long): Value = Value(longDataType, value)

  /** Create a float value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def float(value: Float): Value = Value(floatDataType, value)

  /** Create a double value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def double(value: Double): Value = Value(doubleDataType, value)

  /** Create a tensor value.
    *
    * Dimensions must have size greater than 0. Use -1 for
    * dimensions that can be any size.
    *
    * @param tensor tensor value
    * @return tensor value
    */
  def tensor[T](tensor: mleap.tensor.Tensor[T]): Value = {
    Value(tensorDataType(TensorSerializer.toBundleType(tensor.base)), tensor)
  }

  /** Create a tensor value with 1 dimension.
    *
    * @param values values of vector
    * @tparam T type of vector
    * @return tensor with vector
    */
  def vector[T: ClassTag](values: Array[T]): Value = {
    tensor(mleap.tensor.Tensor.denseVector(values))
  }

  /** Create a byte string value.
    *
    * @param bs byte string
    * @return value with byte string
    */
  def byteString(bs: bundle.ByteString): Value = {
    Value(byteStringDataType, bs)
  }

  /** Create a data type value.
    *
    * @param dt data type to store
    * @return value with data type
    */
  def dataType(dt: DataType): Value = {
    Value(dataTypeDataType, dt)
  }

  /** Create a data shape value.
    *
    * @param ds data shape to store
    * @return value with data type
    */
  def dataShape(ds: DataShape): Value = {
    Value(dataShapeDataType, ds)
  }

  /** Create a basic type value.
    *
    * @param bt basic type to store
    * @return value with data type
    */
  def basicType(bt: BasicType): Value = {
    Value(basicTypeDataType, bt)
  }

  /** Create a list of booleans value.
    *
    * @param value Scala boolean list
    * @return wrapped value
    */
  def booleanList(value: Seq[Boolean]): Value = Value(booleanListDataType, value)

  /** Create a list of strings value.
    *
    * @param value Scala string list
    * @return wrapped value
    */
  def stringList(value: Seq[String]): Value = Value(stringListDataType, value)

  /** Create a list of byte value.
    *
    * @param value Scala byte list
    * @return wrapped value
    */
  def byteList(value: Seq[Byte]): Value = Value(byteListDataType, value)

  /** Create a list of short value.
    *
    * @param value Scala long list
    * @return wrapped value
    */
  def shortList(value: Seq[Short]): Value = Value(shortListDataType, value)

  /** Create a list of int value.
    *
    * @param value Scala int list
    * @return wrapped value
    */
  def intList(value: Seq[Int]): Value = Value(intListDataType, value)

  /** Create a list of longs value.
    *
    * @param value Scala long list
    * @return wrapped value
    */
  def longList(value: Seq[Long]): Value = Value(longListDataType, value)

  /** Create a list of float value.
    *
    * @param value Scala float list
    * @return wrapped value
    */
  def floatList(value: Seq[Float]): Value = Value(floatListDataType, value)

  /** Create a list of doubles value.
    *
    * @param value Scala double list
    * @return wrapped value
    */
  def doubleList(value: Seq[Double]): Value = Value(doubleListDataType, value)

  /** Create a list of byte string.
    *
    * @param bss data types
    * @return value with data types
    */
  def byteStringList(bss: Seq[bundle.ByteString]): Value = {
    Value(byteStringListDataType, bss)
  }

  /** Create a list of data types.
    *
    * @param dts data types
    * @return value with data types
    */
  def dataTypeList(dts: Seq[DataType]): Value = {
    Value(listDataTypeDataType, dts)
  }

  /** Create a list of data shapes.
    *
    * @param dss data shapes
    * @return value with data shapes
    */
  def dataShapeList(dss: Seq[DataShape]): Value = {
    Value(listDataShapeDataType, dss)
  }

  /** Create a list of basic types.
    *
    * @param bts basic types
    * @return value with basic types
    */
  def basicTypeList(bts: Seq[BasicType]): Value = {
    Value(listBasicTypeDataType, bts)
  }
}

/** This class is used to wrap Scala objects for later serialization into Bundle.ML
  *
  * @param bundleDataType data type of the value being stored
  * @param value Scala object that will be serialized later
  */
case class Value(bundleDataType: DataType, value: Any) {
  def asBundle: ml.bundle.Value.Value = {
    Value.bundleValue(bundleDataType, value)
  }

  /** Whether or not this value has a small serialization size.
    *
    * See [[isLarge]] for usage documentation.
    * This method will always return the inverse of [[isLarge]].
    *
    * @return true if the value has a small serialization size, false otherwise
    */
  def isSmall: Boolean = !isLarge

  /** Whether or not this value has a large serialization size.
    *
    * This method is used in [[ml.combust.bundle.serializer.SerializationFormat.Mixed]] mode
    * serialization to determine whether to serialize the value as JSON or as Protobuf.
    *
    * @return true if the value has a large serialization size, false otherwise
    */
  def isLarge: Boolean = {
    bundleDataType.shape.get.base.isTensor && getTensor[Any].rawSize > 1024 ||
      bundleDataType.shape.get.base.isList && !bundleDataType.shape.get.base.isScalar
  }

  /** Get value as a boolean.
    *
    * @return boolean value
    */
  def getBoolean: Boolean = value.asInstanceOf[Boolean]

  /** Get value as a string.
    *
    * @return string value
    */
  def getString: String = value.asInstanceOf[String]

  /** Get value as a byte.
    *
    * @return byte value
    */
  def getByte: Byte = value.asInstanceOf[Byte]

  /** Get value as a short.
    *
    * @return short value
    */
  def getShort: Short = value.asInstanceOf[Short]

  /** Get value as an int.
    *
    * @return int value
    */
  def getInt: Int = value.asInstanceOf[Int]

  /** Get value as a long.
    *
    * @return long value
    */
  def getLong: Long = value.asInstanceOf[Long]

  /** Get value as a float.
    *
    * @return float value
    */
  def getFloat: Float = value.asInstanceOf[Float]

  /** Get value as a double.
    *
    * @return double value
    */
  def getDouble: Double = value.asInstanceOf[Double]

  /** Get value as a tensor.
    *
    * @tparam T base type of tensor Double or String
    * @return Scala seq of tensor values.
    */
  def getTensor[T]: mleap.tensor.Tensor[T] = value.asInstanceOf[mleap.tensor.Tensor[T]]

  /** Get value as a byte string.
    *
    * @return byte string
    */
  def getByteString: bundle.ByteString = value.asInstanceOf[bundle.ByteString]

  /** Get value as a data type.
    *
    * @return data type
    */
  def getDataType: DataType = value.asInstanceOf[DataType]

  /** Get value as a data shape.
    *
    * @return data shape
    */
  def getDataShape: DataShape = value.asInstanceOf[DataShape]

  /** Get value as a basic type.
    *
    * @return basic type
    */
  def getBasicType: BasicType = value.asInstanceOf[BasicType]

  /** Get value as seq of strings.
    *
    * @return string tensor values
    */
  def getStringVector: Seq[String] = value.asInstanceOf[Seq[String]]

  /** Get list of strings.
    *
    * @return list of strings
    */
  def getStringList: Seq[String] = value.asInstanceOf[Seq[String]]

  /** Get list of ints.
    *
    * @return list of ints
    */
  def getIntList: Seq[Int] = value.asInstanceOf[Seq[Int]]

  /** Get list of longs.
    *
    * @return list of longs
    */
  def getLongList: Seq[Long] = value.asInstanceOf[Seq[Long]]

  /** Get list of doubles.
    *
    * @return list of doubles
    */
  def getDoubleList: Seq[Double] = value.asInstanceOf[Seq[Double]]

  /** Get list of booleans.
    *
    * @return list of booleans
    */
  def getBooleanList: Seq[Boolean] = value.asInstanceOf[Seq[Boolean]]

  /** Get list of tensors.
    *
    * @tparam T Scala class of tensors Double or String
    * @return list of tensors
    */
  def getTensorList[T]: Seq[mleap.tensor.Tensor[T]] = value.asInstanceOf[Seq[mleap.tensor.Tensor[T]]]

  /** Get list of byte strings.
    *
    * @return list of byte strings
    */
  def getByteStringList: Seq[bundle.ByteString] = value.asInstanceOf[Seq[bundle.ByteString]]

  /** Get list of data types.
    *
    * @return list of data types
    */
  def getDataTypeList: Seq[DataType] = value.asInstanceOf[Seq[DataType]]

  /** Get list of data shapes.
    *
    * @return list of data shapes
    */
  def getDataShapeList: Seq[DataShape] = value.asInstanceOf[Seq[DataShape]]

  /** Get list of basic types.
    *
    * @return list of basic types
    */
  def getBasicTypeList: Seq[BasicType] = value.asInstanceOf[Seq[BasicType]]

  /** Get nested list of any type.
    *
    * @return nested list of Scala objects
    */
  def getListN: Seq[_] = value.asInstanceOf[Seq[_]]
}
