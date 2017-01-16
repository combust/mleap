package ml.combust.bundle.dsl

import java.nio.ByteBuffer

import com.google.protobuf.ByteString
import ml.bundle.BasicType.BasicType
import ml.bundle.DataType.DataType
import ml.bundle.DataType.DataType.ListType
import ml.bundle.Tensor.Tensor
import ml.bundle.TensorType.TensorType
import ml.bundle.Value.Value.ListValue
import ml.bundle.Value.{Value => BValue}
import ml.combust.bundle.HasBundleRegistry
import ml.combust.bundle.tensor.TensorSerializer
import ml.combust.mleap

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
    * @param tt bundle type of tensor
    * @param tensor bundle tensor
    * @return mleap tensor
    */
  def fromTensorValue(tt: TensorType, tensor: Tensor): mleap.tensor.Tensor[_] = {
    TensorSerializer.fromProto(tt, tensor)
  }

  /** Convert a [[ml.bundle.Value.Value.ListValue]] into a Scala Seq.
    *
    * Lists can be of any depth, and so can be decoded to Seq[Seq[Seq[String\]\]\]
    * for example.
    *
    * @param lt list type to extract to a Scala value
    * @param list protobuf list to extract to Scala value
    * @param hr bundle registry for custom types
    * @return Scala value of protobuf object
    */
  def fromListValue(lt: ListType, list: ListValue)
                   (implicit hr: HasBundleRegistry): Any = {
    val base = lt.base.get
    val u = base.underlying
    if(u.isList) {
      list.list.map(l => fromListValue(base.getList, l))
    } else if(u.isBasic) {
      base.getBasic match {
        case BasicType.BOOLEAN => list.b
        case BasicType.STRING => list.s
        case BasicType.BYTE => list.i.map(_.toByte)
        case BasicType.SHORT => list.i.map(_.toShort)
        case BasicType.INT => list.i.map(_.toInt)
        case BasicType.LONG => list.i
        case BasicType.FLOAT => list.f.map(_.toFloat)
        case BasicType.DOUBLE => list.f
        case _ => throw new IllegalArgumentException("unsupported list type")
      }
    } else if(u.isTensor) {
      list.tensor.map(t => fromTensorValue(base.getTensor, t))
    } else if(u.isCustom) {
      val ct = hr.bundleRegistry.custom(base.getCustom)
      list.custom.map(b => ct.fromBytes(b.toByteArray))
    } else if(u.isDt) {
      list.`type`
    } else { throw new IllegalArgumentException("unsupported list type") }
  }

  /** Convert a [[ml.bundle.Value.Value]] into a [[ml.combust.bundle.dsl.Value]].
    *
    * @param dataType data type of the protobuf value
    * @param value protobuf value to convert
    * @param hr bundle registry for custom types
    * @return wrapped Scala value of protobuf object
    */
  def fromBundle(dataType: DataType, value: ml.bundle.Value.Value)
                (implicit hr: HasBundleRegistry): Value = {
    val v = if(dataType.underlying.isCustom) {
      hr.bundleRegistry.custom(dataType.getCustom).fromBytes(value.getCustom.toByteArray)
    } else if(dataType.underlying.isTensor) {
      fromTensorValue(dataType.getTensor, value.getTensor)
    } else if(dataType.underlying.isBasic) {
      dataType.getBasic match {
        case BasicType.STRING => value.getS
        case BasicType.BOOLEAN => value.getB
        case BasicType.BYTE => value.getI.toByte
        case BasicType.SHORT => value.getI.toShort
        case BasicType.INT => value.getI.toInt
        case BasicType.LONG => value.getI
        case BasicType.FLOAT => value.getF.toFloat
        case BasicType.DOUBLE => value.getF
        case _ => throw new IllegalArgumentException("unsupported basic type")
      }
    } else if(dataType.underlying.isList) {
      fromListValue(dataType.getList, value.getList)
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
    * @param lt list type
    * @param value Scala list
    * @param hr bundle registry for custom types
    * @return list protobuf object of the Scala list
    */
  def listValue(lt: ListType, value: Seq[_])
               (implicit hr: HasBundleRegistry): ListValue = {
    val base = lt.base.get
    val u = base.underlying
    if(u.isBasic) {
      base.getBasic match {
        case BasicType.STRING => ListValue(s = value.asInstanceOf[Seq[String]])
        case BasicType.BOOLEAN => ListValue(b = value.asInstanceOf[Seq[Boolean]])
        case BasicType.BYTE => ListValue(i = value.asInstanceOf[Seq[Byte]].map(_.toLong))
        case BasicType.SHORT => ListValue(i = value.asInstanceOf[Seq[Short]].map(_.toLong))
        case BasicType.INT => ListValue(i = value.asInstanceOf[Seq[Int]].map(_.toLong))
        case BasicType.LONG => ListValue(i = value.asInstanceOf[Seq[Long]])
        case BasicType.FLOAT => ListValue(f = value.asInstanceOf[Seq[Float]].map(_.toDouble))
        case BasicType.DOUBLE => ListValue(f = value.asInstanceOf[Seq[Double]])
        case _ => throw new IllegalArgumentException("unsupported basic type")
      }
    } else if(u.isCustom) {
      val ct = hr.bundleRegistry.custom[Any](base.getCustom)
      val custom = value.map(a => ByteString.copyFrom(ct.toBytes(a)))
      ListValue(custom = custom)
    } else if(u.isTensor) {
      ListValue(tensor = value.map(_.asInstanceOf[mleap.tensor.Tensor[_]]).map(tensorValue))
    } else if(u.isList) {
      val lb = base.getList
      val list = value.map(a => listValue(lb, a.asInstanceOf[Seq[_]]))
      ListValue(list = list)
    } else if(u.isDt) {
      ListValue(`type` = value.asInstanceOf[Seq[DataType]])
    } else { throw new IllegalArgumentException("unsupported data type") }
  }

  /** Converts a Scala value to a protobuf value.
    *
    * @param dataType type of Scala value
    * @param value Scala value to convert
    * @param hr bundle registry for custom types
    * @return protobuf value of the Scala value
    */
  def bundleValue(dataType: DataType, value: Any)
                 (implicit hr: HasBundleRegistry): BValue = {
    val u = dataType.underlying
    val v = if(u.isTensor) {
      BValue.V.Tensor(tensorValue(value.asInstanceOf[mleap.tensor.Tensor[_]]))
    } else if(u.isBasic) {
      dataType.getBasic match {
        case BasicType.BOOLEAN => BValue.V.B(value.asInstanceOf[Boolean])
        case BasicType.STRING => BValue.V.S(value.asInstanceOf[String])
        case BasicType.BYTE => BValue.V.I(value.asInstanceOf[Long].toByte)
        case BasicType.SHORT => BValue.V.I(value.asInstanceOf[Long].toShort)
        case BasicType.INT => BValue.V.I(value.asInstanceOf[Long].toInt)
        case BasicType.LONG => BValue.V.I(value.asInstanceOf[Long])
        case BasicType.FLOAT => BValue.V.F(value.asInstanceOf[Double].toFloat)
        case BasicType.DOUBLE => BValue.V.F(value.asInstanceOf[Double])
        case _ => throw new IllegalArgumentException(s"unsupported basic type ${dataType.getBasic}")
      }
    } else if(u.isList) {
      BValue.V.List(listValue(dataType.getList, value.asInstanceOf[Seq[_]]))
    } else if(u.isCustom) {
      val ct = hr.bundleRegistry.customForObj[Any](value)
      BValue.V.Custom(ByteString.copyFrom(ct.toBytes(value)))
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
  def basicDataType(basic: BasicType): DataType = DataType(DataType.Underlying.Basic(basic))

  /** Create a list data type.
    *
    * Use [[ml.combust.bundle.dsl.Value.listDataTypeN]] for nested lists.
    *
    * @param base type of values held within list
    * @return list data type for the base type
    */
  def listDataType(base: DataType): DataType = DataType(DataType.Underlying.List(DataType.ListType(Some(base))))

  /** Create a tensor data type.
    *
    * @param basic basic data type of the tensor
    * @return tensor data type
    */
  def tensorDataType(basic: BasicType): DataType = DataType(DataType.Underlying.Tensor(TensorType(basic)))

  val booleanDataType: DataType = basicDataType(BasicType.BOOLEAN)
  val stringDataType: DataType = basicDataType(BasicType.STRING)
  val byteDataType: DataType = basicDataType(BasicType.BYTE)
  val shortDataType: DataType = basicDataType(BasicType.SHORT)
  val intDataType: DataType = basicDataType(BasicType.INT)
  val longDataType: DataType = basicDataType(BasicType.LONG)
  val floatDataType: DataType = basicDataType(BasicType.FLOAT)
  val doubleDataType: DataType = basicDataType(BasicType.DOUBLE)

  val stringListDataType: DataType = listDataType(stringDataType)
  val booleanListDataType: DataType = listDataType(booleanDataType)
  val byteListDataType: DataType = listDataType(byteDataType)
  val shortListDataType: DataType = listDataType(shortDataType)
  val intListDataType: DataType = listDataType(intDataType)
  val longListDataType: DataType = listDataType(longDataType)
  val floatListDataType: DataType = listDataType(floatDataType)
  val doubleListDataType: DataType = listDataType(doubleDataType)

  val dataTypeDataType: DataType = DataType(DataType.Underlying.Dt(true))
  val listDataTypeDataType: DataType = {
    DataType(DataType.Underlying.List(DataType.ListType(Some(DataType(DataType.Underlying.Dt(true))))))
  }

  /** Create a nested list data type of any depth.
    *
    * To represent a Seq[Seq[Seq[Long\]\]\] from Scala,
    * pass in a longDataType for base and n = 3.
    *
    * @param base data type of the nested list
    * @param n depth of the nested list
    * @return data type for a nested list
    */
  def listDataTypeN(base: DataType, n: Int): DataType = n match {
    case 1 => DataType(DataType.Underlying.List(DataType.ListType(Some(base))))
    case _ => DataType(DataType.Underlying.List(DataType.ListType(Some(listDataTypeN(base, n - 1)))))
  }

  /** Create a custom data type.
    *
    * Custom data types are used to store arbitrary Scala objects.
    *
    * @param hr bundle registry for custom value serializer
    * @tparam T Scala class of the custom value
    * @return data type for the custom value
    */
  def customDataType[T: ClassTag](implicit hr: HasBundleRegistry): DataType = {
    val name = hr.bundleRegistry.customForClass[T].name
    DataType(DataType.Underlying.Custom(name))
  }

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

  /** Create a custom value.
    *
    * @param value value to wrap
    * @param hr bundle registry for custom value serializer
    * @tparam T Scala class of the custom value
    * @return wrapped custom value
    */
  def custom[T: ClassTag](value: T)
                         (implicit hr: HasBundleRegistry): Value = {
    Value(customDataType[T], value)
  }

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

  /** Create a data type value.
    *
    * @param dt data type to store
    * @return value with data type
    */
  def dataType(dt: DataType): Value = {
    Value(dataTypeDataType, dt)
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

  /** Create a list of custom objects value.
    *
    * @param value Scala list of custom objects
    * @param hr bundle registry for constructing custom data type
    * @tparam T Scala class of the custom values
    * @return wrapped custom list
    */
  def customList[T: ClassTag](value: Seq[T])
                             (implicit hr: HasBundleRegistry): Value = {
    val base = customDataType[T]
    val lt = DataType(DataType.Underlying.List(DataType.ListType(Some(base))))
    Value(lt, value)
  }

  /** Create a list of tensors.
    *
    * Dimensions of a tensor indicate how many values are in each dimension.
    * Each dimension must be a positive integer. Use -1 if the dimension
    * can have any number of values in it.
    *
    * @param tensors tensors in the list
    * @return wrapped list of tensors
    */
  def tensorList[T: ClassTag](tensors: Seq[mleap.tensor.Tensor[T]]): Value = {
    Value(listDataType(tensorDataType(TensorSerializer.toBundleType(mleap.tensor.Tensor.tensorType[T]))), tensors)
  }

  /** Create a list of data types.
    *
    * @param dts data types
    * @return value with data types
    */
  def dataTypeList(dts: Seq[DataType]): Value = {
    Value(listDataTypeDataType, dts)
  }

  /** Construct a nested list of any value.
    *
    * For custom values use [[Value.customListN]]
    * Example to construct a nested list of Double.
    *
    * {{{
    * scala> import ml.bundle.BasicType.BasicType
    *        import ml.bundle.dsl._
    *        val list: Seq[Seq[Seq[Double]]] = Seq(Seq(
    *          Seq(45.6, 77.8, 34.5),
    *          Seq(2.3, 5.6, 44.5)
    *        ))
    * scala> val value = Value.listN(Value.doubleDataType, 3, list)
    * value: ml.bundle.dsl.Value = Value(DataType(List(ListType(DataType(List(ListType(DataType(List(ListType(DataType(Basic(DOUBLE))))))))))),List(List(List(45.6, 77.8, 34.5), List(2.3, 5.6, 44.5))))
    * }}}
    *
    * @param base data type of the list
    * @param n depth of the nested list
    * @param value Scala nested list with base values in it
    * @return wrapped nested list
    */
  def listN(base: DataType, n: Int, value: Seq[_]): Value = Value(listDataTypeN(base, n), value)

  /** Construct a nested list of custom values.
    *
    * See [[Value.listN]] for example usage.
    *
    * @param n depth of nesting
    * @param value Scala nested list of custom objects
    * @param hr bundle registry needed for custom data type
    * @tparam T Scala class of the custom data
    * @return wrapped nested list of custom values
    */
  def customListN[T: ClassTag](n: Int, value: Seq[_])
                              (implicit hr: HasBundleRegistry): Value = {
    val base = customDataType[T]
    listN(base, n, value)
  }
}

/** This class is used to wrap Scala objects for later serialization into Bundle.ML
  *
  * @param bundleDataType data type of the value being stored
  * @param value Scala object that will be serialized later
  */
case class Value(bundleDataType: DataType, value: Any) {
  def asBundle(implicit hr: HasBundleRegistry): ml.bundle.Value.Value = {
    Value.bundleValue(bundleDataType, value)
  }

  /** Whether or not this value has a small serialization size.
    *
    * See [[isLarge]] for usage documentation.
    * This method will always return the inverse of [[isLarge]].
    *
    * @param hr bundle registry needed for custom values
    * @return true if the value has a small serialization size, false otherwise
    */
  def isSmall(implicit hr: HasBundleRegistry): Boolean = !isLarge

  /** Whether or not this value has a large serialization size.
    *
    * This method is used in [[ml.combust.bundle.serializer.SerializationFormat.Mixed]] mode
    * serialization to determine whether to serialize the value as JSON or as Protobuf.
    *
    * @param hr bundle registry needed for custom values
    * @return true if the value has a large serialization size, false otherwise
    */
  def isLarge(implicit hr: HasBundleRegistry): Boolean = {
    bundleDataType.underlying.isTensor && getTensor[Any].rawSize > 1024 ||
      bundleDataType.underlying.isList && !(bundleDataType.getList.base.get.underlying.isBasic || bundleDataType.getList.base.get.underlying.isDt) ||
      bundleDataType.underlying.isCustom && hr.bundleRegistry.custom[Any](bundleDataType.getCustom).isLarge(value)
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

  /** Get value as a Scala object.
    *
    * @tparam T type of the custom value
    * @return custom Scala object
    */
  def getCustom[T]: T = value.asInstanceOf[T]

  /** Get value as a tensor.
    *
    * @tparam T base type of tensor Double or String
    * @return Scala seq of tensor values.
    */
  def getTensor[T]: mleap.tensor.Tensor[T] = value.asInstanceOf[mleap.tensor.Tensor[T]]

  /** Get value as a data type.
    *
    * @return data type
    */
  def getDataType: DataType = value.asInstanceOf[DataType]

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

  /** Get list of custom Scala objects.
    *
    * @tparam T data type of objects
    * @return list of custom objects
    */
  def getCustomList[T]: Seq[T] = value.asInstanceOf[Seq[T]]

  /** Get list of tensors.
    *
    * @tparam T Scala class of tensors Double or String
    * @return list of tensors
    */
  def getTensorList[T]: Seq[mleap.tensor.Tensor[T]] = value.asInstanceOf[Seq[mleap.tensor.Tensor[T]]]

  /** Get list of data types.
    *
    * @return list of data types
    */
  def getDataTypeList: Seq[DataType] = value.asInstanceOf[Seq[DataType]]

  /** Get nested list of any type.
    *
    * @return nested list of Scala objects
    */
  def getListN: Seq[_] = value.asInstanceOf[Seq[_]]
}
