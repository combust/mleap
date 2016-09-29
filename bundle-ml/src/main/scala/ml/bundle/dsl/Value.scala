package ml.bundle.dsl

import com.google.protobuf.ByteString
import ml.bundle.BasicType.BasicType
import ml.bundle.DataType.DataType
import ml.bundle.DataType.DataType.ListType
import ml.bundle.Tensor.Tensor
import ml.bundle.TensorType.TensorType
import ml.bundle.Value.Value.ListValue
import ml.bundle.serializer.{HasBundleRegistry, SerializationContext}
import ml.bundle.Value.{Value => BValue}

import scala.reflect.{ClassTag, classTag}

/** Provides a set of helper methods for easily creating
  * [[ml.bundle.dsl.Value]] objects.
  *
  * Easily create [[ml.bundle.dsl.Value]] objects of any
  * type using the helper methods provided here. The helper
  * methods will wrap a Scala value for later serializing
  * into Bundle.ML.
  *
  * Also provides several helper methods for converting
  * from Bundle.ML protobuf objects back into wrapped
  * Scala objects.
  */
object Value {
  /** Convert a [[ml.bundle.Tensor.Tensor]] into a Scala object
    * representing the tensor.
    *
    * @param base base type of the tensor
    * @param tensor tensor to convert
    * @return Scala value of protobuf object
    */
  def fromTensorValue(base: BasicType, tensor: Tensor): Any = base match {
    case BasicType.DOUBLE => tensor.doubleVal
    case BasicType.STRING => tensor.stringVal
    case _ => throw new Error("unsupported tensor type")
  }

  /** Convert a [[ml.bundle.Value.Value.ListValue]] into a Scala Seq.
    *
    * Lists can be of any depth, and so can be decoded to Seq[Seq[Seq[String\]\]\]
    * for example.
    *
    * @param lt list type to extract to a Scala value
    * @param list protobuf list to extract to Scala value
    * @param context serialization context for decoding custom values
    * @return Scala value of protobuf object
    */
  def fromListValue(lt: ListType, list: ListValue)
                   (implicit context: SerializationContext): Any = {
    val base = lt.base.get
    val u = base.underlying
    if(u.isList) {
      list.list.map(l => fromListValue(base.getList, l))
    } else if(u.isBasic) {
      base.getBasic match {
        case BasicType.STRING => list.s
        case BasicType.DOUBLE => list.f
        case BasicType.BOOLEAN => list.b
        case BasicType.LONG => list.i
        case _ => throw new Error("unsupported list type")
      }
    } else if(u.isTensor) {
      val basic = base.getTensor.base
      list.tensor.map(t => fromTensorValue(basic, t))
    } else if(u.isCustom) {
      val serializer = context.bundleRegistry.custom(base.getCustom).formatSerializer
      list.custom.map(b => serializer.fromBytes(b.toByteArray))
    } else { throw new Error("unsupported list type") }
  }

  /** Convert a [[ml.bundle.Value.Value]] into a [[ml.bundle.dsl.Value]].
    *
    * @param dataType data type of the protobuf value
    * @param value protobuf value to convert
    * @param context serialization context for decoding custom values
    * @return wrapped Scala value of protobuf object
    */
  def fromBundle(dataType: DataType, value: ml.bundle.Value.Value)
           (implicit context: SerializationContext): Value = {
    val v = if(dataType.underlying.isCustom) {
      context.bundleRegistry.custom(dataType.getCustom).formatSerializer.fromBytes(value.getCustom.toByteArray)
    } else if(dataType.underlying.isTensor) {
      fromTensorValue(dataType.getTensor.base, value.getTensor)
    } else if(dataType.underlying.isBasic) {
      dataType.getBasic match {
        case BasicType.STRING => value.getS
        case BasicType.DOUBLE => value.getF
        case BasicType.BOOLEAN => value.getB
        case BasicType.LONG => value.getI
        case _ => throw new Error("unsupported basic type")
      }
    } else if(dataType.underlying.isList) {
      fromListValue(dataType.getList, value.getList)
    } else { throw new Error("unsupported value type") }

    Value(dataType, v)
  }

  /** Create a tensor value.
    *
    * value should be a Seq[Double] or Seq[String] depending on the base
    *
    * @param base basic type of the tensor
    * @param value Scala object holding the tensor data
    * @return tensor protobuf object of the Scala tensor
    */
  def tensorValue(base: BasicType, value: Any): Tensor = base match {
    case BasicType.DOUBLE => Tensor(doubleVal = value.asInstanceOf[Seq[Double]])
    case BasicType.STRING => Tensor(stringVal = value.asInstanceOf[Seq[String]])
    case _ => throw new Error("unsupported vector base type")
  }

  /** Create a list value.
    *
    * value can be a nested list, as long as lt is also nested.
    * For example, lt can be a list of list of double then value
    * should be a Seq[Seq[Double]].
    *
    * @param lt list type
    * @param value Scala list
    * @param context serialization context for encoding custom values
    * @return list protobuf object of the Scala list
    */
  def listValue(lt: ListType, value: Seq[_])
               (implicit context: SerializationContext): ListValue = {
    val base = lt.base.get
    val u = base.underlying
    if(u.isBasic) {
      base.getBasic match {
        case BasicType.STRING => ListValue(s = value.asInstanceOf[Seq[String]])
        case BasicType.LONG => ListValue(i = value.asInstanceOf[Seq[Long]])
        case BasicType.BOOLEAN => ListValue(b = value.asInstanceOf[Seq[Boolean]])
        case BasicType.DOUBLE => ListValue(f = value.asInstanceOf[Seq[Double]])
        case _ => throw new Error("unsupported basic type")
      }
    } else if(u.isCustom) {
      val ct = context.bundleRegistry.custom[Any](base.getCustom)
      val serializer = ct.formatSerializer
      val custom = value.map(a => ByteString.copyFrom(serializer.toBytes(a)))
      ListValue(custom = custom)
    } else if(u.isTensor) {
      val tb = base.getTensor.base
      val tensor = value.map(a => tensorValue(tb, a))
      ListValue(tensor = tensor)
    } else if(u.isList) {
      val lb = base.getList
      val list = value.map(a => listValue(lb, a.asInstanceOf[Seq[_]]))
      ListValue(list = list)
    } else { throw new Error("unsupported data type") }
  }

  /** Converts a Scala value to a protobuf value.
    *
    * @param dataType type of Scala value
    * @param value Scala value to convert
    * @param context serialization context for encoding custom values
    * @return protobuf value of the Scala value
    */
  def bundleValue(dataType: DataType, value: Any)
                 (implicit context: SerializationContext): BValue = {
    val u = dataType.underlying
    val v = if(u.isTensor) {
      BValue.V.Tensor(tensorValue(dataType.getTensor.base, value))
    } else if(u.isBasic) {
      dataType.getBasic match {
        case BasicType.STRING => BValue.V.S(value.asInstanceOf[String])
        case BasicType.LONG => BValue.V.I(value.asInstanceOf[Long])
        case BasicType.BOOLEAN => BValue.V.B(value.asInstanceOf[Boolean])
        case BasicType.DOUBLE => BValue.V.F(value.asInstanceOf[Double])
        case _ => throw new Error("unsupported basic type")
      }
    } else if(u.isList) {
      BValue.V.List(listValue(dataType.getList, value.asInstanceOf[Seq[_]]))
    } else if(u.isCustom) {
      val ct = context.bundleRegistry.customForObj[Any](value)
      BValue.V.Custom(ByteString.copyFrom(ct.formatSerializer.toBytes(value)))
    } else { throw new Error("unsupported data type") }

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
    * Use [[ml.bundle.dsl.Value.listDataTypeN]] for nested lists.
    *
    * @param base type of values held within list
    * @return list data type for the base type
    */
  def listDataType(base: DataType): DataType = DataType(DataType.Underlying.List(DataType.ListType(Some(base))))

  /** Create a tensor data type.
    *
    * Supported basic types are double and string.
    *
    * Dimensions must be a positive integer for how many values are contained
    * in each of the tensor dimensions. -1 can be used if a dimension can
    * be of arbitrary size.
    *
    * @param basic basic data type of the tensor
    * @param dims size of each dimension of the tensor
    * @return tensor data type
    */
  def tensorDataType(basic: BasicType, dims: Seq[Int]): DataType = DataType(DataType.Underlying.Tensor(TensorType(basic, dims)))

  val stringDataType: DataType = basicDataType(BasicType.STRING)
  val doubleDataType: DataType = basicDataType(BasicType.DOUBLE)
  val booleanDataType: DataType = basicDataType(BasicType.BOOLEAN)
  val longDataType: DataType = basicDataType(BasicType.LONG)

  val stringListDataType: DataType = listDataType(stringDataType)
  val doubleListDataType: DataType = listDataType(doubleDataType)
  val booleanListDataType: DataType = listDataType(booleanDataType)
  val longListDataType: DataType = listDataType(longDataType)

  val doubleVectorDataType: DataType = tensorDataType(BasicType.DOUBLE, Seq(-1))
  val stringVectorDataType: DataType = tensorDataType(BasicType.STRING, Seq(-1))

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

  /** Create a double value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def double(value: Double): Value = Value(doubleDataType, value)

  /** Create a boolean value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def boolean(value: Boolean): Value = Value(booleanDataType, value)

  /** Create a long value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def long(value: Long): Value = Value(longDataType, value)

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
    * @param value tensor data
    * @param dims dimensional data for tensor
    * @tparam T Scala class of underlying tensor data Double or String
    * @return tensor value
    */
  def tensor[T: ClassTag](value: Seq[T], dims: Seq[Int]): Value = {
    val (basic, tensor) = classTag[T].runtimeClass.getName match {
      case "Double" => (BasicType.DOUBLE, Tensor(doubleVal = value.asInstanceOf[Seq[Double]]))
      case "String" => (BasicType.STRING, Tensor(stringVal = value.asInstanceOf[Seq[String]]))
      case _ => throw new Error("unsupported vector type")
    }
    Value(tensorDataType(basic, dims), tensor)
  }

  /** Create a list of strings value.
    *
    * @param value Scala string list
    * @return wrapped value
    */
  def stringList(value: Seq[String]): Value = Value(stringListDataType, value)

  /** Create a list of doubles value.
    *
    * @param value Scala double list
    * @return wrapped value
    */
  def doubleList(value: Seq[Double]): Value = Value(doubleListDataType, value)

  /** Create a list of booleans value.
    *
    * @param value Scala boolean list
    * @return wrapped value
    */
  def booleanList(value: Seq[Boolean]): Value = Value(booleanListDataType, value)

  /** Create a list of longs value.
    *
    * @param value Scala long list
    * @return wrapped value
    */
  def longList(value: Seq[Long]): Value = Value(longListDataType, value)

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
    * @param value Scala list of tensor data
    * @param dims dimensions of the tensor data
    * @tparam T base type of the tensor, Double or String
    * @return wrapped list of tensors
    */
  def tensorList[T: ClassTag](value: Seq[Seq[T]], dims: Seq[Int]): Value = {
    val basic = classTag[T].runtimeClass.getName match {
      case "Double" => BasicType.DOUBLE
      case "String" => BasicType.STRING
      case _ => throw new Error("unsupported vector type")
    }
    Value(listDataType(tensorDataType(basic, dims)), value)
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

  /** Create a double vector.
    *
    * A double vector is a type of tensor with 1 dimension.
    *
    * @param value Scala vector data
    * @return wrapped double vector
    */
  def doubleVector(value: Seq[Double]): Value = Value(doubleVectorDataType, value)

  /** Create a string vector.
    *
    * A string vector is a type of tensor with 1 dimension.
    *
    * @param value Scala vector data
    * @return wrapped string vector
    */
  def stringVector(value: Seq[String]): Value = Value(stringVectorDataType, value)
}

/** This class is used to wrap Scala objects for later serialization into Bundle.ML
  *
  * @param _bundleDataType data type of the value being stored
  * @param value Scala object that will be serialized later
  */
case class Value(private val _bundleDataType: DataType, value: Any) {
  /** Create the protobuf value used for serialization.
    *
    * @param context serialization context needed for encoding custom values
    * @return protobuf value
    */
  def bundleValue(implicit context: SerializationContext): BValue = {
    Value.bundleValue(_bundleDataType, value)
  }

  /** Get the protobuf data type of the value.
    *
    * @return protobuf data type
    */
  def bundleDataType: DataType = _bundleDataType

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
    * This method is used in [[ml.bundle.serializer.SerializationFormat.Mixed]] mode
    * serialization to determine whether to serialize the value as JSON or as Protobuf.
    *
    * @param hr bundle registry needed for custom values
    * @return true if the value has a large serialization size, false otherwise
    */
  def isLarge(implicit hr: HasBundleRegistry): Boolean = {
    _bundleDataType.underlying.isTensor && getTensor[Any].length > 1024 ||
    _bundleDataType.underlying.isList && !_bundleDataType.getList.base.get.underlying.isBasic ||
    _bundleDataType.underlying.isCustom && hr.bundleRegistry.custom[Any](_bundleDataType.getCustom).isLarge(value)
  }

  /** Get value as a string.
    *
    * @return string value
    */
  def getString: String = value.asInstanceOf[String]

  /** Get value as a long.
    *
    * @return long value
    */
  def getLong: Long = value.asInstanceOf[Long]

  /** Get value as a double.
    *
    * @return double value
    */
  def getDouble: Double = value.asInstanceOf[Double]

  /** Get value as a boolean.
    *
    * @return boolean value
    */
  def getBoolean: Boolean = value.asInstanceOf[Boolean]

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
  def getTensor[T]: Seq[T] = value.asInstanceOf[Seq[T]]

  /** Get value as seq of doubles.
    *
    * @return double tensor values
    */
  def getDoubleVector: Seq[Double] = value.asInstanceOf[Seq[Double]]

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
  def getTensorList[T]: Seq[Seq[T]] = value.asInstanceOf[Seq[Seq[T]]]

  /** Get nested list of any type.
    *
    * @return nested list of Scala objects
    */
  def getListN: Seq[_] = value.asInstanceOf[Seq[_]]
}
