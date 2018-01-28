package ml.combust.bundle.v07.dsl

import com.google.protobuf
import ml.bundle.{BasicType, DataShape, DataType, List, Scalar, Tensor}
import ml.combust.bundle.v07.tensor.TensorSerializer
import ml.combust.mleap.tensor.ByteString
import ml.combust.mleap

import scala.reflect.ClassTag

/** Provides a set of helper methods for easily creating
  * [[ml.combust.bundle.v07.dsl.Value]] objects.
  *
  * Easily create [[ml.combust.bundle.v07.dsl.Value]] objects of any
  * type using the helper methods provided here. The helper
  * methods will wrap a Scala value for later serializing
  * into Bundle.ML.
  *
  * Also provides several helper methods for converting
  * from Bundle.ML protobuf objects back into wrapped
  * Scala objects.
  */
object Value {
  /** Convert [[ml.combust.mleap.tensor.Tensor]] to [[ml.bundle.Tensor]].
    *
    * @param tensor mleap tensor
    * @return bundle tensor
    */
  def tensorValue(tensor: mleap.tensor.Tensor[_]): Tensor = {
    TensorSerializer.toProto(tensor)
  }

  def scalarValue(scalar: Scalar): Value = {
    Value(ml.bundle.Value(ml.bundle.Value.V.S(scalar)))
  }

  /** Create a boolean value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def boolean(value: Boolean): Value = scalarValue(Scalar(b = value))

  /** Create an byte value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def byte(value: Byte): Value = scalarValue(Scalar(i = value))

  /** Create an short value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def short(value: Short): Value = scalarValue(Scalar(i = value))

  /** Create an int value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def int(value: Int): Value = scalarValue(Scalar(i = value))

  /** Create a long value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def long(value: Long): Value = scalarValue(Scalar(l = value))

  /** Create a float value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def float(value: Float): Value = scalarValue(Scalar(f = value))

  /** Create a double value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def double(value: Double): Value = scalarValue(Scalar(d = value))

  /** Create a string value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def string(value: String): Value = scalarValue(Scalar(s = value))

  /** Create a byte string value.
    *
    * @param value value to wrap
    * @return wrapped value
    */
  def byteString(value: ByteString): Value = scalarValue(Scalar(bs = protobuf.ByteString.copyFrom(value.bytes)))

  /** Create a tensor value.
    *
    * Dimensions must have size greater than 0. Use -1 for
    * dimensions that can be any size.
    *
    * @param tensor tensor value
    * @return tensor value
    */
  def tensor[T](tensor: mleap.tensor.Tensor[T]): Value = {
    Value(ml.bundle.Value(ml.bundle.Value.V.T(TensorSerializer.toProto(tensor))))
  }

  /** Create a data type value.
    *
    * @param bt data type to store
    * @return value with data type
    */
  def basicType(bt: BasicType): Value = {
    scalarValue(Scalar(bt = bt))
  }

  /** Create a data shape value.
    *
    * @param ds data shape to store
    * @return value with data shape
    */
  def dataShape(ds: DataShape): Value = {
    scalarValue(Scalar(ds = Some(ds)))
  }

  /** Create a model value.
    *
    * @param value data shape to store
    * @return value with model
    */
  def model(value: Model): Value = {
    scalarValue(Scalar(m = Some(value.asBundle)))
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

  def listValue(list: List): Value = {
    Value(ml.bundle.Value(ml.bundle.Value.V.L(list)))
  }

  /** Create a list of booleans value.
    *
    * @param value Scala boolean list
    * @return wrapped value
    */
  def booleanList(value: Seq[Boolean]): Value = listValue(List(b = value))

  /** Create a list of byte value.
    *
    * @param value Scala byte list
    * @return wrapped value
    */
  def byteList(value: Seq[Byte]): Value = listValue(List(i = value.map(_.toInt)))

  /** Create a list of short value.
    *
    * @param value Scala short list
    * @return wrapped value
    */
  def shortList(value: Seq[Short]): Value = listValue(List(i = value.map(_.toInt)))

  /** Create a list of int value.
    *
    * @param value Scala int list
    * @return wrapped value
    */
  def intList(value: Seq[Int]): Value = listValue(List(i = value))

  /** Create a list of longs value.
    *
    * @param value Scala long list
    * @return wrapped value
    */
  def longList(value: Seq[Long]): Value = listValue(List(l = value))

  /** Create a list of float value.
    *
    * @param value Scala float list
    * @return wrapped value
    */
  def floatList(value: Seq[Float]): Value = listValue(List(f = value))

  /** Create a list of doubles value.
    *
    * @param value Scala double list
    * @return wrapped value
    */
  def doubleList(value: Seq[Double]): Value = listValue(List(d = value))

  /** Create a list of strings value.
    *
    * @param value Scala string list
    * @return wrapped value
    */
  def stringList(value: Seq[String]): Value = listValue(List(s = value))

  /** Create a list of byte string.
    *
    * @param value byte strings
    * @return value with byte strings
    */
  def byteStringList(value: Seq[ByteString]): Value = {
    listValue(List(bs = value.map(bs => protobuf.ByteString.copyFrom(bs.bytes))))
  }

  /** Create a list of tensors.
    *
    * @param value tensors
    * @return value with tensors
    */
  def tensorList[T](value: Seq[mleap.tensor.Tensor[T]]): Value = {
    listValue(List(t = value.map(TensorSerializer.toProto)))
  }

  /** Create a list of data types.
    *
    * @param value data types
    * @return value with data types
    */
  def basicTypeList(value: Seq[BasicType]): Value = listValue(List(bt = value))

  /** Create a list of data shapes.
    *
    * @param value data shapes
    * @return value with data shapes
    */
  def dataShapeList(value: Seq[DataShape]): Value = listValue(List(ds = value))

  /** Create a list of models value.
    *
    * @param value list of models
    * @return value with list of models
    */
  def modelList(value: Seq[Model]): Value = {
    listValue(List(m = value.map(_.asBundle)))
  }
}

/** This class is used to wrap Scala objects for later serialization into Bundle.ML
  *
  * @param value bundle protobuf value
  */
case class Value(value: ml.bundle.Value) {
  /** Get value as a boolean.
    *
    * @return boolean value
    */
  def getBoolean: Boolean = value.getS.b

  /** Get value as a byte.
    *
    * @return byte value
    */
  def getByte: Byte = value.getS.i.toByte

  /** Get value as a short.
    *
    * @return short value
    */
  def getShort: Short = value.getS.i.toShort

  /** Get value as an int.
    *
    * @return int value
    */
  def getInt: Int = value.getS.i

  /** Get value as a long.
    *
    * @return long value
    */
  def getLong: Long = value.getS.l

  /** Get value as a float.
    *
    * @return float value
    */
  def getFloat: Float = value.getS.f

  /** Get value as a double.
    *
    * @return double value
    */
  def getDouble: Double = value.getS.d

  /** Get value as a tensor.
    *
    * @tparam T base type of tensor Double or String
    * @return Scala seq of tensor values.
    */
  def getTensor[T]: mleap.tensor.Tensor[T] = {
    TensorSerializer.fromProto[T](value.getT)
  }

  /** Get value as a string.
    *
    * @return string value
    */
  def getString: String = value.getS.s

  /** Get value as a byte string.
    *
    * @return byte string
    */
  def getByteString: ByteString = ByteString(value.getS.bs.toByteArray)

  /** Get value as a data type.
    *
    * @return data type
    */
  def getBasicType: BasicType = value.getS.bt

  /** Get value as a data shape.
    *
    * @return data shape
    */
  def getDataShape: DataShape = value.getS.ds.get

  /** Get value as a model.
    *
    * @return model
    */
  def getModel: Model = Model.fromBundle(value.getS.m.get)

  /** Get list of booleans.
    *
    * @return list of booleans
    */
  def getBooleanList: Seq[Boolean] = value.getL.b

  /** Get list of bytes.
    *
    * @return list of bytes
    */
  def getByteList: Seq[Byte] = value.getL.i.map(_.toByte)

  /** Get list of shorts.
    *
    * @return list of shorts
    */
  def getShortList: Seq[Short] = value.getL.i.map(_.toShort)

  /** Get list of ints.
    *
    * @return list of ints
    */
  def getIntList: Seq[Int] = value.getL.i

  /** Get list of longs.
    *
    * @return list of longs
    */
  def getLongList: Seq[Long] = value.getL.l

  /** Get list of floats.
    *
    * @return list of floats
    */
  def getFloatList: Seq[Float] = value.getL.f

  /** Get list of doubles.
    *
    * @return list of doubles
    */
  def getDoubleList: Seq[Double] = value.getL.d

  /** Get list of strings.
    *
    * @return list of strings
    */
  def getStringList: Seq[String] = value.getL.s

  /** Get list of byte strings.
    *
    * @return list of byte strings
    */
  def getByteStringList: Seq[ByteString] = value.getL.bs.map(bs => ByteString(bs.toByteArray))

  /** Get list of tensors.
    *
    * @tparam T Scala class of tensors Double or String
    * @return list of tensors
    */
  def getTensorList[T]: Seq[mleap.tensor.Tensor[T]] = value.getL.t.map(TensorSerializer.fromProto[T])

  /** Get list of data types.
    *
    * @return list of data types
    */
  def getBasicTypeList: Seq[BasicType] = value.getL.bt

  /** Get list of data shapes.
    *
    * @return list of data shapes
    */
  def getDataShapeList: Seq[DataShape] = value.getL.ds

  /** Get model list.
    *
    * @return list of models
    */
  def getModelList: Seq[Model] = value.getL.m.map(Model.fromBundle)
}
