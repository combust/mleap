package ml.combust.mleap.runtime.frame

import ml.combust.mleap.runtime.frame.Row.RowSelector
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.{ByteString, Tensor}

import scala.collection.JavaConverters._

/** Companion object for creating default rows.
  */
object Row {
  type RowSelector = (Row) => Any

  /** Create a row using the default implementation [[ArrayRow]].
    *
    * @param values values in the row
    * @return default row implementation with values
    */
  def apply(values: Any *): Row = ArrayRow(values.toArray)
}

/** Base trait for row data.
  */
trait Row extends Iterable[Any] {
  /** Get value at index.
    *
    * Alias for [[Row#get]]
    *
    * @param index index of value
    * @return value at index
    */
  def apply(index: Int): Any = get(index)

  /** Get value at index.
    *
    * Does not perform null checks
    *
    * @param index index of value
    * @return value at index
    */
  def getRaw(index: Int): Any

  /** Get value at index.
    *
    * Performs a null check
    *
    * @param index index of value
    * @return value at index
    */
  def get(index: Int): Any = {
    Option(getRaw(index)).getOrElse {
      throw new NullPointerException(s"value at $index is null")
    }
  }

  /** Get optional value at index.
    *
    * @param index index of value
    * @return optional value at index
    */
  def option(index: Int): Option[Any] = Option(getRaw(index))

  /** Get value at index as specified type.
    *
    * @param index index of value
    * @tparam T type of value
    * @return value at index cast to given type
    */
  def getAs[T](index: Int): T = get(index).asInstanceOf[T]

  /** Get value at index as specified type.
    *
    * @param index index of value
    * @tparam T type of value
    * @return optional value at index cast to given type
    */
  def optionAs[T](index: Int): Option[T] = option(index).asInstanceOf[Option[T]]

  /** Get value at index as a boolean.
    *
    * @param index index of value
    * @return boolean value
    */
  def getBool(index: Int): Boolean = getAs[Boolean](index)

  /** Get value at index as a byte.
    *
    * @param index index of value
    * @return byte value
    */
  def getByte(index: Int): Byte = getAs[Byte](index)

  /** Get value at index as a short.
    *
    * @param index index of value
    * @return short value
    */
  def getShort(index: Int): Short = getAs[Short](index)

  /** Get value at index as a byte string.
    *
    * @param index index of value
    * @return byte string value
    */
  def getByteString(index: Int): ByteString = getAs[ByteString](index)

  /** Get value at index as a float.
    *
    * @param index index of value
    * @return float value
    */
  def getFloat(index: Int): Float = getAs[Float](index)

  /** Get value at index as a double.
    *
    * @param index index of value
    * @return double value
    */
  def getDouble(index: Int): Double = getAs[Double](index)

  /** Get value at index as a double.
    *
    * @param index index of value
    * @return optional double value
    */
  def optionDouble(index: Int): Option[Double] = optionAs[Double](index)

  /** Get value at index as an int.
    *
    * @param index index of value
    * @return int value
    */
  def getInt(index: Int): Int = get(index).asInstanceOf[Int]

  /** Get value at index as an int.
    *
    * @param index index of value
    * @return optional int value
    */
  def optionInt(index: Int): Option[Int] = optionAs[Int](index)

  /** Get value at index as a long.
    *
    * @param index index of value
    * @return long value
    */
  def getLong(index: Int): Long = get(index).asInstanceOf[Long]

  /** Get value at index as a long.
    *
    * @param index index of value
    * @return optional long value
    */
  def optionLong(index: Int): Option[Long] = optionAs[Long](index)

  /** Get value at index as a string.
    *
    * @param index index of value
    * @return string value
    */
  def getString(index: Int): String = get(index).asInstanceOf[String]

  /** Get value at index as a string.
    *
    * @param index index of value
    * @return optional string value
    */
  def optionString(index: Int): Option[String] = optionAs[String](index)

  /** Get value at index as a tensor.
    *
    * @param index index of value
    * @return tensor value
    */
  def getTensor[T](index: Int): Tensor[T] = get(index).asInstanceOf[Tensor[T]]

  /** Get value at index as a tensor.
    *
    * @param index index of value
    * @return optional tensor value
    */
  def optionTensor[T](index: Int): Option[Tensor[T]] = optionAs[Tensor[T]](index)

  /** Get value at index as an array.
    *
    * @param index index of value
    * @tparam T inner type of the array
    * @return seq value
    */
  def getList[T](index: Int): java.util.List[T] = getSeq[T](index).asJava

  /** Get value at index as an array.
    *
    * @param index index of value
    * @tparam T inner type of the array
    * @return seq value
    */
  def getSeq[T](index: Int): Seq[T] = get(index).asInstanceOf[Seq[T]]

  /** Get value at index as an array.
    *
    * @param index index of value
    * @tparam T inner type of the array
    * @return optional seq value
    */
  def optionSeq[T](index: Int): Option[Seq[T]] = optionAs[Seq[T]](index)

  /** Get value at index as a map.
    *
    * @param index index of value
    * @tparam K type of the map keys
    * @tparam V type of the map values
    * @return map value
    */
  def getMap[K, V](index: Int): Map[K, V] = get(index) match {
    case m: java.util.Map[_, _] => m.asScala.toMap.asInstanceOf[Map[K, V]]
    case m: Map[_, _] => m.asInstanceOf[Map[K, V]]
    case o => throw new IllegalArgumentException(s"Index $index is not a map type, $o")
  }

  /** Get value at index as a map.
    *
    * @param index index of value
    * @tparam K type of the map keys
    * @tparam V type of the map values
    * @return optional map value
    */
  def optionMap[K, V](index: Int): Option[Map[K, V]] = optionAs[Map[K, V]](index)

  /** Convert row to a seq of values.
    *
    * @return seq of values from row
    */
  def toSeq: Seq[Any]

  /** Add value to row with a user defined function.
    *
    * @param selectors row selectors to generate inputs to function
    * @param udf user defined function to call
    * @return row with calculated value added
    */
  def withValue(selectors: RowSelector *)(udf: UserDefinedFunction): Row = {
    withValue(udfValue(selectors: _*)(udf))
  }

  def withValues(selectors: RowSelector *)(udf: UserDefinedFunction): Row = {
    udfValue(selectors: _*)(udf) match {
      case r: Row => withValues(r.toSeq)
      case p: Product => withValues(p.productIterator.toSeq)
      case _ => throw new IllegalArgumentException("Output of udf must be a Row or Product for multiple outputs")
    }
  }

  def shouldFilter(selectors: RowSelector *)(udf: UserDefinedFunction): Boolean = {
    udfValue(selectors: _*)(udf).asInstanceOf[Boolean]
  }

  def udfValue(selectors: RowSelector *)(udf: UserDefinedFunction): Any = {
    udf.inputs.length match {
      case 0 =>
        udf.f.asInstanceOf[() => Any]()
      case 1 =>
        udf.f.asInstanceOf[(Any) => Any](selectors.head(this))
      case 2 =>
        udf.f.asInstanceOf[(Any, Any) => Any](selectors.head(this), selectors(1)(this))
      case 3 =>
        udf.f.asInstanceOf[(Any, Any, Any) => Any](selectors.head(this), selectors(1)(this), selectors(2)(this))
      case 4 =>
        udf.f.asInstanceOf[(Any, Any, Any, Any) => Any](selectors.head(this), selectors(1)(this), selectors(2)(this), selectors(3)(this))
      case 5 =>
        udf.f.asInstanceOf[(Any, Any, Any, Any, Any) => Any](selectors.head(this), selectors(1)(this), selectors(2)(this), selectors(3)(this), selectors(4)(this))
    }
  }

  /** Add a value to the row.
    *
    * @param value value to add
    * @return row with the new value
    */
  def withValue(value: Any): Row

  def withValues(values: Seq[Any]): Row

  /** Create a new row from specified indices.
    *
    * @param indices indices to create new row with
    * @return new row with only selected values
    */
  def selectIndices(indices: Int *): Row

  /** Drop value at specified index.
    *
    * @param indices indices of values to drop
    * @return new row without specified value
    */
  def dropIndices(indices: Int *): Row

  override def equals(obj: scala.Any): Boolean = obj match {
    case obj: Row => iterator sameElements obj.iterator
    case _ => false
  }

  override def toString: String  = s"Row(${mkString(",")})"
}
