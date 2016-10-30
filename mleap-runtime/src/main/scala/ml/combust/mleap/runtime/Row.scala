package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.Row.RowSelector
import ml.combust.mleap.runtime.function.UserDefinedFunction
import org.apache.spark.ml.linalg.Vector

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
trait Row {
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
    * @param index index of value
    * @return value at index
    */
  def get(index: Int): Any

  /** Get value at index as specified type.
    *
    * @param index index of value
    * @tparam T type of value
    * @return value at index cast to given type
    */
  def getAs[T](index: Int): T = get(index).asInstanceOf[T]

  /** Get value at index as a double.
    *
    * @param index index of value
    * @return double value
    */
  def getDouble(index: Int): Double = get(index).asInstanceOf[Double]

  /** Get value at index as an int.
    *
    * @param index index of value
    * @return int value
    */
  def getInt(index: Int): Int = get(index).asInstanceOf[Int]

  /** Get value at index as a long.
    *
    * @param index index of value
    * @return long value
    */
  def getLong(index: Int): Long = get(index).asInstanceOf[Long]

  /** Get value at index as a string.
    *
    * @param index index of value
    * @return string value
    */
  def getString(index: Int): String = get(index).asInstanceOf[String]

  /** Get value at index as a vector.
    *
    * @param index index of value
    * @return vector value
    */
  def getVector(index: Int): Vector = get(index).asInstanceOf[Vector]

  /** Get value at index as an array.
    *
    * @param index index of value
    * @tparam T inner type of the array
    * @return array value
    */
  def getArray[T](index: Int): Array[T] = get(index).asInstanceOf[Array[T]]

  /** Convert row to an array of values.
    *
    * @return array of values from row
    */
  def toArray: Array[Any]

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
    udf.inputs.length match {
      case 0 =>
        val f = udf.f.asInstanceOf[() => Any]
        withValue(f())
      case 1 =>
        val f = udf.f.asInstanceOf[(Any) => Any]
        withValue(f(selectors.head(this)))
      case 2 =>
        val f = udf.f.asInstanceOf[(Any, Any) => Any]
        withValue(f(selectors.head(this), selectors(1)(this)))
      case 3 =>
        val f = udf.f.asInstanceOf[(Any, Any, Any) => Any]
        withValue(f(selectors.head(this), selectors(1)(this), selectors(2)(this)))
      case 4 =>
        val f = udf.f.asInstanceOf[(Any, Any, Any, Any) => Any]
        withValue(f(selectors.head(this), selectors(1)(this), selectors(2)(this), selectors(3)(this)))
      case 5 =>
        val f = udf.f.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
        withValue(f(selectors.head(this), selectors(1)(this), selectors(2)(this), selectors(3)(this), selectors(4)(this)))
    }
  }

  /** Add a value to the row.
    *
    * @param value value to add
    * @return row with the new value
    */
  def withValue(value: Any): Row

  /** Create a new row from specified indices.
    *
    * @param indices indices to create new row with
    * @return new row with only selected values
    */
  def selectIndices(indices: Int *): Row

  /** Drop value at specified index.
    *
    * @param index index of value to drop
    * @return new row without specified value
    */
  def dropIndex(index: Int): Row

  override def toString: String  = s"Row(${mkString(",")})"

  def mkString: String = toArray.mkString
  def mkString(sep: String): String = toArray.mkString(sep)
  def mkString(start: String, sep: String, end: String): String = toArray.mkString(start, sep, end)
}

/** Class for holding Row values in an array.
  *
  * @param values array of values in row
  */
case class ArrayRow(values: Array[Any]) extends Row {
  override def get(index: Int): Any = values(index)

  override def toSeq: Seq[Any] = values.toSeq
  override def toArray: Array[Any] = values

  override def withValue(value: Any): Row = ArrayRow(values :+ value)

  override def selectIndices(indices: Int*): Row = ArrayRow(indices.toArray.map(values))
  override def dropIndex(index: Int): Row = ArrayRow(values.take(index) ++ values.drop(index + 1))
}

/** Companion object for creating a SeqRow.
  */
object SeqRow {
  def apply(values: Seq[Any]): SeqRow = new SeqRow(values.reverse)
}

/** Class for holding Row values in a Seq.
  *
  * @param values seq values in the row
  */
class SeqRow private(values: Seq[Any]) extends Row {
  override def toArray: Array[Any] = values.reverse.toArray
  override def toSeq: Seq[Any] = values.reverse

  override def get(index: Int): Any = values(realIndex(index))

  override def selectIndices(indices: Int *): SeqRow = SeqRow(indices.map(index => values(realIndex(index))))

  override def withValue(value: Any): Row = new SeqRow(value +: values)

  override def dropIndex(index: Int): Row = {
    val i = realIndex(index)
    new SeqRow(values.take(i) ++ values.drop(i + 1))
  }

  private def realIndex(index: Int): Int = values.length - index - 1
}