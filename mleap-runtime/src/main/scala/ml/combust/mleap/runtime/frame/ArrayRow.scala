package ml.combust.mleap.runtime.frame

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by hollinwilkins on 10/5/17.
  */
object ArrayRow {
  def apply(values: Seq[Any]): ArrayRow = new ArrayRow(mutable.WrappedArray.make[Any](values.toArray))
}

/** Class for holding Row values in an array.
  *
  * @param values array of values in row
  */
case class ArrayRow(values: mutable.WrappedArray[Any]) extends Row {
  def this(values: java.lang.Iterable[Any]) = this(values.asScala.toArray)

  override def getRaw(index: Int): Any = values(index)

  override def iterator: Iterator[Any] = values.iterator

  override def withValue(value: Any): ArrayRow = ArrayRow(values :+ value)
  override def withValues(values: Seq[Any]): ArrayRow = ArrayRow(this.values ++ values)

  override def selectIndices(indices: Int *): ArrayRow = ArrayRow(indices.toArray.map(values).toSeq)

  override def dropIndices(indices: Int *): ArrayRow = {
    val drops = Set(indices: _*)
    val newValues = values.zipWithIndex.filter {
      case (_, i) => !drops.contains(i)
    }.map(_._1)

    ArrayRow(newValues)
  }

  def set(index: Int, value: Any): ArrayRow = {
    values(index) = value
    this
  }

  override def hashCode(): Int = {
    values.foldLeft(0) {
      (hash, value) => hash * 13 + value.hashCode()
    }
  }
}