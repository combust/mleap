package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{BasicType, ListType, StructType}

/**
  * Created by mikhail on 9/29/16.
  */
case class NGramModel(n: Int) extends Model{
  def apply(value: Seq[String]): Seq[String] = {
    value.iterator.sliding(n).withPartial(false).map(_.mkString(" ")).toSeq
  }

  override def inputSchema: StructType = StructType("input" -> ListType(BasicType.String)).get


  override def outputSchema: StructType = StructType("output" -> ListType(BasicType.String)).get
}
