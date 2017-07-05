package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model

/**
  * Created by mikhail on 9/29/16.
  */
case class NGramModel(n: Int) extends Model{
  def apply(value: Seq[String]): Seq[String] = {
    value.iterator.sliding(n).withPartial(false).map(_.mkString(" ")).toSeq
  }
}
