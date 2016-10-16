package ml.combust.mleap.core.feature

/**
  * Created by mikhail on 9/29/16.
  */
case class NGramModel(
                n: Int
                ) extends Serializable{

  def apply(value: Array[String]): Array[String] = {
    value.iterator.sliding(n).withPartial(false).map(_.mkString(" ")).toArray
  }
}
