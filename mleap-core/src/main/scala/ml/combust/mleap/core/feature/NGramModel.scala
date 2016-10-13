package ml.combust.mleap.core.feature

/**
  * Created by mikhail on 9/29/16.
  */
class NGramModel(
                n: Int
                ) extends Serializable{

  def apply(value: Seq[String]): Seq[String] = {
    value.iterator.sliding(n).withPartial(false).map(_.mkString(" ")).toSeq
  }

}
