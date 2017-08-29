package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{BasicType, ListType, StructType}

/**
  * Created by mikhail on 10/16/16.
  */
case class StopWordsRemoverModel(stopWords: Seq[String],
                                 caseSensitive: Boolean) extends Model {
  def apply(value: Seq[String]): Seq[String] = {
    if(caseSensitive) {
      val stopWordsSet = stopWords.toSet
      value.filter(s => !stopWordsSet.contains(s))
    } else {
      val toLower = (s: String) => if (s != null) s.toLowerCase else s
      val lowerStopWords = stopWords.map(toLower(_)).toSet
      value.filter(s => !lowerStopWords.contains(toLower(s)))
    }
  }

  override def inputSchema: StructType = StructType("input" -> ListType(BasicType.String)).get

  override def outputSchema: StructType = StructType("output" -> ListType(BasicType.String)).get
}
