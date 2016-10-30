package ml.combust.mleap.core.feature

/**
  * Created by mikhail on 10/16/16.
  */
case class StopWordsRemoverModel(stopWords: Array[String],
                                 caseSensitive: Boolean) extends Serializable {
  def apply(value: Array[String]): Array[String] = {
    if (caseSensitive) {
      val stopWordsSet = stopWords.toSet
      value.filter(s => !stopWordsSet.contains(s))
    } else {
      val toLower = (s: String) => if (s != null) s.toLowerCase else s
      val lowerStopWords = stopWords.map(toLower(_)).toSet
      value.filter(s => !lowerStopWords.contains(toLower(s)))
    }
  }
}
