package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StopWordsRemoverModel
import ml.combust.mleap.runtime.{LeapFrame, Row, LocalDataset}
import ml.combust.mleap.runtime.types.{StringType, ListType, StructField, StructType}
import org.scalatest.FunSpec

/**
  * Created by mikhail on 10/16/16.
  */
class StopWordsRemoverSpec extends FunSpec{
  val schema = StructType(Seq(StructField("test_string_seq", ListType(StringType)))).get
  val dataset = LocalDataset(Array(Row("I used MLeap transformer".split(" ")), Row("You use Mleap transformer".split(" "))))
  val frame = LeapFrame(schema,dataset)

  val stopwordsTransformer = StopWordsRemover(inputCol = "test_string_seq",
    outputCol = "output_seq",
    model = StopWordsRemoverModel(Array("I", "You", "the"), caseSensitive = true)
  )

  describe("#transform") {
    it("removes stop words from an array of strings") {
      val frame2 = stopwordsTransformer.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getArray[String](1).sameElements(Array("used", "MLeap", "transformer")))
      assert(data(1).getArray[String](1).sameElements(Array("use", "Mleap", "transformer")))
    }
  }
}
