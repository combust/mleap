package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.NGramModel
import ml.combust.mleap.runtime.{LeapFrame, Row, LocalDataset}
import ml.combust.mleap.runtime.types.{ArrayType, StringType, StructField, StructType}
import org.scalatest.FunSpec

/**
  * Created by mikhail on 10/16/16.
  */
class NGramSpec extends FunSpec{
  val schema = StructType(Seq(StructField("test_string_seq", ArrayType(StringType)))).get
  val dataset = LocalDataset(Seq(Row("a b c".split(" ")), Row("d e f".split(" ")), Row("g h i".split(" "))))
  val frame = LeapFrame(schema,dataset)

  val ngram = NGram(inputCol = "test_string_seq",
    outputCol = "output_ngram",
    model = NGramModel(2)
  )

  describe("#transform") {
    it("converts an array of string into a bi-gram array") {
      val frame2 = ngram.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getArray[String](1)(0) == "a b")
      assert(data(0).getArray[String](1)(1) == "b c")
    }
  }

  describe("with invalid input column") {
    val ngram2 = ngram.copy(inputCol = "invalid_column")

    it("returns a failure") {assert(ngram2.transform(frame).isFailure)}
  }

}
