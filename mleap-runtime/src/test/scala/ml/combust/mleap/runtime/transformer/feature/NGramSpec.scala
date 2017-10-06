package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.NGramModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import org.scalatest.FunSpec

/**
  * Created by mikhail on 10/16/16.
  */
class NGramSpec extends FunSpec{
  val schema = StructType(Seq(StructField("test_string_seq", ListType(BasicType.String)))).get
  val dataset = Seq(Row("a b c".split(" ").toSeq), Row("d e f".split(" ").toSeq), Row("g h i".split(" ").toSeq))
  val frame = DefaultLeapFrame(schema,dataset)

  val ngram = NGram(
    shape = NodeShape().withStandardInput("test_string_seq").
          withStandardOutput("output_ngram"),
    model = NGramModel(2)
  )

  describe("#transform") {
    it("converts an array of string into a bi-gram array") {
      val frame2 = ngram.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getSeq[String](1).head == "a b")
      assert(data(0).getSeq[String](1)(1) == "b c")
    }
  }

  describe("with invalid input column") {
    val ngram2 = ngram.copy(shape = NodeShape().withStandardInput("bad_input").
          withStandardOutput("output"))

    it("returns a failure") {assert(ngram2.transform(frame).isFailure)}
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(ngram.schema.fields ==
        Seq(StructField("test_string_seq", ListType(BasicType.String)),
          StructField("output_ngram", ListType(BasicType.String))))
    }
  }
}
