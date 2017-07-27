package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.RegexTokenizerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import org.scalatest.FunSpec

class RegexTokenizerSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_string", ScalarType.String))).get
  val dataset = LocalDataset(Seq(Row("dies isT Ein TEST text te")))
  val frame = LeapFrame(schema, dataset)

  val gapRegexTokenizer = RegexTokenizer(
    shape = NodeShape().withStandardInput("test_string").
          withStandardOutput("test_tokens"),
    model = RegexTokenizerModel(
      regex = """\s""".r,
      matchGaps = true,
      tokenMinLength = 3,
      lowercaseText = true
    )
  )

  val wordRegexTokenizer = RegexTokenizer(
    shape = NodeShape().withStandardInput("test_string").
          withStandardOutput("test_tokens"),
    model = RegexTokenizerModel(
      regex = """\w+""".r,
      matchGaps = false,
      tokenMinLength = 4,
      lowercaseText = false
    )
  )

  describe("#transform") {
    it("converts input string into tokens by matching the gap") {
      val frame2 = gapRegexTokenizer.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getSeq(1) == Seq("dies", "ist", "ein", "test", "text"))
    }

    it("converts input string into tokens by matching the words") {
      val frame2 = wordRegexTokenizer.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getSeq(1) == Seq("dies", "TEST", "text"))
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(wordRegexTokenizer.schema.fields ==
        Seq(StructField("test_string", ScalarType.String),
          StructField("test_tokens", ListType(BasicType.String))))
    }
  }
}
