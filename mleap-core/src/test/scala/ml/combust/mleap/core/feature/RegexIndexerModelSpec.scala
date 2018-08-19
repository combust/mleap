package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.FunSpec

class RegexIndexerModelSpec extends FunSpec {
  private val indexer = RegexIndexerModel(Seq(
    ("""(?i)HELLO""".r.unanchored, 23),
    ("""(?i)NO""".r.unanchored, 32),
    ("""(?i)NEY""".r.unanchored, 42),
    ("""(?i)NO""".r.unanchored, 11)
  ), defaultIndex = Some(99))

  describe("predicting") {
    it("returns the fist index that matches the value") {
      assert(indexer("ney") == 42)
      assert(indexer("no") == 32)
    }

    it("falls back to the default value") {
      assert(indexer("what") == 99)
    }

    it("throws an error if no match and no default value") {
      val indexer2 = indexer.copy(defaultIndex = None)
      assertThrows[NoSuchElementException](indexer2("what"))
    }
  }

  describe("input/output schema") {
    it("has the right input schema") {
      assert(indexer.inputSchema.fields == Seq(StructField("input", ScalarType.String)))
    }

    it("has the right output schema") {
      assert(indexer.outputSchema.fields == Seq(StructField("output", ScalarType.Int.nonNullable)))
    }
  }
}
