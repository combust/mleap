package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.prop.TableDrivenPropertyChecks

/**
  * Created by hwilkins on 1/21/16.
  */
class StringIndexerModelSpec extends org.scalatest.funspec.AnyFunSpec with TableDrivenPropertyChecks {
  describe("#apply") {
    it("returns the index of the string") {
      val indexer = StringIndexerModel(Array("hello", "there", "dude"))

      assert(indexer("hello") == 0.0)
      assert(indexer("there") == 1.0)
      assert(indexer("dude") == 2.0)
    }

    it("throws NullPointerException when encounters NULL/None and handleInvalid is not keep") {
      val indexer = StringIndexerModel(Array("hello"))
      assertThrows[NullPointerException](indexer(null))
    }

    it("throws NoSuchElementException when encounters unseen label and handleInvalid is not keep") {
      val indexer = StringIndexerModel(Array("hello"))
      val unseenLabels = Table("unknown1", "unknown2")

      forAll(unseenLabels) { (label: Any) =>
        intercept[NoSuchElementException] {
          indexer(label)
        }
      }
    }

    it("returns default index for HandleInvalid.keep mode") {
      val indexer = StringIndexerModel(Array("hello", "there", "dude"), handleInvalid = HandleInvalid.Keep)
      val invalidLabels = Table("unknown", "other unknown", null, None)

      forAll(invalidLabels) { (label: Any) =>
        assert(indexer(label) == 3.0)
      }
    }
  }

  describe("input/output schema") {
    val indexer = StringIndexerModel(Array("hello", "there", "dude"))

    it("has the right input schema") {
      assert(indexer.inputSchema.fields == Seq(StructField("input", ScalarType.String)))
    }

    it("has the right output schema") {
      assert(indexer.outputSchema.fields == Seq(StructField("output", ScalarType.Double.nonNullable)))
    }
  }
}
