package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.FunSpec

class StringMapModelSpec extends FunSpec {

  val model = StringMapModel(Map("label1" -> 1.0, "label2" -> 1.0, "label3" -> 2.0))

  describe("#apply") {
    it("returns the mapped value by label") {
      assert(model("label1") == 1.0)
      assert(model("label2") == 1.0)
      assert(model("label3") == 2.0)
    }

    it("throws NoSuchElementException when encounters a missing label and handleInvalid is not keep") {
      val thrown =
        intercept[NoSuchElementException] {
          model("missing_label")
        }
      assert(thrown.getMessage == "Missing label: missing_label. To handle unseen labels, set handleInvalid to keep" +
        " and optionally set a custom defaultValue")
    }

    it("returns default value for HandleInvalid.Keep mode") {
      val model = StringMapModel(Map("label1" -> 1.0), handleInvalid = HandleInvalid.Keep)
      assert(model("label1") == 1.0)
      assert(model("missing_label") == 0.0)
    }

    it("returns custom default value for HandleInvalid.keep mode") {
      val model = StringMapModel(Map("label1" -> 1.0), handleInvalid = HandleInvalid.Keep, defaultValue = 2.0)
      assert(model("label1") == 1.0)
      assert(model("missing_label") == 2.0)
    }
  }

  describe("string map model") {
    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ScalarType.String)))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ScalarType.Double.nonNullable)))
    }
  }
}
