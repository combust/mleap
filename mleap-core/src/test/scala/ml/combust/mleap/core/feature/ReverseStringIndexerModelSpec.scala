package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class ReverseStringIndexerModelSpec extends FunSpec {

  describe("reverse string indexer model") {
    val model = ReverseStringIndexerModel(Seq("one", "two", "three"))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ScalarType.Double.nonNullable)))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ScalarType.String)))
    }
  }

  describe("with a list input shape") {
    val model = ReverseStringIndexerModel(Seq("one", "two", "three"), ListShape(false))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ListType.Double.nonNullable)))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ListType.String)))
    }
  }

  describe("invalid shapes") {
    it("tensor shape throws an IllegalArgumentException") {
      assertThrows[IllegalArgumentException] {
        ReverseStringIndexerModel(Seq("one", "two", "three"), TensorShape(None, isNullable = false))
      }
    }

    it("nullable shape throws an IllegalArgumentException") {
      assertThrows[IllegalArgumentException] {
        ReverseStringIndexerModel(Seq("one", "two", "three"), ScalarShape(true))
      }

      assertThrows[IllegalArgumentException] {
        ReverseStringIndexerModel(Seq("one", "two", "three"), TensorShape(None, true))
      }

      assertThrows[IllegalArgumentException] {
        ReverseStringIndexerModel(Seq("one", "two", "three"), ListShape(true))
      }
    }
  }
}
