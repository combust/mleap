package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ReverseStringIndexerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import org.scalatest.FunSpec

class ReverseStringIndexerSpec extends FunSpec {
  val scalarFrame = DefaultLeapFrame(
    StructType(StructField("input", ScalarType.Double.nonNullable)).get,
    Seq(Row(1.0))
  )

  val listFrame = DefaultLeapFrame(
    StructType(StructField("input", ListType.Double.nonNullable)).get,
    Seq(Row(Seq(1.0, 2.0)))
  )

  describe("scalar input/output") {
    it("has the correct inputs and outputs") {
      val transformer = ReverseStringIndexer(shape = NodeShape.feature(),
        model = ReverseStringIndexerModel(Seq("one", "two", "three")))

      assert(transformer.schema.fields ==
        Seq(StructField("input", ScalarType.Double.nonNullable),
          StructField("output", ScalarType.String)))

      assert(transformer.transform(scalarFrame).get.dataset.head.getString(1) == "two")
    }
  }

  describe("list input/output") {
    it("has the correct inputs and outputs") {
      val transformer = ReverseStringIndexer(shape = NodeShape.feature(),
        model = ReverseStringIndexerModel(Seq("one", "two", "three"), ListShape(false)))

      assert(transformer.schema.fields ==
        Seq(StructField("input", ListType.Double.nonNullable),
          StructField("output", ListType.String)))

      assert(transformer.transform(listFrame).get.dataset.head.getSeq[String](1) == Seq("two", "three"))
    }
  }
}