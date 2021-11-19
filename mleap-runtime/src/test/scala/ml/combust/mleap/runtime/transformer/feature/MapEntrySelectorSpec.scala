package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MapEntrySelectorModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import org.scalatest.FunSpec

class MapEntrySelectorSpec extends FunSpec {
  val schema = StructType(Seq(
    StructField("test_map", MapType(BasicType.String, BasicType.Double)),
    StructField("my_key", ScalarType.String)
  )).get
  val dataset = Seq(
    Row(Map("a"->1.0), "a"),
    Row(Map("b"->2.0), "key_does_not_exist"),
    Row(Map("c"->3.0), "c")
  )
  val frame = DefaultLeapFrame(schema, dataset)

  val transformer = MapEntrySelector(
    shape=NodeShape()
      .withStandardInput("test_map")
      .withInput("key", "my_key")
      .withStandardOutput("result"),
    model=MapEntrySelectorModel[String, Double](defaultValue = 42.42)
  )

  describe("#transform") {
    it("selects from the map into double") {
      val data = transformer.transform(frame).get.dataset
      assert(data(0).getDouble(2) == 1.0)
      assert(data(1).getDouble(2) == 42.42)
      assert(data(2).getDouble(2) == 3.0)
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(transformer.schema.fields == Seq(
        StructField("test_map", MapType(BasicType.String, BasicType.Double)),
        StructField("my_key", ScalarType.String.nonNullable),
        StructField("result", ScalarType.Double.nonNullable)
      ))
    }
  }
}
