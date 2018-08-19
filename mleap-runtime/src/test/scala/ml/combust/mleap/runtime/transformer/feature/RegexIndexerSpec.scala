package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.RegexIndexerModel
import ml.combust.mleap.core.types.{NodeShape, ScalarType, StructField, StructType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import org.scalatest.FunSpec

class RegexIndexerSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_string", ScalarType.String))).get
  val dataset = Seq(Row("hEllo"), Row("NpOE"), Row("NEy"))
  val frame = DefaultLeapFrame(schema, dataset)

  private val model = RegexIndexerModel(Seq(
    ("""(?i)HELLO""".r.unanchored, 23),
    ("""(?i)NO""".r.unanchored, 32),
    ("""(?i)NEY""".r.unanchored, 42),
    ("""(?i)NO""".r.unanchored, 11)
  ), defaultIndex = Some(99))

  private val indexer = RegexIndexer(shape = NodeShape.feature(
    inputCol = "test_string",
    outputCol = "test_index"), model = model)

  describe("#transform") {
    it("transforms strings into indices") {
      val data = (for (frame2 <- indexer.transform(frame);
                      frame3 <- frame2.select("test_index")) yield {
        frame3.dataset.map(_.getInt(0))
      }).get

      assert(data == Seq(23, 99, 42))
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(indexer.schema.fields ==
        Seq(StructField("test_string", ScalarType.String),
          StructField("test_index", ScalarType.Int.nonNullable)))
    }
  }
}
