package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MathUnaryModel
import ml.combust.mleap.core.feature.UnaryOperation.Sin
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.runtime.types.{DoubleType, StructField, StructType}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathUnarySpec extends FunSpec {
  val schema = StructType(StructField("test_a", DoubleType())).get
  val dataset = LocalDataset(Seq(Row(42.0)))
  val frame = LeapFrame(schema, dataset)

  describe("#transform") {
    val model = MathUnary(inputCol = "test_a",
      outputCol = "test_out",
      model = MathUnaryModel(Sin))

    it("transforms the leap frame using the given input and operation") {
      val calc = model.transform(frame).get.dataset(0).getDouble(1)
      assert(calc == Math.sin(42.0))
    }
  }
}
