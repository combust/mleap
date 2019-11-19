package ml.combust.mleap.runtime.transformer.ensemble

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{NodeShape, ScalarType, StructType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, MultiTransformer, Row, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import org.scalatest.FunSpec

case class TestClassifierModel(shift: Int) extends Model {
  def apply(feature: Int): Double = {
    feature + shift
  }
  def predictProbability(feature: Int): Double = 1.0

  override def inputSchema: StructType = StructType("input" -> ScalarType.Int.nonNullable).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.Double.nonNullable,
    "probability" -> ScalarType.Double.nonNullable).get
}

case class TestClassifier(override val uid: String = Transformer.uniqueName("drilldown_classification"),
                          override val shape: NodeShape,
                          override val model: TestClassifierModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = UserDefinedFunction(
    (feature: Int) => Row(model(feature), model.predictProbability(feature)),
    outputSchema,
    inputSchema)
}

class CategoricalDrilldownSpec extends FunSpec {
  val classifier1 = TestClassifier(shape = NodeShape().withStandardInput("feature").
    withOutput("output", "prediction").
    withOutput("probability", "probability"),
    model = TestClassifierModel(10))
  val classifier2 = TestClassifier(shape = NodeShape().withStandardInput("feature").
    withOutput("output", "prediction").
    withOutput("probability", "probability"),
    model = TestClassifierModel(100))
  val classifier3 = TestClassifier(shape = NodeShape().withStandardInput("feature").
    withOutput("output", "prediction").
    withOutput("probability", "probability"),
    model = TestClassifierModel(1000))
  val classifier4 = TestClassifier(shape = NodeShape().withStandardInput("feature").
    withOutput("output", "prediction").
    withOutput("probability", "probability"),
    model = TestClassifierModel(10000))
  val drilldownModel = CategoricalDrilldownModel(Map("class1" -> classifier1,
    "class2" -> classifier2,
    "class3" -> classifier3,
    "class4" -> classifier4))

  val drilldown = CategoricalDrilldown(shape = NodeShape().
    withInput("label", "label").
    withInput("drilldown.input", "feature").
    withOutput("drilldown.output", "prediction").
    withOutput("drilldown.probability", "probability"),
    model = drilldownModel)

  val schema = StructType("feature" -> ScalarType.Int.nonNullable,
    "label" -> ScalarType.String.nonNullable).get
  val data = Seq(Row(10, "class2"), Row(100, "class3"), Row(1000, "class1"), Row(10000, "class4"))
  val frame = DefaultLeapFrame(schema, data)

  describe("transforming") {
    it("uses the input field to drill down using a different model") {
      val data = (for (frame2 <- drilldown.transform(frame);
                       frame3 <- frame2.select("prediction", "probability")) yield {
        frame3.dataset
      }).get

      assert(data.map(_.getDouble(0)) == Seq(110.0, 1100.0, 1010.0, 20000.0))
      assert(data.map(_.getDouble(1)) == Seq(1.0, 1.0, 1.0, 1.0))
    }
  }
}
