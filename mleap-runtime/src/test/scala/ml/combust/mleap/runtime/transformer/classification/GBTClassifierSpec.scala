package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.GBTClassifierModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.test.TestUtil
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.Tensor
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hollinwilkins on 9/28/16.
  */
class GBTClassifierSpec extends AnyFunSpec {
  val schema = StructType(Seq(StructField("features", TensorType(BasicType.Double)))).get
  val dataset = Seq(Row(Tensor.denseVector(Array(0.2, 0.7, 0.4))))
  val frame = DefaultLeapFrame(schema, dataset)
  val tree1 = TestUtil.buildDecisionTreeRegression(0.5, 0, goLeft = true)
  val tree2 = TestUtil.buildDecisionTreeRegression(0.75, 1, goLeft = false)
  val tree3 = TestUtil.buildDecisionTreeRegression(0.1, 2, goLeft = true)
  val gbt = GBTClassifier(shape = NodeShape.probabilisticClassifier(),
    model = GBTClassifierModel(Seq(tree1, tree2, tree3), Seq(0.5, 2.0, 1.0), 3))

  describe("#transform") {
    val schema = StructType(Seq(StructField("features", TensorType.Double(3)))).get
    val dataset = Seq(Row(Tensor.denseVector(Array(0.2, 0.7, 0.4))))
    val frame = DefaultLeapFrame(schema, dataset)
    val tree1 = TestUtil.buildDecisionTreeRegression(0.5, 0, goLeft = true)
    val tree2 = TestUtil.buildDecisionTreeRegression(0.75, 1, goLeft = false)
    val tree3 = TestUtil.buildDecisionTreeRegression(0.1, 2, goLeft = true)
    val gbt = GBTClassifier(shape = NodeShape.probabilisticClassifier(),
      model = GBTClassifierModel(Seq(tree1, tree2, tree3), Seq(0.5, 2.0, 1.0), 3))

    it("uses the GBT to make predictions on the features column") {
      val frame2 = gbt.transform(frame).get
      val prediction = frame2.dataset(0).getDouble(1)

      assert(prediction == 1.0)
    }

    describe("with invalid features column") {
      val gbt2 = gbt.copy(shape = NodeShape.probabilisticClassifier(featuresCol = "bad_features").
              withOutput("prediction", "prediction"))

      it("returns a Failure") { assert(gbt2.transform(frame).isFailure) }
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs with only prediction column") {
      val transformer = GBTClassifier(
        shape = NodeShape.probabilisticClassifier(),
        model = new GBTClassifierModel(null, Seq(1.0, 1.0), 3))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with prediction column as well as probabilityCol") {
      val transformer = GBTClassifier(shape = NodeShape.probabilisticClassifier(probabilityCol = Some("probability")),
        model = new GBTClassifierModel(null, Seq(1.0, 1.0), 3))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with prediction column as well as rawPredictionCol") {
      val transformer = GBTClassifier(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp")),
        model = new GBTClassifierModel(null, Seq(1.0, 1.0), 3))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with prediction column as well as both rawPredictionCol and probabilityCol") {
      val transformer = GBTClassifier(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp"),
        probabilityCol = Some("probability")),
        model = new GBTClassifierModel(null, Seq(1.0, 1.0), 3))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}
