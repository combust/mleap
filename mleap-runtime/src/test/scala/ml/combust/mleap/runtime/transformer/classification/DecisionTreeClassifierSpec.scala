package ml.combust.mleap.runtime.transformer.classification

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.classification.DecisionTreeClassifierModel
import ml.combust.mleap.core.tree.{ContinuousSplit, InternalNode, LeafNode}
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.test.TestUtil
import org.scalatest.funspec.AnyFunSpec
import scala.util.Using

import java.io.File
import java.net.URI

class DecisionTreeClassifierSpec extends AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs with only prediction column") {
      val transformer = DecisionTreeClassifier(
        shape = NodeShape.probabilisticClassifier(),
        model = DecisionTreeClassifierModel(null, 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with prediction column as well as probabilityCol") {
      val transformer = DecisionTreeClassifier(shape = NodeShape.probabilisticClassifier(probabilityCol = Some("probability")),
        model = DecisionTreeClassifierModel(null, 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with prediction column as well as rawPredictionCol") {
      val transformer = DecisionTreeClassifier(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp")),
        model = DecisionTreeClassifierModel(null, 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with prediction column as well as both rawPredictionCol and probabilityCol") {
      val transformer = DecisionTreeClassifier(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp"),
        probabilityCol = Some("probability")),
        model = DecisionTreeClassifierModel(null, 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }

  describe("save/load model") {
    it("correctly reproduces the model when saved and loaded") {
      val node = InternalNode(LeafNode(Seq(0.78)), LeafNode(Seq(0.34)), ContinuousSplit(0, 0.5))
      val transformer = DecisionTreeClassifier(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp"),
        probabilityCol = Some("probability")),
        model = DecisionTreeClassifierModel(node, 3, 2))

      // serialization
      val fileName = s"${TestUtil.baseDir}/decision_tree_classifier_saved_model.json.zip"
      val uri = new URI(s"jar:file:$fileName")
      Using(BundleFile(uri)) { file =>
        transformer.writeBundle.name("bundle")
          .format(SerializationFormat.Json)
          .save(file)
      }

      // de-serialization
      val file = new File(fileName)
      val loadedTransformer = Using(BundleFile(file)) { bf =>
        bf.loadMleapBundle()
      }.flatten.get.root.asInstanceOf[DecisionTreeClassifier]

      // checks
      assert(transformer.inputSchema.fields equals loadedTransformer.inputSchema.fields)
      assert(transformer.outputSchema.fields equals loadedTransformer.outputSchema.fields)
    }
  }
}
