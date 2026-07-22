package ml.combust.mleap.xgboost.runtime

import java.io.{ByteArrayOutputStream, File}
import java.nio.file.Files

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.xgboost.runtime.bundle.ops.{XGBoostPredictorClassificationOp, XGBoostPredictorRegressionOp}
import com.yelp.xgboost.parser.PredictorFactory
import com.yelp.xgboost.Predictor
import ml.combust.mleap.xgboost.runtime.testing.{BoosterUtils, CachedDatasetUtils, FloatingPointApproximations}
import ml.dmlc.xgboost4j.scala.Booster
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import scala.util.Using
import XgbConverters._

/**
 * Proves BOTH model formats load AND predict correctly through a full MLeap bundle round-trip:
 *   - Legacy-binary: a committed xgboost 2.0.3 fixture (native 3.3.0 cannot load it), asserted
 *     against frozen goldens.
 *   - UBJSON: a freshly trained 3.3.0 model, asserted against its own Booster.
 * Both go through store -> bundle zip -> load -> predict, exercising the leading-byte format
 * dispatcher on each branch (legacy header -> ModelReader, '{' -> UBJSONReader).
 */
class XGBoostBundleBackwardCompatSpec extends AnyFunSpec
  with Matchers
  with BoosterUtils
  with CachedDatasetUtils
  with FloatingPointApproximations {

  private implicit val context: MleapContext = {
    implicit val c = MleapContext.defaultContext
    c.bundleRegistry.register(new XGBoostPredictorClassificationOp())
    c.bundleRegistry.register(new XGBoostPredictorRegressionOp())
    c
  }

  private def bytesOf(resource: String): Array[Byte] = {
    val stream = getClass.getClassLoader.getResourceAsStream(resource)
    assert(stream != null, s"Missing resource $resource")
    try stream.readAllBytes() finally stream.close()
  }

  private def predictorFrom(bytes: Array[Byte]): Predictor = PredictorFactory.fromModelBytes(bytes)

  private def roundTrip(transformer: Transformer): Transformer = {
    val file = new File(Files.createTempDirectory("xgboost-bwc").toFile, "bundle.zip")
    import ml.combust.mleap.runtime.MleapSupport._
    Using(BundleFile(file)) { bf =>
      transformer.writeBundle.format(SerializationFormat.Json).save(bf).get
    }.get
    Using(BundleFile(file)) { bf =>
      bf.loadMleapBundle()
    }.flatten.get.root
  }

  private def classificationTransformer(predictor: Predictor, numClasses: Int, numFeatures: Int): Transformer = {
    val impl =
      if (numClasses == 2) XGBoostPredictorBinaryClassificationModel(predictor, numFeatures, 0)
      else XGBoostPredictorMultinomialClassificationModel(predictor, numClasses, numFeatures, 0)
    XGBoostPredictorClassification(
      "xgb-bwc-classifier",
      NodeShape.probabilisticClassifier(probabilityCol = Some("probability")),
      XGBoostPredictorClassificationModel(impl))
  }

  private def regressionTransformer(predictor: Predictor, numFeatures: Int): Transformer =
    XGBoostPredictorRegression(
      "xgb-bwc-regression",
      NodeShape.regression(),
      XGBoostPredictorRegressionModel(predictor, numFeatures, 0))

  private def probabilities(frame: DefaultLeapFrame): Array[Double] = {
    val idx = frame.schema.indexOf("probability").get
    frame.dataset.head.getTensor[Double](idx).toArray
  }

  private def prediction(frame: DefaultLeapFrame): Double = {
    val idx = frame.schema.indexOf("prediction").get
    frame.dataset.head.getDouble(idx)
  }

  private def singleRowFrame(leapFrame: DefaultLeapFrame, rowIndex: Int): DefaultLeapFrame =
    DefaultLeapFrame(leapFrame.schema, Seq(leapFrame.dataset(rowIndex)))

  describe("Legacy-binary bundle (xgboost 2.0.3 fixture)") {
    it("dispatches the legacy header to the ModelReader, not the UBJSON reader") {
      val bytes = bytesOf("datasources/golden/binary_logistic.model")
      assert(bytes(0) != '{', "fixture must be legacy-binary (no JSON object marker)")
      noException should be thrownBy predictorFrom(bytes)
    }

    it("binary:logistic round-trips through a bundle and matches frozen 2.0.3 probabilities") {
      val predictor = predictorFrom(bytesOf("datasources/golden/binary_logistic.model"))
      val transformer = roundTrip(classificationTransformer(predictor, numClasses = 2, numFeatures = numFeatures(leapFrameBinomial)))

      // Frozen 2.0.3 probability for agaricus row 0 (see golden_2.0.3.json binary_logistic case).
      val result = transformer.transform(singleRowFrame(leapFrameBinomial, 0)).get
      val p = probabilities(result)
      assert(almostEqual(p(1), 0.9843438863754272))
      assert(almostEqual(p(0), 1.0 - 0.9843438863754272))
    }

    it("reg:squarederror round-trips through a bundle (dense and sparse) and matches frozen 2.0.3") {
      val predictor = predictorFrom(bytesOf("datasources/golden/reg_squarederror.model"))
      val transformer = roundTrip(regressionTransformer(predictor, numFeatures(leapFrameBinomial)))

      val sparseResult = transformer.transform(singleRowFrame(leapFrameBinomial, 0)).get
      assert(almostEqual(prediction(sparseResult), 0.9793035387992859))

      val denseResult = transformer.transform(singleRowFrame(toDenseFeaturesLeapFrame(leapFrameBinomial), 0)).get
      assert(almostEqual(prediction(denseResult), 0.9793035387992859))
    }
  }

  describe("UBJSON bundle (xgboost 3.3.0)") {
    it("dispatches the UBJSON marker to the UBJSON reader without throwing 'Too long string'") {
      val booster: Booster = trainBooster(binomialDataset)
      val out = new ByteArrayOutputStream()
      booster.saveModel(out)
      val bytes = out.toByteArray
      assert(bytes(0) == '{', "3.3.0 saveModel must emit a JSON/UBJSON object")
      noException should be thrownBy predictorFrom(bytes)
    }

    it("binary:logistic round-trips through a bundle and matches its Booster (dense and sparse)") {
      val booster: Booster = trainBooster(binomialDataset)
      val out = new ByteArrayOutputStream()
      booster.saveModel(out)
      val transformer = roundTrip(classificationTransformer(predictorFrom(out.toByteArray), 2, numFeatures(leapFrameBinomial)))

      Seq(leapFrameBinomial, toDenseFeaturesLeapFrame(leapFrameBinomial)).foreach { frame =>
        val row = singleRowFrame(frame, 0)
        val dm = row.dataset.head(frame.schema.indexOf("features").get).asInstanceOf[Tensor[Double]].asXGB
        val boosterProb = booster.predict(dm, false, 0).head(0).toDouble
        val p = probabilities(transformer.transform(row).get)
        assert(almostEqual(p(1), boosterProb))
      }
    }

    it("multi:softprob round-trips through a bundle and matches its Booster") {
      val booster: Booster = trainMultinomialBooster(multinomialDataset)
      val out = new ByteArrayOutputStream()
      booster.saveModel(out)
      val transformer = roundTrip(classificationTransformer(predictorFrom(out.toByteArray), 3, numFeatures(leapFrameMultinomial)))

      val row = singleRowFrame(leapFrameMultinomial, 0)
      val dm = row.dataset.head(leapFrameMultinomial.schema.indexOf("features").get).asInstanceOf[Tensor[Double]].asXGB
      val boosterProb = booster.predict(dm, false, 0).head.map(_.toDouble)
      val p = probabilities(transformer.transform(row).get)
      assert(almostEqualSequences(Seq(boosterProb), Seq(p)))
    }
  }
}
