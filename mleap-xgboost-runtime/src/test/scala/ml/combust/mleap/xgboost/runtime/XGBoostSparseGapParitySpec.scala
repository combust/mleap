package ml.combust.mleap.xgboost.runtime

import java.io.ByteArrayOutputStream

import com.yelp.xgboost.parser.PredictorFactory
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import org.apache.spark.ml.linalg.SparseVector
import org.scalatest.funspec.AnyFunSpec
import XgbConverters._

import scala.util.Random

/**
 * A model trained on rows that are almost all gaps learns each gap as a missing feature, so MLeap has
 * to score a gap as missing too. Ground truth is the booster's own prediction on the same sparse row,
 * built the way xgboost4j-spark 1.7.6 builds it, from the stored indices and values. Reading the gaps
 * as 0.0 makes MLeap diverge from it by a large margin.
 */
class XGBoostSparseGapParitySpec extends AnyFunSpec {

  private val numFeatures = 2000
  private val numRows = 300
  private val entriesPerRow = 6

  private val random = new Random(7)

  private val rows: Seq[(Array[Int], Array[Float], Float)] = Seq.fill(numRows) {
    val indices = random.shuffle((0 until numFeatures).toList).take(entriesPerRow).sorted.toArray
    val values = Array.fill(entriesPerRow)(random.nextFloat() * 4 + 0.5f)
    val signal = indices.zip(values).collect { case (i, v) if i < numFeatures / 4 => v }.sum
    (indices, values, if (signal > 2.5f) 1.0f else 0.0f)
  }

  private val trainingSet = new DMatrix(
    rows.iterator.map { case (idx, vals, label) => new LabeledPoint(label, numFeatures, idx, vals) },
    null)

  private val booster: Booster = XGBoost.train(
    trainingSet,
    Map("eta" -> 0.3, "max_depth" -> 4, "objective" -> "binary:logistic", "base_score" -> 0.5),
    30)

  private val predictor = {
    val out = new ByteArrayOutputStream()
    booster.saveModel(out)
    PredictorFactory.fromModelBytes(out.toByteArray)
  }

  private def nativeProbability(indices: Array[Int], values: Array[Float]): Double = {
    val row = new DMatrix(Iterator(new LabeledPoint(0.0f, numFeatures, indices, values)), null)
    try booster.predict(row, false, 0).head.head.toDouble
    finally row.delete()
  }

  private def predictorProbability(vector: SparseVector): Double =
    predictor.predict(vector.asXGBPredictor()).head.toDouble

  describe("a model trained on mostly empty sparse rows") {
    it("is predicted by the MLeap predictor like the booster predicts the same sparse row") {
      val maxDifference = rows.map { case (idx, vals, _) =>
        val vector = new SparseVector(numFeatures, idx, vals.map(_.toDouble))
        math.abs(nativeProbability(idx, vals) - predictorProbability(vector))
      }.max
      assert(maxDifference < 1e-5, s"max |booster - MLeap| = $maxDifference")
    }

    it("is not predicted like the dense row, which would read every gap as 0.0") {
      val differing = rows.count { case (idx, vals, _) =>
        val vector = new SparseVector(numFeatures, idx, vals.map(_.toDouble))
        val dense = predictor.predict(vector.toDense.asXGBPredictor()).head.toDouble
        math.abs(dense - predictorProbability(vector)) > 1e-3
      }
      assert(differing > numRows / 10, s"only $differing of $numRows rows differ between gap=missing and gap=0.0")
    }

    it("keeps the booster-backed DMatrix path equal to the booster for the same sparse row") {
      val maxDifference = rows.map { case (idx, vals, _) =>
        val vector = new SparseVector(numFeatures, idx, vals.map(_.toDouble))
        val viaConverter = booster.predict(vector.asXGB(), false, 0).head.head.toDouble
        math.abs(nativeProbability(idx, vals) - viaConverter)
      }.max
      assert(maxDifference < 1e-6, s"max |booster - asXGB| = $maxDifference")
    }
  }
}
