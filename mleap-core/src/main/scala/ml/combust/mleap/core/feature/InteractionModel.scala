package ml.combust.mleap.core.feature

import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.DataType
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable

/**
  * Created by hollinwilkins on 4/26/17.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/branch-2.1/mllib/src/main/scala/org/apache/spark/ml/feature/Interaction.scala")
case class InteractionModel(featuresSpec: Array[Array[Int]],
                            inputTypes: Seq[DataType]) {
  val encoders: Array[FeatureEncoder] = featuresSpec.map(FeatureEncoder.apply)

  def apply(features: Seq[Any]): Vector = {
    var indices = mutable.ArrayBuilder.make[Int]
    var values = mutable.ArrayBuilder.make[Double]
    var size = 1
    indices += 0
    values += 1.0
    var featureIndex = features.length - 1
    while (featureIndex >= 0) {
      val prevIndices = indices.result()
      val prevValues = values.result()
      val prevSize = size
      val currentEncoder = encoders(featureIndex)
      indices = mutable.ArrayBuilder.make[Int]
      values = mutable.ArrayBuilder.make[Double]
      size *= currentEncoder.outputSize
      currentEncoder.foreachNonzeroOutput(features(featureIndex), (i, a) => {
        var j = 0
        while (j < prevIndices.length) {
          indices += prevIndices(j) + i * prevSize
          values += prevValues(j) * a
          j += 1
        }
      })
      featureIndex -= 1
    }
    Vectors.sparse(size, indices.result(), values.result()).compressed
  }
}

case class FeatureEncoder(numFeatures: Array[Int]) {
  assert(numFeatures.forall(_ > 0), "Features counts must all be positive.")

  /** The size of the output vector. */
  val outputSize = numFeatures.sum

  /** Precomputed offsets for the location of each output feature. */
  private val outputOffsets = {
    val arr = new Array[Int](numFeatures.length)
    var i = 1
    while (i < arr.length) {
      arr(i) = arr(i - 1) + numFeatures(i - 1)
      i += 1
    }
    arr
  }

  /**
    * Given an input row of features, invokes the specific function for every non-zero output.
    *
    * @param v The row value to encode, either a Double or Vector.
    * @param f The callback to invoke on each non-zero (index, value) output pair.
    */
  def foreachNonzeroOutput(v: Any, f: (Int, Double) => Unit): Unit = {
    val value = v match {
      case tensor: Tensor[_] => tensor.asInstanceOf[Tensor[Double]]: Vector
      case _ => v
    }

    value match {
      case d: Double =>
        assert(numFeatures.length == 1, "DoubleType columns should only contain one feature.")
        val numOutputCols = numFeatures.head
        if (numOutputCols > 1) {
          assert(
            d >= 0.0 && d == d.toInt && d < numOutputCols,
            s"Values from column must be indices, but got $d.")
          f(d.toInt, 1.0)
        } else {
          f(0, d)
        }
      case vec: Vector =>
        assert(numFeatures.length == vec.size,
          s"Vector column size was ${vec.size}, expected ${numFeatures.length}")
        vec.foreachActive { (i, v) =>
          val numOutputCols = numFeatures(i)
          if (numOutputCols > 1) {
            assert(
              v >= 0.0 && v == v.toInt && v < numOutputCols,
              s"Values from column must be indices, but got $v.")
            f(outputOffsets(i) + v.toInt, 1.0)
          } else {
            f(outputOffsets(i), v)
          }
        }
      case null =>
        throw new IllegalArgumentException("Values to interact cannot be null.")
      case o =>
        throw new IllegalArgumentException(s"$o of type ${o.getClass.getName} is not supported.")
    }
  }
}
