package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.{DenseTensor, SparseTensor, Tensor}
import ml.combust.mleap.core.util.VectorConverters._
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable

/**
  * Created by hollinwilkins on 4/26/17.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/branch-2.1/mllib/src/main/scala/org/apache/spark/ml/feature/Interaction.scala")
case class InteractionModel(featuresSpec: Array[Array[Int]],
                            inputShapes: Seq[DataShape]) extends Model {
  assert(inputShapes.find(s => !s.isScalar && !s.isTensor) == None, "must provide scalar and tensor shapes as inputs")

  val outputSize = featuresSpec.map(_.sum).product
  // The Seq created below are required by SparseTensor api during initialization
  // For performance optimization, we initialize these sequences here so we don't have to at runtime
  val seqCache: Array[Seq[Int]] = {
    val arr = mutable.ArrayBuilder.make[Seq[Int]]
    for (i <- 0 to outputSize){
      arr += Seq(i)
    }
    arr.result()
  }
  val encoders: Array[FeatureEncoder] = featuresSpec.map(FeatureEncoder.apply)

  def apply(features: Seq[Any]): Vector = {
    val (size, indices, values) = _apply(features)
    Vectors.sparse(size, indices, values)
  }

  def mleapApply(features: Seq[Any]): Tensor[Double] = {
    val (size, indices, values) = _apply(features)
    SparseTensor(indices.map(e=>seqCache(e)), values, Seq(size))
  }

  def _apply(features: Seq[Any]): (Int, Array[Int], Array[Double]) = {
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
    Tuple3(size, indices.result(), values.result())
  }

  override def inputSchema: StructType = {
    val inputFields = inputShapes.zipWithIndex.map {
      case (shape, i) => StructField(s"input$i", DataType(BasicType.Double, shape))
    }
    StructType(inputFields).get
  }

  override def outputSchema: StructType = {
    StructType(StructField("output" -> TensorType.Double(outputSize))).get
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

  private def checkAndApplyWithOffset(
                                       numOutputCols: Int, dV: Double, idx: Int, f: (Int, Double) => Unit
                                     ): Unit = {
    if (numOutputCols > 1) {
      assert(
        dV >= 0.0 && dV == dV.toInt && dV < numOutputCols,
        s"Values from column must be indices, but got $dV.")
      f(outputOffsets(idx) + dV.toInt, 1.0)
    } else {
      f(outputOffsets(idx), dV)
    }
  }
  /**
    * Given an input row of features, invokes the specific function for every non-zero output.
    *
    * @param v The row value to encode, either a Double or Vector.
    * @param f The callback to invoke on each non-zero (index, value) output pair.
    */
  def foreachNonzeroOutput(v: Any, f: (Int, Double) => Unit): Unit = {
    v match {
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
          checkAndApplyWithOffset(numOutputCols, v, i, f)
        }
      case dTensor: DenseTensor[_] =>
        assert(numFeatures.length == dTensor.size,
          s"Vector column size was ${dTensor.size}, expected ${numFeatures.length}")
        dTensor.values.zipWithIndex foreach {
          case (nV: Number, idx: Int)=>
            val numOutputCols = numFeatures(idx)
            val dV = nV.doubleValue()
            checkAndApplyWithOffset(numOutputCols, dV, idx, f)
        }
      case sTensor: SparseTensor[_] =>
        assert(numFeatures.length == sTensor.size,
          s"Vector column size was ${sTensor.size}, expected ${numFeatures.length}")
        var idx = 0
        sTensor.indices.map(_.head).foreach { i =>
          val numOutputCols = numFeatures(i)
          val dV = sTensor.values(idx).asInstanceOf[Number].doubleValue()
          checkAndApplyWithOffset(numOutputCols, dV, idx, f)
          idx += 1
        }
      case null =>
        throw new IllegalArgumentException("Values to interact cannot be null.")
      case o =>
        throw new IllegalArgumentException(s"$o of type ${o.getClass.getName} is not supported.")
    }
  }
}
