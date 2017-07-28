package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{StructType, TensorType}
import org.apache.commons.math3.util.CombinatoricsUtils
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}

import scala.collection.mutable

/**
  * Created by mikhail on 10/16/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/feature/PolynomialExpansion.scala")
case class PolynomialExpansionModel(degree: Int) extends Model {
  def apply(vector: Vector): Vector = {
    vector match {
      case dense: DenseVector => expand(dense, degree)
      case sparse: SparseVector => expand(sparse, degree)
      case _ => throw new IllegalArgumentException
    }
  }

  def expand(denseVector: DenseVector, degree: Int): DenseVector = {
    val n = denseVector.size
    val polySize = getPolySize(n, degree)
    val polyValues = new Array[Double](polySize - 1)
    expandDense(denseVector.values, n - 1, degree, 1.0, polyValues, -1)
    new DenseVector(polyValues)
  }

  def expand(sparseVector: SparseVector, degree: Int): SparseVector = {
    val polySize = getPolySize(sparseVector.size, degree)
    val nnz = sparseVector.values.length
    val nnzPolySize = getPolySize(nnz, degree)
    val polyIndices = mutable.ArrayBuilder.make[Int]
    polyIndices.sizeHint(nnzPolySize - 1)
    val polyValues = mutable.ArrayBuilder.make[Double]
    polyValues.sizeHint(nnzPolySize - 1)
    expandSparse(
      sparseVector.indices, sparseVector.values, nnz - 1, sparseVector.size - 1, degree, 1.0, polyIndices, polyValues, -1)
    new SparseVector(polySize - 1, polyIndices.result(), polyValues.result())
  }

  def getPolySize(numFeatures: Int, degree: Int): Int = {
    val n = CombinatoricsUtils.binomialCoefficient(numFeatures + degree, degree)
    require(n <= Integer.MAX_VALUE)
    n.toInt
  }

  private def expandDense(values: Array[Double],
                          lastIdx: Int,
                          degree: Int,
                          multiplier: Double,
                          polyValues: Array[Double],
                          curPolyIdx: Int): Int = {
    if (multiplier == 0.0) {
      // do nothing
    } else if (degree == 0 || lastIdx < 0) {
      if (curPolyIdx >= 0) { // skip the very first 1
        polyValues(curPolyIdx) = multiplier
      }
    } else {
      val v = values(lastIdx)
      val lastIdx1 = lastIdx - 1
      var alpha = multiplier
      var i = 0
      var curStart = curPolyIdx
      while (i <= degree && alpha != 0.0) {
        curStart = expandDense(values, lastIdx1, degree - i, alpha, polyValues, curStart)
        i += 1
        alpha *= v
      }
    }
    curPolyIdx + getPolySize(lastIdx + 1, degree)
  }

  private def expandSparse(indices: Array[Int],
                           values: Array[Double],
                           lastIdx: Int,
                           lastFeatureIdx: Int,
                           degree: Int,
                           multiplier: Double,
                           polyIndices: mutable.ArrayBuilder[Int],
                           polyValues: mutable.ArrayBuilder[Double],
                           curPolyIdx: Int): Int = {
    if (multiplier == 0.0) {
      // do nothing
    } else if (degree == 0 || lastIdx < 0) {
      if (curPolyIdx >= 0) { // skip the very first 1
        polyIndices += curPolyIdx
        polyValues += multiplier
      }
    } else {
      // Skip all zeros at the tail.
      val v = values(lastIdx)
      val lastIdx1 = lastIdx - 1
      val lastFeatureIdx1 = indices(lastIdx) - 1
      var alpha = multiplier
      var curStart = curPolyIdx
      var i = 0
      while (i <= degree && alpha != 0.0) {
        curStart = expandSparse(indices, values, lastIdx1, lastFeatureIdx1, degree - i, alpha,
          polyIndices, polyValues, curStart)
        i += 1
        alpha *= v
      }
    }
    curPolyIdx + getPolySize(lastFeatureIdx + 1, degree)
  }

  override def inputSchema: StructType = StructType("input" -> TensorType.Double()).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double()).get

}
