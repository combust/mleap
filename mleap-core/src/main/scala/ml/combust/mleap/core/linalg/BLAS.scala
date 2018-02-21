package ml.combust.mleap.core.linalg

import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}
import breeze.linalg.{Vector, DenseVector, SparseVector}

object BLAS {
  lazy val l1: NetlibBLAS = new F2jBLAS

  def scal(a: Double, vec: Vector[Double]): Unit = vec match {
    case vec: DenseVector[_] => scal(a, vec.asInstanceOf[DenseVector[Double]])
    case vec: SparseVector[_] => scal(a, vec.asInstanceOf[SparseVector[Double]])
  }

  def scal(a: Double, vec: DenseVector[Double]): Unit = {
    val data = vec.asInstanceOf[DenseVector[Double]].data
    l1.dscal(data.length, a, data, 1)
  }

  def scal(a: Double, vec: SparseVector[Double]): Unit = {
    val data = vec.asInstanceOf[SparseVector[Double]].data
    l1.dscal(data.length, a, data, 1)
  }
}
