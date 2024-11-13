package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{BasicType, ListType, StructType, TensorType}
import org.apache.spark.ml.linalg.mleap.BLAS
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

/**
  * Created by hollinwilkins on 12/28/16.
  */
sealed trait WordToVectorKernel {
  def apply(size: Int, sentenceSize: Int, vectors: Iterator[Vector]): Vector
  def name: String
}
object WordToVectorKernel {
  private val lookup: Map[String, WordToVectorKernel] = Seq(Default, Sqrt).map {
    k => (k.name, k)
  }.toMap

  def forName(name: String): WordToVectorKernel = lookup(name)

  case object Default extends WordToVectorKernel {
    override def apply(size: Int, sentenceSize: Int, vectors: Iterator[Vector]): Vector = {
      val sum = Vectors.zeros(size)
      for (v <- vectors) {
        BLAS.axpy(1.0, v, sum)
      }
      BLAS.scal(1.0 / sentenceSize, sum)
      sum
    }

    override def name: String = "default"
  }

  case object Sqrt extends WordToVectorKernel {
    override def apply(size: Int, sentenceSize: Int, vectors: Iterator[Vector]): Vector = {
      val sum = Vectors.zeros(size)
      for (v <- vectors) {
        BLAS.axpy(1.0, v, sum)
      }

      val values = sum match {
        case sum: DenseVector => sum.values
        case sum: SparseVector => sum.values
      }

      var i = 0
      val s = values.length
      val sqrt = Math.sqrt(BLAS.dot(sum, sum))
      while (i < s) {
        values(i) /= sqrt
        i += 1
      }

      sum
    }

    override def name: String = "sqrt"
  }
}

case class WordToVectorModel(wordIndex: Map[String, Int],
                             wordVectors: Array[Double],
                             kernel: WordToVectorKernel = WordToVectorKernel.Default) extends Model {
  val numWords: Int = wordIndex.size
  val vectorSize: Int = wordVectors.length / numWords
  val vectors: Map[String, Vector] = {
    wordIndex.map { case (word, ind) =>
      (word, wordVectors.slice(vectorSize * ind, vectorSize * ind + vectorSize))
    }
  }.mapValues(Vectors.dense).map(identity).toMap

  def apply(sentence: Seq[String]): Vector = {
    if (sentence.isEmpty) {
      Vectors.sparse(vectorSize, Array.empty[Int], Array.empty[Double])
    } else {
      val vs = sentence.iterator.map(vectors.get).
        filter(_.isDefined).
        map(_.get)
      kernel(vectorSize, sentence.size, vs)
    }
  }

  override def inputSchema: StructType = StructType("input" -> ListType(BasicType.String)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(vectorSize)).get
}
