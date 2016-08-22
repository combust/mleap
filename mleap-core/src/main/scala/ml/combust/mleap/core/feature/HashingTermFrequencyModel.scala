package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable

/** Class for hashing token frequencies into a vector.
  *
  * Source adapted from: Apache Spark Utils and HashingTF, see NOTICE for contributors
  *
  * @param numFeatures size of feature vector to hash into
  */
case class HashingTermFrequencyModel(numFeatures: Int = 1 << 18) {
  def indexOf(term: Any): Int = nonNegativeMod(term.##, numFeatures)

  def apply(document: Iterable[_]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    document.foreach { term =>
      val i = indexOf(term)
      termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
    }
    Vectors.sparse(numFeatures, termFrequencies.toSeq)
  }

  /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
 * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
 * so function return (x % mod) + mod in that case.
 */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}
