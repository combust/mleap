package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{BasicType, ListType, StructType, TensorType}
import ml.combust.mleap.tensor.{SparseTensor, Tensor}

import scala.collection.{SortedMap, mutable}

/**
  * Created by hollinwilkins on 12/28/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/CountVectorizer.scala")
case class CountVectorizerModel(vocabulary: Array[String],
                                binary: Boolean,
                                minTf: Double) extends Model {
  val dict: Map[String, Int] = vocabulary.zipWithIndex.toMap
  val outputSize = dict.size
  // The Seq created below are required by SparseTensor api during initialization
  // For performance optimization, we initialize these sequences here so we don't have to at runtime
  val seqCache: Array[Seq[Int]] = {
    val arr = mutable.ArrayBuilder.make[Seq[Int]]
    for (i <- 0 to outputSize){
      arr += Seq(i)
    }
    arr.result()
  }
  def _apply(document: Seq[String]): (Seq[Seq[Int]], Array[Double]) = {
    var termCounts = SortedMap[Int, Double]()
    var tokenCount = 0L
    document.foreach {
      term =>
        dict.get(term) match {
          case Some(index) => termCounts += (index -> termCounts.get(index).map(_ + 1).getOrElse(1))
          case None => // ignore terms not found in dictionary
        }
        tokenCount += 1
    }
    val valuesArray = mutable.ArrayBuilder.make[Double]
    val indicesArray = mutable.ArrayBuilder.make[Seq[Int]]
    val effectiveMinTF = if (minTf >= 1.0) minTf else tokenCount * minTf
    val iterator = termCounts.iterator
    if (binary){
      while (iterator.hasNext){
        val (termIndex, count) = iterator.next()
        if (count>= effectiveMinTF){
          valuesArray += 1.0
          indicesArray += seqCache(termIndex)
        }
      }
    }else{
      while (iterator.hasNext){
        val (termIndex, count) = iterator.next()
        if (count>= effectiveMinTF){
          valuesArray += count
          indicesArray += seqCache(termIndex)
        }
      }
    }
    Tuple2(indicesArray.result(), valuesArray.result())
  }


  def apply(document: Seq[String]): Tensor[Double] = {
    val (indices, values) = _apply(document)
    SparseTensor(indices, values, Seq(outputSize))
  }

  override def inputSchema: StructType = StructType("input" -> ListType(BasicType.String)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(dict.size)).get
}
