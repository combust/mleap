package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{BasicType, ListType, StructType, TensorType}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable

/**
  * Created by hollinwilkins on 12/28/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/CountVectorizer.scala")
case class CountVectorizerModel(vocabulary: Array[String],
                                binary: Boolean,
                                minTf: Double) extends Model {
  val dict: Map[String, Int] = vocabulary.zipWithIndex.toMap

  def apply(document: Seq[String]): Vector = {
    val termCounts = mutable.Map[Int, Double]()
    var tokenCount = 0L
    document.foreach {
      term =>
        dict.get(term) match {
          case Some(index) => termCounts += (index -> termCounts.get(index).map(_ + 1).getOrElse(1))
          case None => // ignore terms not found in dictionary
        }
        tokenCount += 1
    }

    val effectiveMinTF = if (minTf >= 1.0) minTf else tokenCount * minTf
    val effectiveCounts = if(binary) {
      termCounts.filter(_._2 >= effectiveMinTF).map(p => (p._1, 1.0)).toSeq
    } else {
      termCounts.filter(_._2 >= effectiveMinTF).toSeq
    }

    Vectors.sparse(dict.size, effectiveCounts)
  }

  override def inputSchema: StructType = StructType("input" -> ListType(BasicType.String)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(dict.size)).get
}
