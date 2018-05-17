package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.util.Platform
import ml.combust.mleap.core.util.Murmur3_x86_32.{hashInt, hashLong, hashUnsafeBytes2}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable

object FeatureHasherModel {
  val seed = HashingTermFrequencyModel.seed

  def murmur3(term: Any): Int = {
    term match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case s: String =>
        val utf8 = s.getBytes("UTF-8")
        hashUnsafeBytes2(utf8, Platform.BYTE_ARRAY_OFFSET, utf8.length, seed)
      case _ => throw new IllegalStateException("FeatureHasher with murmur3 algorithm does not " +
        s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }

}

/** Feature hashing projects a set of categorical or numerical features into a feature vector of
  * specified dimension (typically substantially smaller than that of the original feature
  * space). This is done using the hashing trick (https://en.wikipedia.org/wiki/Feature_hashing)
  * to map features to indices in the feature vector.
  *
  * Source adapted from: Apache Spark Utils and FeatureHasher, see NOTICE for contributors
  *
  * @param numFeatures size of feature vector to hash into
  * @param categoricalCols Numeric columns to treat as categorical features
  * @param inputTypes the datatypes of the inputs
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.3.0/mllib/src/main/scala/org/apache/spark/ml/feature/FeatureHasher.scala")
case class FeatureHasherModel(numFeatures: Int = 1 << 18, 
                              categoricalCols: Seq[String],
                              inputNames: Seq[String],
                              inputTypes: Seq[DataType]
                             ) extends Model  {
  assert(inputTypes.forall(dt ⇒ dt.shape.isScalar), "must provide scalar shapes as inputs")

  val schema = inputNames.zip(inputTypes)
  val realFields = schema.filter(t ⇒ t._2.base match {
    case BasicType.Short if !categoricalCols.contains(t._1) ⇒ true
    case BasicType.Double if !categoricalCols.contains(t._1) ⇒ true
    case BasicType.Float if !categoricalCols.contains(t._1) ⇒ true
    case BasicType.Int if !categoricalCols.contains(t._1) ⇒ true
    case BasicType.Long if !categoricalCols.contains(t._1) ⇒ true
    case _ ⇒ false
  }).toMap.keys.toSeq

  def getDouble(x: Any): Double = {
    x match {
      case n: java.lang.Number ⇒ n.doubleValue()
      // will throw ClassCastException if it cannot be cast, as would row.getDouble
      case other ⇒ other.asInstanceOf[Double]
    }
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def apply(things: Seq[Any]): Vector = {
    val map = new mutable.OpenHashMap[Int, Double]()
    schema.zip(things).foreach { case (sc, item) ⇒
      if (item != null) {
        val (rawIdx, value) = if (realFields.contains(sc._1)) {
          // numeric values are kept as is, with vector index based on hash of "column_name"
          val value = getDouble(item)
          val hash = FeatureHasherModel.murmur3(sc._1)
          (hash, value)
        } else {
          // string, boolean and numeric values that are in catCols are treated as categorical,
          // with an indicator value of 1.0 and vector index based on hash of "column_name=value"
          val value = item.toString
          val fieldName = s"${sc._1}=$value"
          val hash = FeatureHasherModel.murmur3(fieldName)
          (hash, 1.0)
        }
        val idx = nonNegativeMod(rawIdx, numFeatures)
        map.+=((idx, map.getOrElse(idx, 0.0) + value))
      }
    }


    // TODO: Figure this out
    Vectors.sparse(numFeatures, map.toSeq)
  }
  
  override def inputSchema: StructType = {
    val inputFields = inputTypes.zipWithIndex.map {
      case (dtype, i) => StructField(s"input$i", dtype)
    }
    StructType(inputFields).get
  }

  override def outputSchema: StructType = {
    StructType(StructField("output" -> TensorType.Double(numFeatures))).get
  }
}