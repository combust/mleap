package ml.combust.mleap.runtime.reflection

import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types._
import org.apache.spark.ml.linalg.Vector
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/21/16.
  */
class MleapReflectionSpec extends FunSpec {
  describe("#dataType") {
    import MleapReflection.dataType
    implicit val context = MleapContext()

    it("returns the Mleap runtime data type from the Scala type") {
      assert(dataType[Boolean] == BooleanType)
      assert(dataType[String] == StringType)
      assert(dataType[Int] == IntegerType)
      assert(dataType[Long] == LongType)
      assert(dataType[Double] == DoubleType)
      assert(dataType[Array[Boolean]] == ArrayType(BooleanType))
      assert(dataType[Array[String]] == ArrayType(StringType))
      assert(dataType[Array[Int]] == ArrayType(IntegerType))
      assert(dataType[Array[Long]] == ArrayType(LongType))
      assert(dataType[Array[Double]] == ArrayType(DoubleType))
      assert(dataType[Array[Array[Boolean]]] == ArrayType(ArrayType(BooleanType)))
      assert(dataType[Array[Array[String]]] == ArrayType(ArrayType(StringType)))
      assert(dataType[Array[Array[Int]]] == ArrayType(ArrayType(IntegerType)))
      assert(dataType[Array[Array[Long]]] == ArrayType(ArrayType(LongType)))
      assert(dataType[Array[Array[Double]]] == ArrayType(ArrayType(DoubleType)))
      assert(dataType[Vector] == TensorType.doubleVector())
      assert(dataType[Any] == AnyType)
    }

    describe("#with an invalid Scala type") {
      it("throws an illegal argument exception") {
        assertThrows[IllegalArgumentException] { dataType[FunSpec] }
      }
    }
  }
}
