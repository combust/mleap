package ml.combust.mleap.runtime.reflection

import ml.combust.mleap.runtime.types._
import org.apache.spark.ml.linalg.Vector
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/21/16.
  */
class MleapReflectionSpec extends FunSpec {
  describe("#dataType") {
    import MleapReflection.dataType

    it("returns the Mleap runtime data type from the Scala type") {
      assert(dataType[Boolean] == BooleanType)
      assert(dataType[String] == StringType)
      assert(dataType[Int] == IntegerType)
      assert(dataType[Long] == LongType)
      assert(dataType[Double] == DoubleType)
      assert(dataType[Array[Boolean]] == ListType(BooleanType))
      assert(dataType[Array[String]] == ListType(StringType))
      assert(dataType[Array[Int]] == ListType(IntegerType))
      assert(dataType[Array[Long]] == ListType(LongType))
      assert(dataType[Array[Double]] == ListType(DoubleType))
      assert(dataType[Array[Array[Boolean]]] == ListType(ListType(BooleanType)))
      assert(dataType[Array[Array[String]]] == ListType(ListType(StringType)))
      assert(dataType[Array[Array[Int]]] == ListType(ListType(IntegerType)))
      assert(dataType[Array[Array[Long]]] == ListType(ListType(LongType)))
      assert(dataType[Array[Array[Double]]] == ListType(ListType(DoubleType)))
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
