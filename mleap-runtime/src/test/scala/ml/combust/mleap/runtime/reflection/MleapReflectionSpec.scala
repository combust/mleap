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
      assert(dataType[Boolean] == BooleanType())
      assert(dataType[String] == StringType())
      assert(dataType[Int] == IntegerType())
      assert(dataType[Long] == LongType())
      assert(dataType[Double] == DoubleType())
      assert(dataType[Seq[Boolean]] == ListType(BooleanType()))
      assert(dataType[Seq[String]] == ListType(StringType()))
      assert(dataType[Seq[Int]] == ListType(IntegerType()))
      assert(dataType[Seq[Long]] == ListType(LongType()))
      assert(dataType[Seq[Double]] == ListType(DoubleType()))
      assert(dataType[Seq[Seq[Boolean]]] == ListType(ListType(BooleanType())))
      assert(dataType[Seq[Seq[String]]] == ListType(ListType(StringType())))
      assert(dataType[Seq[Seq[Int]]] == ListType(ListType(IntegerType())))
      assert(dataType[Seq[Seq[Long]]] == ListType(ListType(LongType())))
      assert(dataType[Seq[Seq[Double]]] == ListType(ListType(DoubleType())))
      assert(dataType[Option[Boolean]] == BooleanType(true))
      assert(dataType[Option[String]] == StringType(true))
      assert(dataType[Option[Int]] == IntegerType(true))
      assert(dataType[Option[Long]] == LongType(true))
      assert(dataType[Option[Double]] == DoubleType(true))
      assert(dataType[Vector] == TensorType.doubleVector())
      assert(dataType[Any] == AnyType())
    }

    describe("#with an invalid Scala type") {
      it("throws an illegal argument exception") {
        assertThrows[IllegalArgumentException] { dataType[FunSpec] }
      }
    }
  }
}
