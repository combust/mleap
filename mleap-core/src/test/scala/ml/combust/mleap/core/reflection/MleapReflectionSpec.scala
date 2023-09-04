package ml.combust.mleap.core.reflection

import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.{ByteString, Tensor}
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hollinwilkins on 10/21/16.
  */
class MleapReflectionSpec extends AnyFunSpec {
  describe("#dataType") {
    import ml.combust.mleap.core.reflection.MleapReflection.dataType

    it("returns the Mleap runtime data type from the Scala type") {
      assert(dataType[Boolean] == ScalarType.Boolean.nonNullable)
      assert(dataType[Int] == ScalarType.Int.nonNullable)
      assert(dataType[Long] == ScalarType.Long.nonNullable)
      assert(dataType[Float] == ScalarType.Float.nonNullable)
      assert(dataType[Double] == ScalarType.Double.nonNullable)
      assert(dataType[String] == ScalarType.String)
      assert(dataType[ByteString] == ScalarType.ByteString)
      assert(dataType[Seq[Boolean]] == ListType(BasicType.Boolean))
      assert(dataType[Seq[String]] == ListType(BasicType.String))
      assert(dataType[Seq[Int]] == ListType(BasicType.Int))
      assert(dataType[Seq[Long]] == ListType(BasicType.Long))
      assert(dataType[Seq[Double]] == ListType(BasicType.Double))
      assert(dataType[java.lang.Boolean] == ScalarType.Boolean)
      assert(dataType[java.lang.Integer] == ScalarType.Int)
      assert(dataType[java.lang.Long] == ScalarType.Long)
      assert(dataType[java.lang.Double] == ScalarType.Double)
      assert(dataType[Tensor[Double]] == TensorType(BasicType.Double))
      assert(dataType[Map[String, String]] == MapType(BasicType.String, BasicType.String))
      assert(dataType[Map[String, Int]] == MapType(BasicType.String, BasicType.Int))
      assert(dataType[Map[String, Double]] == MapType(BasicType.String, BasicType.Double))
      assert(dataType[Map[String, Boolean]] == MapType(BasicType.String, BasicType.Boolean))
      assert(dataType[Map[Int, String]] == MapType(BasicType.Int, BasicType.String))
      assert(dataType[Map[Int, Int]] == MapType(BasicType.Int, BasicType.Int))
      assert(dataType[Map[Int, Double]] == MapType(BasicType.Int, BasicType.Double))
      assert(dataType[Map[Int, Boolean]] == MapType(BasicType.Int, BasicType.Boolean))
      assert(dataType[Map[Double, String]] == MapType(BasicType.Double, BasicType.String))
      assert(dataType[Map[Double, Int]] == MapType(BasicType.Double, BasicType.Int))
      assert(dataType[Map[Double, Double]] == MapType(BasicType.Double, BasicType.Double))
      assert(dataType[Map[Double, Boolean]] == MapType(BasicType.Double, BasicType.Boolean))
      assert(dataType[Map[Boolean, String]] == MapType(BasicType.Boolean, BasicType.String))
      assert(dataType[Map[Boolean, Int]] == MapType(BasicType.Boolean, BasicType.Int))
      assert(dataType[Map[Boolean, Double]] == MapType(BasicType.Boolean, BasicType.Double))
      assert(dataType[Map[Boolean, Boolean]] == MapType(BasicType.Boolean, BasicType.Boolean))
    }

    describe("#with an invalid Scala type") {
      it("throws an illegal argument exception") {
        assertThrows[IllegalArgumentException] { dataType[AnyFunSpec] }
      }
    }
  }

  describe("#extractConstructorParameters") {
    import ml.combust.mleap.core.reflection.MleapReflection.extractConstructorParameters

    it("extracts constructor parameters from simple case class") {
      val params = extractConstructorParameters[DummyData]
      assert(params.map(_._1).toArray sameElements Array("d", "a", "b"))
    }

    it("extracts constructor parameters from case classes with overloaded constructors") {
      val employeeParams = extractConstructorParameters[EmployeeData]
      assert(employeeParams.map(_._1).toArray sameElements Array("n", "s"))

      val personParams = extractConstructorParameters[PersonData]
      assert(personParams.map(_._1).toArray sameElements Array("name", "age"))
    }

    it("throws an illegal argument exception with ordinary class") {
      assertThrows[IllegalArgumentException] { extractConstructorParameters[SimpleData] }
    }

    it("throws an illegal argument exception with Product classes that are not case classes") {
      assertThrows[IllegalArgumentException] { extractConstructorParameters[List[_]] }
    }
  }

  describe("#newInstance") {
    import ml.combust.mleap.core.reflection.MleapReflection.newInstance

    it("creates new instance of simple case class") {
      val dummyData = newInstance[DummyData](Seq(1, "hello world", 1.1))
      assert(dummyData.productIterator.toArray sameElements Array(1, "hello world", 1.1))
    }

    it("creates new instances of case classes with overloaded constructors") {
      val employeeData = newInstance[EmployeeData](Seq("Test", 2.0))
      assert(employeeData.productIterator.toArray sameElements Array("Test", 2.0))

      val personData = newInstance[PersonData](Seq("Test", 28))
      assert(personData.productIterator.toArray sameElements Array("Test", 28))
    }

    it("throws an illegal argument exception with ordinary class") {
      assertThrows[IllegalArgumentException] { newInstance[SimpleData](Seq(23)) }
    }

    it("throws an illegal argument exception with Product classes that are not case classes") {
      assertThrows[IllegalArgumentException] { newInstance[List[_]](Seq(2,3,4)) }
    }

    it("doesn't accumulate memory") {
      val before = MleapReflection.universe.asInstanceOf[scala.reflect.runtime.JavaUniverse].undoLog.log.size
      for (_ <- 0 until 1000) {
        MleapReflection.extractConstructorParameters[LargeFeatures]
      }
      val after = MleapReflection.universe.asInstanceOf[scala.reflect.runtime.JavaUniverse].undoLog.log.size
      assert(before == after)
    }
  }
}

case class DummyData(d: Int, a: String, b:Double)
class SimpleData(val d: Int)

case class EmployeeData(n: String, s: Double) {
  def this() = this("<no name>", 0.0)

  val name: String = n
  val salary: Double = s
}

case class PersonData(var name: String, var age: Int)
object PersonData {
  def apply() = new PersonData("<no name>", 0)
  def apply(name: String) = new PersonData(name, 0)
}
case class LargeFeatures(
                     a: String,
                     b: String,
                     c: String,
                     d: String,
                     e: String,
                     f: String,
                     g: String,
                     h: String,
                     i: List[String])
