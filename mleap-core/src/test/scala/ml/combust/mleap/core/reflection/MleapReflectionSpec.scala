package ml.combust.mleap.core.reflection

import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/21/16.
  */
class MleapReflectionSpec extends FunSpec {
  describe("#dataType") {
    import ml.combust.mleap.core.reflection.MleapReflection.dataType

    it("returns the Mleap runtime data type from the Scala type") {
      assert(dataType[Boolean] == ScalarType.Boolean)
      assert(dataType[String] == ScalarType.String)
      assert(dataType[Int] == ScalarType.Int)
      assert(dataType[Long] == ScalarType.Long)
      assert(dataType[Float] == ScalarType.Float)
      assert(dataType[Double] == ScalarType.Double)
      assert(dataType[Seq[Boolean]] == ListType(BasicType.Boolean))
      assert(dataType[Seq[String]] == ListType(BasicType.String))
      assert(dataType[Seq[Int]] == ListType(BasicType.Int))
      assert(dataType[Seq[Long]] == ListType(BasicType.Long))
      assert(dataType[Seq[Double]] == ListType(BasicType.Double))
      assert(dataType[Option[Boolean]] == ScalarType.Boolean.asNullable)
      assert(dataType[Option[String]] == ScalarType.String.asNullable)
      assert(dataType[Option[Int]] == ScalarType.Int.asNullable)
      assert(dataType[Option[Long]] == ScalarType.Long.asNullable)
      assert(dataType[Option[Double]] == ScalarType.Double.asNullable)
      assert(dataType[Tensor[Double]] == TensorType(BasicType.Double))
      assert(dataType[Any] == AnyType())
      assert(dataType[(String, Double)] == TupleType(ScalarType.String, ScalarType.Double))
    }

    describe("#with an invalid Scala type") {
      it("throws an illegal argument exception") {
        assertThrows[IllegalArgumentException] { dataType[FunSpec] }
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
