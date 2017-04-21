package ml.combust.mleap.runtime.reflection

import ml.combust.mleap.runtime.test.MyCustomObject
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/21/16.
  */
class MleapReflectionSpec extends FunSpec {
  describe("#dataType") {
    import MleapReflection.dataType

    it("returns the Mleap runtime data type from the Scala type") {
      assert(dataType[Boolean] == BooleanType())
      assert(dataType[String] == StringType())
      assert(dataType[Int] == IntegerType())
      assert(dataType[Long] == LongType())
      assert(dataType[Float] == FloatType())
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
      assert(dataType[Tensor[Double]] == TensorType(DoubleType()))
      assert(dataType[Any] == AnyType())
      assert(dataType[(String, Double)] == TupleDataType(StringType(), DoubleType()))
    }

    describe("#with an invalid Scala type") {
      it("throws an illegal argument exception") {
        assertThrows[IllegalArgumentException] { dataType[FunSpec] }
      }
    }
  }

  describe("#extractConstructorParameters") {
    import MleapReflection.extractConstructorParameters

    it("extracts constructor parameters from simple case class") {
      val params = extractConstructorParameters[DummyData]
      assert(params.map(_._1).toArray sameElements Array("d", "a", "b"))
    }

    it("extracts constructor parameters from case class with custom type") {
      val params = extractConstructorParameters[CustomData]
      assert(params.map(_._1).toArray sameElements Array("t", "b"))
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
    import MleapReflection.newInstance

    it("creates new instance of simple case class") {
      val dummyData = newInstance[DummyData](Seq(1, "hello world", 1.1))
      assert(dummyData.productIterator.toArray sameElements Array(1, "hello world", 1.1))
    }

    it("creates new instance of case class with custom type") {
      val customData = newInstance[CustomData](Seq(MyCustomObject("hello"), "world"))
      assert(customData.productIterator.toArray sameElements Array(MyCustomObject("hello"), "world"))
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
case class CustomData(t: MyCustomObject, b:String)
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
