package ml.combust.mleap.core.types

import java.io.{ByteArrayOutputStream, PrintStream}

import org.scalatest.{FunSuite, GivenWhenThen, TryValues}

import scala.util.Success

/**
  * Created by pahsan on 3/8/16.
  */
class StructTypeSpec extends FunSuite with GivenWhenThen with TryValues{
  val fields = Seq(StructField("first", ScalarType.String),
                   StructField("second", ScalarType.String),
                   StructField("third", ScalarType.String),
                   StructField("fourth", ScalarType.String),
                   StructField("fifth", ScalarType.String)
  )

  val testStruct = StructType(fields).get

  test("getField should return the desired field wrapped in an Option") {
    assert(testStruct.getField("first").get.name == "first")
  }

  test("indexOf should return the integer index of the desired field") {
    assert(testStruct.indexOf("first") == Success(0))
  }

  test("contains should return true whenever a field exists") {
    assert(fields.map(f => testStruct.hasField(f.name)).forall(identity))
  }

  test("contains should return false when a field doesn't exist") {
    val fieldsPrime = fields:+StructField("sixth", ScalarType.String)

    assert(!fieldsPrime.map(f => testStruct.hasField(f.name)).forall(identity))
  }

  test("withField should return a StructType with the field added") {
    val field = StructField("sixth", ScalarType.String)

    assert(testStruct.withField(field).get.hasField(field.name))
  }

  test("withFields should return a StructType with the fields added") {
    val fields = Seq(StructField("sixth", ScalarType.String), StructField("seventh", ScalarType.Double))

    assert(testStruct.withFields(fields).get.hasField("sixth"))
    assert(testStruct.withFields(fields).get.hasField("seventh"))
  }
  
  test("Dropping a field from a StructType should remove the field") {
    assert(testStruct.dropField("first").get.getField("first").isEmpty)
    assert(testStruct.dropField("first").get.fields.length == testStruct.fields.length - 1)
  }

  test("select should return a StructType with selected fields") {
    Given("an array of valid String field names")
    val selection = Array("first", "second", "third")

    When("a selection is made")
    val selectedFields = testStruct.select(selection:_*)

    Then("the operation should return a success")
    assert(selectedFields.isSuccess)

    And("the StructType should contain the selected fields")
    assert(selection.map(f => selectedFields.success.value.hasField(f)).forall(identity))
  }

  test("indicesOf should return the correct indices for valid fields") {
    Given("an array of valid String field names")
    val selection = Array("fifth", "second", "fourth")

    When("indicesOf is invoked")
    val indices = testStruct.indicesOf(selection:_*)

    Then("the returned object should be a successful Seq[Int]")
    assert(indices.isSuccess)

    And("the Seq should contain the correct indices")
    val sequence = indices.success.value
    assert(Seq(4, 1, 3).map(i => sequence.contains(i)).forall(identity))

    And("they should be in order")
    assert(Seq(4, 1, 3) == sequence)
  }

  test("indicesOf should return a failure for invalid fields") {
    Given("an array of invalid field names")
    val names = Array("sixth", "seventh")

    When("tryIndicesOf is invoked")
    val failed = testStruct.indicesOf(names:_*)

    Then("the returned object should be a failure")
    assert(failed.isFailure)
  }

  test("indexOf should return the correct index for a valid field") {
    Given("a valid String field name")
    val name = "fifth"

    When("indexOf is invoked")
    val successfulIndex = testStruct.indexOf(name)

    Then("the returned object should be a successful Int")
    assert(successfulIndex.isSuccess)

    And("the try should contain the correct index")
    val index = successfulIndex.success.value
    assert(index == 4)
  }

  test("indexOf should return a failure for an invalid field") {
    assert(testStruct.indexOf("sixth").isFailure)
  }

  test("prints the schema to a PrintStream") {
    val printStruct = StructType(StructField("a_double", ScalarType.Double),
      StructField("a_list", ListType(BasicType.Float).nonNullable),
      StructField("a_tensor", TensorType(BasicType.Byte))).get

    val out = new ByteArrayOutputStream()
    val print = new PrintStream(out)
    printStruct.print(print)
    print.flush()
    val schema = new String(out.toByteArray)
    print.close()

    val expected =
      """
        |root
        | |-- a_double: scalar(base=double,nullable=true)
        | |-- a_list: list(base=float,nullable=false)
        | |-- a_tensor: tensor(base=byte,nullable=true)
      """.stripMargin

    assert(schema.trim == expected.trim)
  }
}
