package ml.combust.mleap.runtime.types

import org.scalatest.{FunSuite, GivenWhenThen, TryValues}

import scala.util.Success

/**
  * Created by pahsan on 3/8/16.
  */
class StructTypeSpec extends FunSuite with GivenWhenThen with TryValues{
  val fields = Seq(StructField("first", StringType),
                   StructField("second", StringType),
                   StructField("third", StringType),
                   StructField("fourth", StringType),
                   StructField("fifth", StringType)
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
    val fieldsPrime = fields:+StructField("sixth", StringType)

    assert(!fieldsPrime.map(f => testStruct.hasField(f.name)).forall(identity))
  }

  test("withField should return a StructType with the field added") {
    val field = StructField("sixth", StringType)

    assert(testStruct.withField(field).get.hasField(field.name))
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
    val selection = Array("first", "fifth", "second")

    When("indicesOf is invoked")
    val indices = testStruct.indicesOf(selection:_*).get

    Then("the returned Seq should contain the correct indices")
    assert(Seq(0, 4, 1).map(i => indices.contains(i)).forall(identity))

    And("they should be in order")
    assert(Seq(0, 4, 1) == indices)
  }

  test("tryIndicesOf should return the correct indices for valid fields") {
    Given("an array of valid String field names")
    val selection = Array("fifth", "second", "fourth")

    When("tryIndicesOf is invoked")
    val indices = testStruct.indicesOf(selection:_*)

    Then("the returned object should be a successful Seq[Int]")
    assert(indices.isSuccess)

    And("the Seq should contain the correct indices")
    val sequence = indices.success.value
    assert(Seq(4, 1, 3).map(i => sequence.contains(i)).forall(identity))

    And("they should be in order")
    assert(Seq(4, 1, 3) == sequence)
  }

  test("tryIndicesOf should return a failure for invalid fields") {
    Given("an array of invalid field names")
    val names = Array("sixth", "seventh")

    When("tryIndicesOf is invoked")
    val failed = testStruct.indicesOf(names:_*)

    Then("the returned object should be a failure")
    assert(failed.isFailure)
  }

  test("tryIndexOf should return the correct index for a valid field") {
    Given("a valid String field name")
    val name = "fifth"

    When("tryIndexOf is invoked")
    val successfulIndex = testStruct.indexOf(name)

    Then("the returned object should be a successful Int")
    assert(successfulIndex.isSuccess)

    And("the Seq should contain the correct indices")
    val index = successfulIndex.success.value
    assert(index == 4)
  }

  test("tryIndexOf should return a failure for an invalid field") {
    assert(testStruct.indexOf("sixth").isFailure)
  }
}
