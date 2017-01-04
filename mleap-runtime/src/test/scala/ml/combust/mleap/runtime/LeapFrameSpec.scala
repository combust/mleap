package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.test.{MyCustomObject, MyCustomType}
import ml.combust.mleap.runtime.types._
import org.scalatest.FunSpec

/** Base trait for testing LeapFrame implementations.
  *
  * @tparam LF LeapFrame type
  */
trait LeapFrameSpec[LF <: LeapFrame[LF]] extends FunSpec {
  val fields = Seq(StructField("test_string", StringType()),
    StructField("test_double", DoubleType()))
  val schema = StructType(fields).get
  val dataset = LocalDataset(Array(
    Row("hello", 42.13),
    Row("there", 13.42)
  ))
  val frame = create(schema, dataset)

  def create(schema: StructType, dataset: Dataset): LF

  def leapFrame(name: String): Unit = {
    describe(name) {
      describe("#schema") {
        it("gets the schema") { assert(frame.schema == schema) }
      }

      describe("#dataset") {
        it("gets the dataset") { assert(frame.dataset == dataset) }
      }

      describe("#select") {
        it("creates a new LeapFrame from selected fields") {
          val frame2 = frame.select("test_double").get
          val data = frame2.dataset.toArray

          assert(frame2.schema.fields.length == 1)
          assert(frame2.schema.indexOf("test_double").get == 0)
          assert(data(0).getDouble(0) == 42.13)
          assert(data(1).getDouble(0) == 13.42)
        }

        describe("with invalid selection") {
          it("returns a Failure") { assert(frame.select("non_existent_field").isFailure) }
        }
      }

      describe("#withField") {
        it("creates a new LeapFrame with field added") {
          val frame2 = frame.withField("test_double_2", "test_double") {
            (r: Double) => r + 10
          }.get
          val data = frame2.dataset.toArray

          assert(frame2.schema.fields.length == 3)
          assert(frame2.schema.indexOf("test_double_2").get == 2)
          assert(data(0).getDouble(2) == 52.13)
          assert(data(1).getDouble(2) == 23.42)
        }

        describe("with a custom data type") {
          val frame2 = frame.withField("test_custom", "test_string") {
            (v: String) => MyCustomObject(v)
          }.flatMap(_.select("test_custom")).get
          val data = frame2.dataset.toArray

          assert(frame2.schema.getField("test_custom").get.dataType == MleapContext.defaultContext.customType[MyCustomObject])
          assert(data(0).getAs[MyCustomObject](0) == MyCustomObject("hello"))
          assert(data(1).getAs[MyCustomObject](0) == MyCustomObject("there"))
        }

        describe("with non-matching data types") {
          it("returns a failure") {
            val frame2 = frame.withField("test_double_2", "test_double") {
              (r: Int) => r + 10
            }

            assert(frame2.isFailure)
          }
        }

        describe("with ArraySelector and non Array[Any] data type") {
          val frame2 = frame.withField("test_double_2", Array("test_double")) {
            (r: Seq[Double]) => r.head
          }

          assert(frame2.isFailure)
        }
      }

      describe("#dropField") {
        it("creates a new LeapFrame with field dropped") {
          val frame2 = frame.dropField("test_string").get

          assert(frame2.schema.fields.map(_.name) == Seq("test_double"))
        }

        describe("with a non-existent field") {
          it("returns a Failure") { assert(frame.dropField("non_existent").isFailure) }
        }
      }
    }
  }
}

class DefaultLeapFrameSpec extends LeapFrameSpec[DefaultLeapFrame] {
  override def create(schema: StructType, dataset: Dataset): DefaultLeapFrame = DefaultLeapFrame(schema, dataset)

  it should behave like leapFrame("DefaultLeapFrame")
}
