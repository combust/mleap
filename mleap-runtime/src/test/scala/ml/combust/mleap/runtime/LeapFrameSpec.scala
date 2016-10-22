package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.FunSpec

/** Base trait for testing LeapFrame implementations.
  *
  * @tparam LF LeapFrame type
  */
trait LeapFrameSpec[LF <: LeapFrame[LF]] extends FunSpec {
  val fields = Seq(StructField("test_string", StringType),
    StructField("test_double", DoubleType))
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

        describe("with non-matching data types") {
          it("returns a failure") {
            val frame2 = frame.withField("test_double_2", "test_double") {
              (r: Int) => r + 10
            }

            assert(frame2.isFailure)
          }
        }
      }

      describe("#withFields") {
        it("creates a new LeapFrame with multiple fields added") {
          val newFields = Seq(StructField("test_string_2", StringType),
            StructField("test_double_2", DoubleType))
          val frame2 = frame.withFields(newFields) {
            r => Row(s"${r.getString(0)}:77", r.getDouble(1) + 20)
          }.get

          val data = frame2.dataset.toArray

          assert(frame2.schema.fields.length == 4)
          assert(frame2.schema.indexOf("test_string_2").get == 2)
          assert(frame2.schema.indexOf("test_double_2").get == 3)
          assert(data(0).toArray sameElements Array("hello", 42.13, "hello:77", 62.13))
          assert(data(1).toArray sameElements Array("there", 13.42, "there:77", 33.42))
        }

        describe("with already existing fields") {
          it("returns a failure") {
            val newFields = Seq(StructField("test_string", StringType),
              StructField("test_double", DoubleType))
            val frame2 = frame.withFields(newFields) {
              r => Row(s"${r.getString(0)}:77", r.getDouble(1) + 20)
            }

            assert(frame2.isFailure)
          }
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
