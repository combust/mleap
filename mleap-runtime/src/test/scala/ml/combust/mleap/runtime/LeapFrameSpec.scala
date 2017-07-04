package ml.combust.mleap.runtime

import java.io.{ByteArrayOutputStream, PrintStream}

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import org.scalatest.FunSpec
import resource._

/** Base trait for testing LeapFrame implementations.
  *
  * @tparam LF LeapFrame type
  */
trait LeapFrameSpec[LF <: LeapFrame[LF]] extends FunSpec {
  val fields = Seq(StructField("test_string", ScalarType.String),
    StructField("test_double", ScalarType.Double))
  val schema: StructType = StructType(fields).get
  val dataset = LocalDataset(Array(
    Row("hello", 42.13),
    Row("there", 13.42)
  ))
  val frame: LF = create(schema, dataset)

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

        describe("with ArraySelector and non Array[Any] data type") {
          val frame2 = frame.withField("test_double_2", Seq("test_double")) {
            (r: Seq[Double]) => r.head
          }

          assert(frame2.isFailure)
        }
      }

      describe("#withFields") {
        it("creates a new LeapFrame with fields added") {
          val f = (r: Double) => Row(r + 10, r.toString)
          val udf = UserDefinedFunction(f,
            Seq(ScalarType.Double, ScalarType.String),
            ScalarType.Double)

          val frame2 = frame.withFields(Seq("test_double_2", "test_double_string"), "test_double")(udf).get
          val data = frame2.dataset.toArray

          assert(frame2.schema.fields.length == 4)
          assert(frame2.schema.indexOf("test_double_2").get == 2)
          assert(frame2.schema.indexOf("test_double_string").get == 3)
          assert(data(0).getDouble(2) == 52.13)
          assert(data(1).getDouble(2) == 23.42)
          assert(data(0).getString(3) == "42.13")
          assert(data(1).getString(3) == "13.42")
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

      describe("#show") {
        it("prints the leap frame to a PrintStream") {
          val str = (for(out <- managed(new ByteArrayOutputStream())) yield {
            val p = new PrintStream(out)
            frame.show(p)
            out.flush()
            new String(out.toByteArray)
          }).tried.get

          val expected =
            """
              |+-----------+-----------+
              ||test_string|test_double|
              |+-----------+-----------+
              ||      hello|      42.13|
              ||      there|      13.42|
              |+-----------+-----------+
            """.stripMargin

          assert(str.trim == expected.trim)
        }
      }
    }
  }
}

class DefaultLeapFrameSpec extends LeapFrameSpec[DefaultLeapFrame] {
  override def create(schema: StructType, dataset: Dataset): DefaultLeapFrame = DefaultLeapFrame(schema, dataset)

  it should behave like leapFrame("DefaultLeapFrame")
}
