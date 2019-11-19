package ml.combust.mleap.runtime.frame

import java.io.{ByteArrayOutputStream, PrintStream}

import ml.combust.mleap.core.types.{SchemaSpec, _}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.ByteString
import org.scalatest.FunSpec

/** Base trait for testing LeapFrame implementations.
  *
  * @tparam LF LeapFrame type
  */
trait LeapFrameSpec[LF <: LeapFrame[LF]] extends FunSpec {
  val fields = Seq(StructField("test_string", ScalarType.String),
    StructField("test_double", ScalarType.Double))
  val schema: StructType = StructType(fields).get
  val dataset = Seq(
    Row("hello", 42.13),
    Row("there", 13.42)
  )
  val frame: LF = create(schema, dataset)

  def create(schema: StructType, dataset: Seq[Row]): LF

  def leapFrame(name: String): Unit = {
    describe(name) {
      describe("#schema") {
        it("gets the schema") { assert(frame.schema == schema) }
      }

      describe("#select") {
        it("creates a new LeapFrame from selected fields") {
          val frame2 = frame.select("test_double").get
          val data = frame2.collect()

          assert(frame2.schema.fields.length == 1)
          assert(frame2.schema.indexOf("test_double").get == 0)
          assert(data.head.getDouble(0) == 42.13)
          assert(data(1).getDouble(0) == 13.42)
        }

        describe("with invalid selection") {
          it("returns a Failure") { assert(frame.select("non_existent_field").isFailure) }
        }
      }

      describe("#select relaxed") {
        it("creates a new LeapFrame from selected fields, ignoring unknown fields") {
          val frame2 = frame.relaxedSelect("test_double", "dummy", "does_not_exist")
          val data = frame2.collect()

          assert(frame2.schema.fields.length == 1)
          assert(frame2.schema.indexOf("test_double").get == 0)
          assert(data.head.getDouble(0) == 42.13)
          assert(data(1).getDouble(0) == 13.42)
        }
      }

      describe("#withColumn") {
        it("creates a new LeapFrame with column added") {
          val frame2 = frame.withColumn("test_double_2", "test_double") {
            (r: Double) => r + 10
          }.get
          val data = frame2.collect()

          assert(frame2.schema.fields.length == 3)
          assert(frame2.schema.indexOf("test_double_2").get == 2)
          assert(data.head.getDouble(2) == 52.13)
          assert(data(1).getDouble(2) == 23.42)
        }

        describe("with non-castable data types") {
          it("returns a failure") {
            val frame2 = frame.withColumn("test_double_2", "test_double") {
              (_: ByteString) => 22
            }

            assert(frame2.isFailure)
          }
        }

        describe("with ArraySelector and non Array[Any] data type") {
          val frame2 = frame.withColumn("test_double_2", Seq("test_double")) {
            (r: Seq[Double]) => r.head
          }

          assert(frame2.isFailure)
        }

        describe("automatic casting") {
          it("automatically casts the values to the UDF") {
            val frame2 = frame.withColumn("test_double_2", "test_double") {
              (r: Int) => r + 10
            }.get
            val data = frame2.collect()

            assert(frame2.schema.fields.length == 3)
            assert(frame2.schema.indexOf("test_double_2").get == 2)
            assert(data.head.getInt(2) == 52)
            assert(data(1).getInt(2) == 23)
          }

          it("automatically casts the data in a struct selector") {
            val f = (r: Row) => r.getString(0) + r.getString(1)
            val exec: UserDefinedFunction = UserDefinedFunction(f,
              ScalarType.String,
              Seq(SchemaSpec(Seq(ScalarType.String, ScalarType.String))))
            val frame2 = frame.withColumn("test_string2", Seq("test_string", "test_double"))(exec).get
            val data = frame2.collect()

            assert(frame2.schema.fields.length == 3)
            assert(data.head.getString(2) == "hello42.13")
          }
        }
      }

      describe("#withColumns") {
        it("creates a new LeapFrame with columns added") {
          val frame2 = frame.withColumns(Seq("test_double_2", "test_double_string"), "test_double"){
            (r: Double) => (r + 10, r.toString)
          }.get
          val data = frame2.collect()

          assert(frame2.schema.fields.length == 4)
          assert(frame2.schema.indexOf("test_double_2").get == 2)
          assert(frame2.schema.indexOf("test_double_string").get == 3)
          assert(data.head.getDouble(2) == 52.13)
          assert(data(1).getDouble(2) == 23.42)
          assert(data.head.getString(3) == "42.13")
          assert(data(1).getString(3) == "13.42")
        }
      }

      describe("#drop") {
        it("creates a new LeapFrame with field dropped") {
          val frame2 = frame.drop("test_string").get

          assert(frame2.schema.fields.map(_.name) == Seq("test_double"))
        }

        describe("with a non-existent field") {
          it("returns a Failure") { assert(frame.drop("non_existent").isFailure) }
        }
      }

      describe("#filter") {
        it("creates a new LeapFrame with rows filtered") {
          val frame2 = frame.filter("test_string") {
            (s: String) => s != "there"
          }.get

          assert(frame2.collect().length == 1)
          assert(frame2.collect().head.getString(0) == "hello")
          assert(frame2.collect().head.getDouble(1) == 42.13)
        }
      }

      describe("#show") {
        it("prints the leap frame to a PrintStream") {
          val out = new ByteArrayOutputStream()
          val p = new PrintStream(out)
          frame.show(p)
          out.flush()
          val str = new String(out.toByteArray)
          out.close()

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
  override def create(schema: StructType, dataset: Seq[Row]): DefaultLeapFrame = DefaultLeapFrame(schema, dataset)

  it should behave like leapFrame("DefaultLeapFrame")
}
