package ml.combust.mleap.runtime.javadsl

import java.io.File
import java.nio.file.Files
import java.util

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer}
import ml.combust.mleap.runtime.transformer.feature.StringIndexer
import ml.combust.mleap.tensor.{ByteString, Tensor}
import org.scalatest.FunSpec

import scala.collection.JavaConverters._

/**
  * Created by hollinwilkins on 4/21/17.
  */
class JavaDSLSpec extends FunSpec {
  val builder = new LeapFrameBuilder

  val row = builder.createRow(true, "hello", Byte.box(1),
    Short.box(2), Int.box(3), Long.box(4),
    Float.box(34.5f), Double.box(44.5), ByteString("hello_there".getBytes()),
    Seq[Long](23, 44, 55).asJava, Tensor.denseVector[Byte](Array[Byte](23, 3, 4)))

  val stringIndexer = StringIndexer(shape = NodeShape().
    withStandardInput("string").
    withStandardOutput("string_index"),
    model = StringIndexerModel(Seq("hello")))

  def buildFrame(): DefaultLeapFrame = {
    val fields = util.Arrays.asList(builder.createField("bool", builder.createBoolean()),
                                    builder.createField("string", builder.createString()),
                                    builder.createField("byte", builder.createByte()),
                                    builder.createField("short", builder.createShort()),
                                    builder.createField("int", builder.createInt()),
                                    builder.createField("long", builder.createLong()),
                                    builder.createField("float", builder.createFloat()),
                                    builder.createField("double", builder.createDouble()),
                                    builder.createField("byte_string", builder.createByteString()),
                                    builder.createField("list", builder.createList(builder.createBasicLong())),
                                    builder.createField("tensor", builder.createTensor(builder.createBasicByte())))
    val schema = builder.createSchema(fields)
    builder.createFrame(schema, util.Arrays.asList(row))
  }

  describe("building a LeapFrame") {
    it("is able to build a LeapFrame with all data types") {
      val frame = buildFrame()
      val schema = frame.schema

      assert(schema.getField("bool").get == StructField("bool", ScalarType.Boolean))
      assert(schema.getField("string").get == StructField("string", ScalarType.String))
      assert(schema.getField("byte").get == StructField("byte", ScalarType.Byte))
      assert(schema.getField("short").get == StructField("short", ScalarType.Short))
      assert(schema.getField("int").get == StructField("int", ScalarType.Int))
      assert(schema.getField("long").get == StructField("long", ScalarType.Long))
      assert(schema.getField("float").get == StructField("float", ScalarType.Float))
      assert(schema.getField("double").get == StructField("double", ScalarType.Double))
      assert(schema.getField("byte_string").get == StructField("byte_string", ScalarType.ByteString))
      assert(schema.getField("list").get == StructField("list", ListType(BasicType.Long)))
      assert(schema.getField("tensor").get == StructField("tensor", TensorType(BasicType.Byte)))

      val d = frame.dataset
      assert(d.head.getBool(0))
      assert(d.head.getString(1) == "hello")
      assert(d.head.getByte(2) == 1)
      assert(d.head.getShort(3) == 2)
      assert(d.head.getInt(4) == 3)
      assert(d.head.getLong(5) == 4)
      assert(d.head.getFloat(6) == 34.5f)
      assert(d.head.getDouble(7) == 44.5)
      assert(d.head.getByteString(8) == ByteString("hello_there".getBytes))
      assert(d.head.getList(9).asScala == Seq[Long](23, 44, 55))
      assert(d.head.getTensor(10).toArray.toSeq == Seq[Byte](23, 3, 4))
    }
  }

  describe("using row transformer") {
    it("is able to transform a single row using a row transformer") {
      val rowTransformer = stringIndexer.transform(RowTransformer(buildFrame.schema)).get
      val result = rowTransformer.transform(row)
      assert(result.getDouble(11) == 0.0)
    }
  }

  describe("can use leap frame operations nicely") {
    val leapFrameSupport = new LeapFrameSupport()
    val frame = buildFrame()

    it("is able to collect all rows to a Java list") {
      val rows : util.List[Row] = leapFrameSupport.collect(frame)
      assert(rows.size() == 1)
    }

    it("is able to select fields given a Java list") {
      val smallerFrame = leapFrameSupport.select(frame, util.Arrays.asList("string", "bool"))
      assert(smallerFrame.schema.getField("bool").nonEmpty)
      assert(smallerFrame.schema.getField("string").nonEmpty)
      assert(smallerFrame.schema.getField("int").isEmpty)
    }

    it("is able to drop fields given a Java list") {
      val smallerFrame = leapFrameSupport.drop(frame, util.Arrays.asList("string", "bool"))
      assert(smallerFrame.schema.getField("bool").isEmpty)
      assert(smallerFrame.schema.getField("string").isEmpty)
      assert(smallerFrame.schema.getField("int").nonEmpty)
    }

    it("is able to get fields from the schema") {
      val fields = leapFrameSupport.getFields(frame.schema)
      assert(fields.size() == 11)
    }
  }

  describe("MLeap bundles") {
    val file = new File(Files.createTempDirectory("mleap").toFile, "model.zip")

    describe("saving/loading an MLeap transformer") {
      it("is able to save and load the transformer") {
        val context = new ContextBuilder().createMleapContext()
        val bundleBuilder = new BundleBuilder()

        bundleBuilder.save(stringIndexer, file, context)

        val transformer = bundleBuilder.load(file, context).root
        val frame = buildFrame()

        val frame2 = transformer.transform(frame).get

        assert(frame2.select("string_index").get.dataset.head.getDouble(0) == 0)
      }
    }
  }
}
