package ml.combust.mleap.runtime.javadsl

import java.io.File
import java.nio.file.Files
import java.util

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.transformer.feature.StringIndexer
import ml.combust.mleap.runtime.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.{ByteString, Tensor}
import org.scalatest.FunSpec

import scala.collection.JavaConverters._

/**
  * Created by hollinwilkins on 4/21/17.
  */
class JavaDSLSpec extends FunSpec {
  def buildFrame(): DefaultLeapFrame = {
    val builder = new LeapFrameBuilder
    val fields = new util.ArrayList[StructField]()

    fields.add(builder.createField("bool", builder.createBool()))
    fields.add(builder.createField("string", builder.createString()))
    fields.add(builder.createField("byte", builder.createByte()))
    fields.add(builder.createField("short", builder.createShort()))
    fields.add(builder.createField("int", builder.createInt()))
    fields.add(builder.createField("long", builder.createLong()))
    fields.add(builder.createField("float", builder.createFloat()))
    fields.add(builder.createField("double", builder.createDouble()))
    fields.add(builder.createField("byte_string", builder.createByteString()))
    fields.add(builder.createField("list", builder.createList(builder.createLong())))
    fields.add(builder.createField("tensor", builder.createTensor(builder.createByte())))

    val rows = new util.ArrayList[Row]()
    val list = Seq[Long](23, 44, 55).asJava
    val tensor = Tensor.denseVector[Byte](Array[Byte](23, 3, 4))
    rows.add(builder.createRow(true, "hello", Byte.box(1),
      Short.box(2), Int.box(3), Long.box(4),
      Float.box(34.5f), Double.box(44.5), ByteString("hello_there".getBytes()),
      list, tensor))

    val schema = builder.createSchema(fields)
    val dataset = builder.createDataset(rows)
    builder.createFrame(schema, dataset)
  }

  describe("building a LeapFrame") {
    it("is able to build a LeapFrame with all data types") {
      val frame = buildFrame()
      val schema = frame.schema

      assert(schema.getField("bool").get == StructField("bool", BooleanType()))
      assert(schema.getField("string").get == StructField("string", StringType()))
      assert(schema.getField("byte").get == StructField("byte", ByteType()))
      assert(schema.getField("short").get == StructField("short", ShortType()))
      assert(schema.getField("int").get == StructField("int", IntegerType()))
      assert(schema.getField("long").get == StructField("long", LongType()))
      assert(schema.getField("float").get == StructField("float", FloatType()))
      assert(schema.getField("double").get == StructField("double", DoubleType()))
      assert(schema.getField("byte_string").get == StructField("byte_string", ByteStringType()))
      assert(schema.getField("list").get == StructField("list", ListType(LongType())))
      assert(schema.getField("tensor").get == StructField("tensor", TensorType(ByteType())))

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

  describe("MLeap bundles") {
    val stringIndexer = StringIndexer(inputCol = "string",
      inputDataType = Some(StringType()),
      outputCol = "string_index",
      model = StringIndexerModel(Seq("hello")))
    val dir = Files.createTempDirectory("mleap")
    val file = new File(dir.toFile, "model.zip")

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
