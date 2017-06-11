package ml.combust.mleap.runtime.javadsl

import java.io.File
import java.nio.file.Files
import java.util

import ml.combust.bundle.ByteString
import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.runtime.javadsl.json.DefaultFrameReaderSupport
import ml.combust.mleap.runtime.transformer.feature.StringIndexer
import ml.combust.mleap.runtime.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

import scala.collection.JavaConverters._
import scala.io.Source

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

  describe("reading a LeapFrame") {
    val frameReader = new DefaultFrameReaderSupport
    val context = new ContextBuilder().createMleapContext()

    it("read a DefaultLeapFrame from String") {
      val source = Source.fromURL(getClass.getResource("/frame.airbnb.json"))
      val frame = try source.mkString finally source.close()
      val frameBytes = new util.ArrayList[Byte]()
      frame.getBytes.foreach(byte => frameBytes.add(byte))

      val defaultLeapFrame = frameReader.fromBytes(frameBytes, context)

      val schema = defaultLeapFrame.schema
      assert(schema.getField("state").get == StructField("state", StringType()))
      assert(schema.getField("bathrooms").get == StructField("bathrooms", DoubleType()))
      assert(schema.getField("square_feet").get == StructField("square_feet", DoubleType()))
      assert(schema.getField("bedrooms").get == StructField("bedrooms", DoubleType()))
      assert(schema.getField("security_deposit").get == StructField("security_deposit", DoubleType()))
      assert(schema.getField("cleaning_fee").get == StructField("cleaning_fee", DoubleType()))
      assert(schema.getField("extra_people").get == StructField("extra_people", DoubleType()))
      assert(schema.getField("number_of_reviews").get == StructField("number_of_reviews", DoubleType()))
      assert(schema.getField("review_scores_rating").get == StructField("review_scores_rating", DoubleType()))
      assert(schema.getField("room_type").get == StructField("room_type", StringType()))
      assert(schema.getField("host_is_superhost").get == StructField("host_is_superhost", StringType()))
      assert(schema.getField("cancellation_policy").get == StructField("cancellation_policy", StringType()))
      assert(schema.getField("instant_bookable").get == StructField("instant_bookable", StringType()))

      val d = defaultLeapFrame.dataset
      assert(d.head.getString(0) == "NY")
      assert(d.head.getDouble(1) == 2.0)
      assert(d.head.getDouble(2) == 1250.0)
      assert(d.head.getDouble(3) == 3.0)
      assert(d.head.getDouble(4) == 50.0)
      assert(d.head.getDouble(5) == 30.0)
      assert(d.head.getDouble(6) == 2.0)
      assert(d.head.getDouble(7) == 56.0)
      assert(d.head.getDouble(8) == 90.0)
      assert(d.head.getString(9) == "Entire home/apt")
      assert(d.head.getString(10) == "1.0")
      assert(d.head.getString(11) == "strict")
      assert(d.head.getString(12) == "1.0")
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
