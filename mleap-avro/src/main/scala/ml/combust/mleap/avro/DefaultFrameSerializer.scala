package ml.combust.mleap.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.Charset

import ml.combust.mleap.runtime.{ArrayRow, DefaultLeapFrame, LeapFrame, LocalDataset}
import ml.combust.mleap.runtime.serialization.{FrameSerializer, FrameSerializerContext, RowSerializer}
import ml.combust.mleap.runtime.types._
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}
import ml.combust.mleap.runtime.serialization.json.JsonSupport._
import spray.json._
import resource._

import scala.collection.JavaConverters._

/**
  * Created by hollinwilkins on 10/31/16.
  */
object DefaultFrameSerializer {
  val charset = Charset.forName("UTF-8")
}

case class DefaultFrameSerializer(override val serializerContext: FrameSerializerContext) extends FrameSerializer {
  private val sparseSchema = Schema.createRecord("SparseVector", "", "blah", false, Seq(new Schema.Field("size", Schema.create(Schema.Type.INT), "", null: AnyRef),
    new Schema.Field("indices", Schema.createArray(Schema.create(Schema.Type.INT)), "", null: AnyRef),
    new Schema.Field("values", Schema.createArray(Schema.create(Schema.Type.DOUBLE)), "", null: AnyRef)).asJava)
  private val denseSchema = Schema.createRecord("DenseVector", "", "blah", false, Seq(new Schema.Field("values", Schema.createArray(Schema.create(Schema.Type.DOUBLE)), "", null: AnyRef)).asJava)
  private val denseRecord = new GenericData.Record(denseSchema)
  private val sparseRecord = new GenericData.Record(sparseSchema)

  override def toBytes[LF <: LeapFrame[LF]](frame: LF): Array[Byte] = {
    val fields = frame.schema.fields.map(toAvroField).asJava
    val writers = frame.schema.fields.map(recordWriter)
    val avroSchema = Schema.createRecord("something", "", "", false, fields)
    val record = new GenericData.Record(avroSchema)

    (for(out <- managed(new ByteArrayOutputStream())) yield {
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      encoder.writeString(avroSchema.toString)
      encoder.writeInt(frame.dataset.toArray.length)
      val dataWriter = new GenericDatumWriter[GenericRecord](avroSchema)

      for(row <- frame.dataset.toArray) {
        var i = 0
        for(writer <- writers) {
          writer(record, row(i))
          i = i + 1
        }

        dataWriter.write(record, encoder)
      }

      encoder.flush()

      out.toByteArray
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(bytes) => bytes
    }
  }

  def recordWriter(field: StructField): (GenericRecord, Any) => Unit = {
    val name = field.name

    field.dataType match {
      case _: BasicType => (record, value) => record.put(name, value)
      case tt: TensorType if tt.base == DoubleType && tt.dimensions.length == 1 =>
        (record, value) => {
          value match {
            case value: DenseVector =>
              denseRecord.put("values", java.util.Arrays.asList(value.toArray: _*))
              record.put(name, denseRecord)
            case value: SparseVector =>
              sparseRecord.put("size", value.size)
              sparseRecord.put("indices", java.util.Arrays.asList(value.indices: _*))
              sparseRecord.put("values", java.util.Arrays.asList(value.values: _*))
              record.put(name, sparseRecord)
          }
        }
    }
  }

  override def fromBytes(bytes: Array[Byte]): DefaultLeapFrame = {
    (for(in <- managed(new ByteArrayInputStream(bytes))) yield {
      val decoder = DecoderFactory.get().binaryDecoder(in, null)
      val schemaString = decoder.readString()
      val avroSchema = new Schema.Parser().parse(schemaString)
      val dataReader = new GenericDatumReader[GenericRecord](avroSchema)
      val record = new GenericData.Record(avroSchema)
      val size = decoder.readInt()
      val schema = toMleapStructType(avroSchema)
      val readers = schema.fields.map(rowReader)

      val rows = (0 until size).map {
        _ =>
          val row = ArrayRow(new Array[Any](schema.fields.length))
          dataReader.read(record, decoder)
          for(i <- schema.fields.indices) {
            row.set(i, readers(i)(record))
          }
          row
      }
      DefaultLeapFrame(schema, LocalDataset(rows.toArray))
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(frame) => frame
    }
  }

  def rowReader(field: StructField): (GenericRecord) => Any = {
    val name = field.name
    field.dataType match {
      case _: BasicType => (record) => record.get(name)
      case tt: TensorType if tt.base == DoubleType && tt.dimensions.length == 1 =>
        (record) =>
          val v = record.get(name).asInstanceOf[GenericData.Record]
          v.getSchema.getName match {
            case "DenseVector" =>
              Vectors.dense(v.get("values").asInstanceOf[GenericData.Array[Double]].asScala.toArray)
            case "SparseVector" =>
              val size = v.get("size").asInstanceOf[Int]
              val indices = v.get("indices").asInstanceOf[GenericData.Array[Int]].asScala.toArray
              val values = v.get("values").asInstanceOf[GenericData.Array[Double]].asScala.toArray
              Vectors.sparse(size, indices, values)
          }
    }
  }

  override def rowSerializer(schema: StructType): RowSerializer = ???

  private def toAvroField(field: StructField): Field = field match {
    case StructField(name, dataType) =>
      new Field(name, toAvroSchema(dataType), name, null: AnyRef)
  }

  private def vectorAvroSchema(tt: TensorType): Schema = {
    Schema.createUnion(denseSchema, sparseSchema)
  }

  private def toMleapStructType(schema: Schema): StructType = {
    if(schema.getType == Schema.Type.RECORD) {
      val fields = schema.getFields.asScala.map {
        field => StructField(field.name(), toMleapDataType(field.schema()))
      }
      StructType(fields).get
    } else { throw new IllegalArgumentException("invalid avro schema") }
  }

  private def toMleapDataType(schema: Schema): DataType = schema.getType match {
    case Schema.Type.DOUBLE => DoubleType
    case Schema.Type.STRING => StringType
    case Schema.Type.INT => IntegerType
    case Schema.Type.LONG => LongType
    case Schema.Type.BOOLEAN => BooleanType
    case Schema.Type.ARRAY => ListType(toMleapDataType(schema.getElementType))
    case Schema.Type.UNION => TensorType.doubleVector()
    case _ => throw new IllegalArgumentException("invalid schema")
  }

  private def toAvroSchema(dataType: DataType): Schema = dataType match {
    case DoubleType => Schema.create(Schema.Type.DOUBLE)
    case StringType => Schema.create(Schema.Type.STRING)
    case LongType => Schema.create(Schema.Type.LONG)
    case IntegerType => Schema.create(Schema.Type.INT)
    case BooleanType => Schema.create(Schema.Type.BOOLEAN)
    case lt: ListType => Schema.createArray(toAvroSchema(lt.base))
    case tt: TensorType => vectorAvroSchema(tt)
    case ct: CustomType => Schema.createRecord(Seq(new Schema.Field("custom", Schema.create(Schema.Type.BYTES), ct.name, null: AnyRef)).asJava)
  }
}
