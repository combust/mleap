package ml.combust.mleap.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.Charset

import ml.combust.mleap.runtime._
import ml.combust.mleap.runtime.serialization.{FrameSerializer, FrameSerializerContext, RowSerializer}
import ml.combust.mleap.runtime.types._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io._
import SchemaConverter._
import resource._

/**
  * Created by hollinwilkins on 10/31/16.
  */
object DefaultFrameSerializer {
  val charset = Charset.forName("UTF-8")
}

case class DefaultFrameSerializer(override val serializerContext: FrameSerializerContext) extends FrameSerializer {
  implicit val context: MleapContext = serializerContext.context
  val valueConverter = ValueConverter()

  override def toBytes[LF <: LeapFrame[LF]](frame: LF): Array[Byte] = {
    val writers = frame.schema.fields.map(_.dataType).map(valueConverter.mleapToAvro)
    val avroSchema = frame.schema: Schema
    val record = new GenericData.Record(avroSchema)

    (for(out <- managed(new ByteArrayOutputStream())) yield {
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      encoder.writeString(avroSchema.toString)
      encoder.writeInt(frame.dataset.toArray.length)
      val dataWriter = new GenericDatumWriter[GenericRecord](avroSchema)

      for(row <- frame.dataset.toArray) {
        var i = 0
        for(writer <- writers) {
          record.put(i, writer(row(i)))
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

  override def fromBytes(bytes: Array[Byte]): DefaultLeapFrame = {
    (for(in <- managed(new ByteArrayInputStream(bytes))) yield {
      val decoder = DecoderFactory.get().binaryDecoder(in, null)
      val schemaString = decoder.readString()
      val avroSchema = new Schema.Parser().parse(schemaString)
      val dataReader = new GenericDatumReader[GenericRecord](avroSchema)
      val record = new GenericData.Record(avroSchema)
      val size = decoder.readInt()
      val schema = avroSchema: StructType
      val readers = schema.fields.map(_.dataType).map(valueConverter.avroToMleap)

      val rows = (0 until size).map {
        _ =>
          val row = SeqRow(new Array[Any](schema.fields.length))
          dataReader.read(record, decoder)
          for(i <- schema.fields.indices) {
            row.set(i, readers(i)(record.get(i)))
          }
          row
      }
      DefaultLeapFrame(schema, LocalDataset(rows))
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(frame) => frame
    }
  }

  override def rowSerializer(schema: StructType): RowSerializer = ???
}
