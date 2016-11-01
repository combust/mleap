package ml.combust.mleap.avro

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

import ml.combust.mleap.runtime._
import ml.combust.mleap.runtime.serialization.{FrameSerializer, FrameSerializerContext, RowSerializer}
import ml.combust.mleap.runtime.types._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import SchemaConverter._
import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput}
import resource._

import scala.collection.mutable

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
    (for(out <- managed(new ByteArrayOutputStream())) yield {
      val writers = frame.schema.fields.map(_.dataType).map(valueConverter.mleapToAvro)
      val avroSchema = frame.schema: Schema
      val record = new GenericData.Record(avroSchema)
      val datumWriter = new GenericDatumWriter[GenericData.Record](avroSchema)
      val writer = new DataFileWriter[GenericData.Record](datumWriter)
      writer.create(avroSchema, out)

      for(row <- frame.dataset.toArray) {
        var i = 0
        for(writer <- writers) {
          record.put(i, writer(row(i)))
          i = i + 1
        }

        writer.append(record)
      }

      writer.close()

      out.toByteArray
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(bytes) => bytes
    }
  }

  override def fromBytes(bytes: Array[Byte]): DefaultLeapFrame = {
    val datumReader = new GenericDatumReader[GenericData.Record]()
    val reader = new DataFileReader[GenericData.Record](new SeekableByteArrayInput(bytes), datumReader)
    val avroSchema = reader.getSchema
    val schema = avroSchema: StructType
    val readers = schema.fields.map(_.dataType).map(valueConverter.avroToMleap)

    var record = new GenericData.Record(avroSchema)
    var rows = mutable.ArrayBuilder.make[Row]()
    while(reader.hasNext) {
      record = reader.next(record)
      val row = ArrayRow(new Array[Any](schema.fields.length))
      for(i <- schema.fields.indices) { row.set(i, readers(i)(record.get(i))) }
      rows += row
    }

    DefaultLeapFrame(schema, LocalDataset(rows.result))
  }

  override def rowSerializer(schema: StructType): AvroRowSerializer = AvroRowSerializer(schema)
}
