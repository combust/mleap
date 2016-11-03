package ml.combust.mleap.avro

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

import ml.combust.mleap.runtime.LeapFrame
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameWriter}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import SchemaConverter._
import resource._

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultFrameWriter extends FrameWriter {
  val valueConverter = ValueConverter()

  override def toBytes[LF <: LeapFrame[LF]](frame: LF, charset: Charset = BuiltinFormats.charset): Array[Byte] = {
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
}
