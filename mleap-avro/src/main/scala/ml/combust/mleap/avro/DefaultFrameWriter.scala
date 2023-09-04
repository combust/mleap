package ml.combust.mleap.avro

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import SchemaConverter._
import ml.combust.mleap.runtime.frame.LeapFrame
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameWriter}
import scala.util.Using

import scala.util.{Failure, Try}

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultFrameWriter[LF <: LeapFrame[LF]](frame: LF) extends FrameWriter {
  val valueConverter = ValueConverter()

  override def toBytes(charset: Charset = BuiltinFormats.charset): Try[Array[Byte]] = {
    Using(new ByteArrayOutputStream()) { out =>
      val writers = frame.schema.fields.map(_.dataType).map(valueConverter.mleapToAvro)
      val avroSchema = frame.schema: Schema
      val record = new GenericData.Record(avroSchema)
      val datumWriter = new GenericDatumWriter[GenericData.Record](avroSchema)
      val writer = new DataFileWriter[GenericData.Record](datumWriter)
      writer.create(avroSchema, out)

      for(row <- frame.collect()) {
        var i = 0
        for(writer <- writers) {
          record.put(i, writer(row.getRaw(i)))
          i = i + 1
        }

        Try(writer.append(record)) match {
          case Failure(error) => error.printStackTrace()
          case _ =>
        }
      }

      writer.close()

      out.toByteArray
    }
  }
}
