package ml.combust.mleap.avro

import java.io.ByteArrayOutputStream

import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.serialization.RowWriter
import ml.combust.mleap.runtime.types.StructType
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, EncoderFactory}
import SchemaConverter._
import resource._

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultRowWriter(override val schema: StructType) extends RowWriter {
  val valueConverter = ValueConverter()
  lazy val writers = schema.fields.map(_.dataType).map(valueConverter.mleapToAvro)
  val avroSchema = schema: Schema
  val datumWriter = new GenericDatumWriter[GenericData.Record](avroSchema)
  var encoder: BinaryEncoder = null
  var record = new GenericData.Record(avroSchema)

  override def toBytes(row: Row): Array[Byte] = {
    (for(out <- managed(new ByteArrayOutputStream(1024))) yield {
      encoder = EncoderFactory.get().binaryEncoder(out, encoder)

      var i = 0
      for(writer <- writers) {
        record.put(i, writer(row(i)))
        i = i + 1
      }
      datumWriter.write(record, encoder)
      encoder.flush()

      out.toByteArray
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(bytes) => bytes
    }
  }
}
