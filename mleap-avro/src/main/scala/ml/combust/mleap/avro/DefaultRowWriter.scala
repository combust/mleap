package ml.combust.mleap.avro

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

import ml.combust.mleap.core.frame.Row
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import SchemaConverter._
import ml.combust.mleap.core.serialization.{BuiltinFormats, RowWriter}
import ml.combust.mleap.core.types.StructType
import resource._

import scala.util.Try

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

  override def toBytes(row: Row, charset: Charset = BuiltinFormats.charset): Try[Array[Byte]] = {
    (for(out <- managed(new ByteArrayOutputStream(1024))) yield {
      encoder = EncoderFactory.get().binaryEncoder(out, encoder)

      var i = 0
      for(writer <- writers) {
        record.put(i, writer(row.getRaw(i)))
        i = i + 1
      }
      datumWriter.write(record, encoder)
      encoder.flush()

      out.toByteArray
    }).tried
  }
}
