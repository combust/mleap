package ml.combust.mleap.avro

import java.nio.charset.Charset

import ml.combust.mleap.runtime.{ArrayRow, Row}
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, RowReader}
import ml.combust.mleap.runtime.types.StructType
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import SchemaConverter._

import scala.util.Try

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultRowReader(override val schema: StructType) extends RowReader {
  val valueConverter = ValueConverter()
  lazy val readers = schema.fields.map(_.dataType).map(valueConverter.avroToMleap)
  val avroSchema = schema: Schema
  val datumReader = new GenericDatumReader[GenericData.Record](avroSchema)
  var decoder: BinaryDecoder = null
  var record = new GenericData.Record(avroSchema)

  override def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset): Try[Row] = Try {
    decoder = DecoderFactory.get().binaryDecoder(bytes, decoder)
    record = datumReader.read(record, decoder)
    val row = ArrayRow(new Array[Any](schema.fields.length))
    for(i <- schema.fields.indices) { row.set(i, readers(i)(record.get(i))) }
    row
  }
}
