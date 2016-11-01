package ml.combust.mleap.avro

import java.io.ByteArrayOutputStream

import ml.combust.mleap.runtime.{Row, ArrayRow}
import ml.combust.mleap.runtime.serialization.RowSerializer
import ml.combust.mleap.runtime.types.StructType
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import SchemaConverter._
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}
import resource._

/**
  * Created by hollinwilkins on 11/1/16.
  */
case class AvroRowSerializer(schema: StructType) extends RowSerializer {
  val valueConverter = ValueConverter()
  lazy val writers = schema.fields.map(_.dataType).map(valueConverter.mleapToAvro)
  lazy val readers = schema.fields.map(_.dataType).map(valueConverter.avroToMleap)
  val avroSchema = schema: Schema
  val datumWriter = new GenericDatumWriter[GenericData.Record](avroSchema)
  val datumReader = new GenericDatumReader[GenericData.Record](avroSchema)
  var encoder: BinaryEncoder = null
  var decoder: BinaryDecoder = null
  var record = new GenericData.Record(avroSchema)

  override def toBytes(row: Row): Array[Byte] = synchronized {
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

  override def fromBytes(bytes: Array[Byte]): Row = synchronized {
    decoder = DecoderFactory.get().binaryDecoder(bytes, decoder)
    record = datumReader.read(record, decoder)
    val row = ArrayRow(new Array[Any](schema.fields.length))
    for(i <- schema.fields.indices) { row.set(i, readers(i)(record.get(i))) }
    row
  }
}
