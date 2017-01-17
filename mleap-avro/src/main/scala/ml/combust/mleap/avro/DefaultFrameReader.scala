package ml.combust.mleap.avro

import java.nio.charset.Charset

import ml.combust.mleap.runtime._
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameReader}
import ml.combust.mleap.runtime.types.StructType
import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import SchemaConverter._

import scala.collection.mutable
import scala.util.Try

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultFrameReader extends FrameReader {
  val valueConverter = ValueConverter()

  override def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset)
                        (implicit context: MleapContext): Try[DefaultLeapFrame] = Try {
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
}
