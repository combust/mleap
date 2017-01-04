package ml.combust.mleap.avro

import ml.combust.mleap.runtime.types._
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}

import scala.collection.JavaConverters._

/**
  * Created by hollinwilkins on 10/31/16.
  */
case class ValueConverter() {
  import SchemaConverter._

  val denseRecord = new GenericData.Record(denseSchema)
  val sparseRecord = new GenericData.Record(sparseSchema)

  def mleapToAvro(dataType: DataType): (Any) => Any = {
    val base = mleapToAvroBase(dataType)

    if(dataType.isNullable) {
      (v: Any) => v.asInstanceOf[Option[Any]].map(base).orNull
    } else { base }
  }

  def mleapToAvroBase(dataType: DataType): (Any) => Any = dataType match {
    case _: BasicType => identity
    case _: ListType => (value) => value.asInstanceOf[Seq[_]].asJava
    case dataType: TensorType =>
      val vectorRecord = new GenericData.Record(dataType)
      (value) => {
        value match {
          case DenseVector(values) =>
            denseRecord.put(denseSchemaValuesIndex, values.toSeq.asJava)
            vectorRecord.put(tensorSchemaIndex, denseRecord)
          case SparseVector(size, indices, values) =>
            sparseRecord.put(sparseSchemaSizeIndex, size)
            sparseRecord.put(sparseSchemaIndicesIndex, indices.toSeq.asJava)
            sparseRecord.put(sparseSchemaValuesIndex, values.toSeq.asJava)
            vectorRecord.put(tensorSchemaIndex, denseRecord)
        }

        vectorRecord
      }
    case dataType: CustomType =>
      val customRecord = new GenericData.Record(customSchema(dataType))
      (value) =>
        customRecord.put(customSchemaIndex, new String(dataType.toBytes(value), bytesCharset))
        customRecord
    case AnyType(_) => throw new IllegalArgumentException(s"invalid data type: $dataType")
  }

  def avroToMleap(dataType: DataType): (Any) => Any = {
    val base = avroToMleapBase(dataType)

    if(dataType.isNullable) {
      (v) =>
        Option[Any](v).map(base)
    } else { base }
  }

  def avroToMleapBase(dataType: DataType): (Any) => Any = dataType match {
    case StringType(_) => (value) => value.asInstanceOf[Utf8].toString
    case _: BasicType => identity
    case at: ListType => at.base match {
      case DoubleType(_) => (value) => value.asInstanceOf[GenericData.Array[Double]].asScala
      case StringType(_) => (value) => value.asInstanceOf[GenericData.Array[Utf8]].asScala.map(_.toString)
      case LongType(_) => (value) => value.asInstanceOf[GenericData.Array[Long]].asScala
      case IntegerType(_) => (value) => value.asInstanceOf[GenericData.Array[Integer]].asScala
      case BooleanType(_) => (value) => value.asInstanceOf[GenericData.Array[Boolean]].asScala
      case _ =>
        val atm = avroToMleap(at.base)
        (value) => value.asInstanceOf[GenericData.Array[_]].asScala.map(atm)
    }
    case tt: TensorType if tt.base == DoubleType(false) && tt.dimensions.length == 1 =>
      (value) => {
        val record = value.asInstanceOf[GenericData.Record].
          get(tensorSchemaIndex).
          asInstanceOf[GenericData.Record]
        record.getSchema.getName match {
          case "DenseTensor" => Vectors.dense(record.get(denseSchemaValuesIndex).asInstanceOf[GenericData.Array[Double]].asScala.toArray)
          case "SparseTensor" =>
            val size = record.get(sparseSchemaSizeIndex).asInstanceOf[Int]
            val indices = record.get(sparseSchemaIndicesIndex).asInstanceOf[GenericData.Array[Int]].asScala.toArray
            val values = record.get(sparseSchemaIndicesIndex).asInstanceOf[GenericData.Array[Double]].asScala.toArray
            Vectors.sparse(size, indices, values)
        }
      }
    case ct: CustomType =>
      (value) => {
        ct.fromBytes(value.asInstanceOf[GenericData.Record].get(customSchemaIndex).toString.getBytes(bytesCharset))
      }
  }
}
