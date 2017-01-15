package ml.combust.mleap.avro

import ml.combust.mleap.core.tensor.SparseTensor
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.{DenseTensor, SparseTensor, Tensor}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

import scala.collection.JavaConverters._

/**
  * Created by hollinwilkins on 10/31/16.
  */
case class ValueConverter() {
  import SchemaConverter._

  def mleapToAvro(dataType: DataType): (Any) => Any = {
    val base = mleapToAvroBase(dataType)

    if(dataType.isNullable) {
      (v: Any) => v.asInstanceOf[Option[Any]].map(base).orNull
    } else { base }
  }

  def mleapToAvroBase(dataType: DataType): (Any) => Any = dataType match {
    case _: BasicType => identity
    case _: ListType => (value) => value.asInstanceOf[Seq[_]].asJava
    case tt: TensorType =>
      val vectorRecord = new GenericData.Record(tt)
      (value) => {
        val tensor = value.asInstanceOf[Tensor[_]]
        val values = tensor.rawValuesIterator.toSeq.asJava
        vectorRecord.put(tensorSchemaDimensionsIndex, tensor.dimensions.asJava)
        vectorRecord.put(tensorSchemaValuesIndex, values)
        tensor match {
          case tensor: DenseTensor[_] =>
            vectorRecord.put(tensorSchemaIndicesIndex, null)
          case tensor: SparseTensor[_] =>
            vectorRecord.put(tensorSchemaIndicesIndex, tensor.indices.map(_.asJava).asJava)
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
      case BooleanType(_) => (value) => value.asInstanceOf[GenericData.Array[Boolean]].asScala
      case StringType(_) => (value) => value.asInstanceOf[GenericData.Array[Utf8]].asScala.map(_.toString)
      case IntegerType(_) => (value) => value.asInstanceOf[GenericData.Array[Integer]].asScala
      case LongType(_) => (value) => value.asInstanceOf[GenericData.Array[Long]].asScala
      case FloatType(_) => (value) => value.asInstanceOf[GenericData.Array[Float]].asScala
      case DoubleType(_) => (value) => value.asInstanceOf[GenericData.Array[Double]].asScala
      case _ =>
        val atm = avroToMleap(at.base)
        (value) => value.asInstanceOf[GenericData.Array[_]].asScala.map(atm)
    }
    case tt: TensorType =>
      (value) => {
        val record = value.asInstanceOf[GenericData.Record]
        val dimensions = record.get(tensorSchemaDimensionsIndex).asInstanceOf[java.util.List[Int]].asScala
        val values = record.get(tensorSchemaValuesIndex)
        val indices = record.get(tensorSchemaIndicesIndex) match {
          case null => None
          case is => Some(is.asInstanceOf[java.util.List[java.util.List[Int]]].asScala.map(_.asScala))
        }

        record.getSchema.getField("values").schema().getElementType.getType match {
          case Schema.Type.BOOLEAN =>
            Tensor.create(values.asInstanceOf[java.util.List[Boolean]].asScala.toArray, dimensions, indices)
          case Schema.Type.STRING =>
            Tensor.create(values.asInstanceOf[java.util.List[String]].asScala.toArray, dimensions, indices)
          case Schema.Type.INT =>
            Tensor.create(values.asInstanceOf[java.util.List[Int]].asScala.toArray, dimensions, indices)
          case Schema.Type.LONG =>
            Tensor.create(values.asInstanceOf[java.util.List[Long]].asScala.toArray, dimensions, indices)
          case Schema.Type.FLOAT =>
            Tensor.create(values.asInstanceOf[java.util.List[Float]].asScala.toArray, dimensions, indices)
          case Schema.Type.DOUBLE =>
            Tensor.create(values.asInstanceOf[java.util.List[Double]].asScala.toArray, dimensions, indices)
          case tpe => throw new IllegalArgumentException(s"invalid base type for tensor $tpe")
        }
      }
    case ct: CustomType =>
      (value) => {
        ct.fromBytes(value.asInstanceOf[GenericData.Record].get(customSchemaIndex).toString.getBytes(bytesCharset))
      }
    case tpe => throw new IllegalArgumentException(s"invalid data type $tpe")
  }
}
