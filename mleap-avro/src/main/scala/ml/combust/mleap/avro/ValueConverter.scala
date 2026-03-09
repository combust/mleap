package ml.combust.mleap.avro

import java.nio.ByteBuffer
import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.{ByteString, DenseTensor, SparseTensor, Tensor}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

import scala.jdk.CollectionConverters._

/**
  * Created by hollinwilkins on 10/31/16.
  */
case class ValueConverter() {
  import SchemaConverter._

  def mleapToAvro(dataType: DataType): (Any) => Any = {
    val simple = mleapToAvroSimple(dataType)

    if(dataType.isNullable) {
      (v: Any) => Option(v).map(simple).orNull
    } else { simple }
  }

  def mleapToAvroBasic(base: BasicType): (Any) => Any = base match {
    case BasicType.ByteString => (v) => ByteBuffer.wrap(v.asInstanceOf[ByteString].bytes)
    case _ => identity
  }

  def mleapToAvroSimple(dataType: DataType): (Any) => Any = dataType match {
    case st: ScalarType => mleapToAvroBasic(st.base)
    case _: ListType => (value) => value.asInstanceOf[Seq[_]].asJava
    case _: MapType => (value) => value.asInstanceOf[Map[String, _]].asJava
    case tt: TensorType =>
      val vectorRecord = new GenericData.Record(tt)
      (value) => {
        val tensor = value.asInstanceOf[Tensor[_]]
        val values = if(tensor.base.runtimeClass == Tensor.ByteClass) {
          ByteBuffer.wrap(tensor.rawValues.asInstanceOf[Array[Byte]])
        } else {
          tensor.rawValuesIterator.toSeq.asJava
        }
        vectorRecord.put(tensorSchemaDimensionsIndex, tensor.dimensions.asJava)
        vectorRecord.put(tensorSchemaValuesIndex, values)
        tensor match {
          case _: DenseTensor[_] =>
            vectorRecord.put(tensorSchemaIndicesIndex, null)
          case tensor: SparseTensor[_] =>
            vectorRecord.put(tensorSchemaIndicesIndex, tensor.indices.map(_.asJava).asJava)
        }
        vectorRecord
      }
    case _ => throw new IllegalArgumentException(s"invalid data type: $dataType")
  }

  def avroToMleapBasic(base: BasicType): (Any) => Any = base match {
    case BasicType.String => _.asInstanceOf[Utf8].toString
    case BasicType.ByteString => (value) => ByteString(value.asInstanceOf[ByteBuffer].array())
    case _ => identity
  }

  def avroToMleap(dataType: DataType): (Any) => Any = if(dataType.isNullable) {
    val simple = avroToMleapSimple(dataType)

    (v) => Option[Any](v).map(simple).orNull
  } else { avroToMleapSimple(dataType) }

  def avroToMleapSimple(dataType: DataType): (Any) => Any = dataType match {
    case st: ScalarType => avroToMleapBasic(st.base)
    case at: ListType => at.base match {
      case BasicType.Boolean => (value) => value.asInstanceOf[GenericData.Array[Boolean]].asScala.toSeq
      case BasicType.Byte => (value) => value.asInstanceOf[GenericData.Array[Integer]].asScala.map(_.toByte).toSeq
      case BasicType.Short => (value) => value.asInstanceOf[GenericData.Array[Integer]].asScala.map(_.toShort).toSeq
      case BasicType.Int => (value) => value.asInstanceOf[GenericData.Array[Integer]].asScala.toSeq
      case BasicType.Long => (value) => value.asInstanceOf[GenericData.Array[Long]].asScala.toSeq
      case BasicType.Float => (value) => value.asInstanceOf[GenericData.Array[Float]].asScala.toSeq
      case BasicType.Double => (value) => value.asInstanceOf[GenericData.Array[Double]].asScala.toSeq
      case BasicType.String => (value) => value.asInstanceOf[GenericData.Array[Utf8]].asScala.map(_.toString).toSeq
      case BasicType.ByteString => (value) => value.asInstanceOf[GenericData.Array[ByteBuffer]].asScala.map(b => ByteString(b.array())).toSeq
      case _ =>
        val atm = avroToMleapBasic(at.base)
        (value) => value.asInstanceOf[GenericData.Array[_]].asScala.map(atm).toSeq
    }
    case mt: MapType => (value) => {
      val kConverter = avroToMleapBasic(mt.key)
      val vConverter = avroToMleapBasic(mt.base)
      value.asInstanceOf[java.util.Map[_,_]].asScala.map {
        case (k, v) => kConverter(k) -> vConverter(v)
      }.toMap
    }
    case tt: TensorType =>
      (value) => {
        val record = value.asInstanceOf[GenericData.Record]
        val dimensions = record.get(tensorSchemaDimensionsIndex).asInstanceOf[java.util.List[Int]].asScala.toSeq
        val values = record.get(tensorSchemaValuesIndex)
        val indices = record.get(tensorSchemaIndicesIndex) match {
          case null => None
          case is => Some(is.asInstanceOf[java.util.List[java.util.List[Int]]].asScala.map(_.asScala.toSeq).toSeq)
        }

        tt.base match {
          case BasicType.Boolean =>
            Tensor.create(values.asInstanceOf[java.util.List[Boolean]].asScala.toSeq.toArray, dimensions, indices)
          case BasicType.Byte =>
            Tensor.create(values.asInstanceOf[ByteBuffer].array(), dimensions, indices)
          case BasicType.Short =>
            Tensor.create(values.asInstanceOf[java.util.List[Int]].asScala.map(_.toShort).toSeq.toArray, dimensions, indices)
          case BasicType.Int =>
            Tensor.create(values.asInstanceOf[java.util.List[Int]].asScala.toSeq.toArray, dimensions, indices)
          case BasicType.Long =>
            Tensor.create(values.asInstanceOf[java.util.List[Long]].asScala.toSeq.toArray, dimensions, indices)
          case BasicType.Float =>
            Tensor.create(values.asInstanceOf[java.util.List[Float]].asScala.toSeq.toArray, dimensions, indices)
          case BasicType.Double =>
            Tensor.create(values.asInstanceOf[java.util.List[Double]].asScala.toSeq.toArray, dimensions, indices)
          case BasicType.String =>
            Tensor.create(values.asInstanceOf[java.util.List[String]].asScala.toSeq.toArray, dimensions, indices)
          case tpe => throw new IllegalArgumentException(s"invalid base type for tensor $tpe")
        }
      }
    case tpe => throw new IllegalArgumentException(s"invalid data type $tpe")
  }
}
