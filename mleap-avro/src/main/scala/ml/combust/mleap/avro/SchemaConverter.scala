package ml.combust.mleap.avro

import java.nio.charset.Charset

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.Tensor
import org.apache.avro.Schema

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}
import scala.util.Try

/**
  * Created by hollinwilkins on 10/31/16.
  */
object SchemaConverter {
  def tensorSchema[T: ClassTag] = {
    val r = Try {
      val (name, valuesSchema) = classTag[T].runtimeClass match {
        case Tensor.BooleanClass =>
          ("Boolean", Schema.createArray(Schema.create(Schema.Type.BOOLEAN)))
        case Tensor.StringClass =>
          ("String", Schema.createArray(Schema.create(Schema.Type.STRING)))
        case Tensor.ByteClass =>
          ("Byte", Schema.create(Schema.Type.BYTES))
        case Tensor.ShortClass =>
          ("Short", Schema.createArray(Schema.create(Schema.Type.INT)))
        case Tensor.IntClass =>
          ("Int", Schema.createArray(Schema.create(Schema.Type.INT)))
        case Tensor.LongClass =>
          ("Long", Schema.createArray(Schema.create(Schema.Type.LONG)))
        case Tensor.FloatClass =>
          ("Float", Schema.createArray(Schema.create(Schema.Type.FLOAT)))
        case Tensor.DoubleClass =>
          ("Double", Schema.createArray(Schema.create(Schema.Type.DOUBLE)))
        case _ => throw new IllegalArgumentException(s"invalid base ${classTag[T].runtimeClass.getName}")
      }
      val indicesSchema = Schema.createUnion(Schema.createArray(Schema.createArray(Schema.create(Schema.Type.INT))),
        Schema.create(Schema.Type.NULL))
      Schema.createRecord(s"${name}Tensor", "", "ml.combust.mleap.avro", false,
        Seq(new Schema.Field("dimensions", Schema.createArray(Schema.create(Schema.Type.INT)), "", null: AnyRef),
          new Schema.Field("values", valuesSchema, "", null: AnyRef),
          new Schema.Field("indices", indicesSchema, "", null: AnyRef)).asJava)
    }

    r.get
  }

  val booleanTensorSchema = tensorSchema[Boolean]
  val stringTensorSchema = tensorSchema[String]
  val byteTensorSchema = tensorSchema[Byte]
  val shortTensorSchema = tensorSchema[Short]
  val integerTensorSchema = tensorSchema[Int]
  val longTensorSchema = tensorSchema[Long]
  val floatTensorSchema = tensorSchema[Float]
  val doubleTensorSchema = tensorSchema[Double]

  val bytesCharset = Charset.forName("UTF-8")

  val tensorSchemaDimensionsIndex = 0
  val tensorSchemaValuesIndex = 1
  val tensorSchemaIndicesIndex = 2

  val customSchemaIndex = 0

  implicit def mleapToAvro(schema: StructType): Schema = {
    val fields = schema.fields.map(mleapToAvroField).asJava
    Schema.createRecord("LeapFrame", "", "ml.combust.mleap.avro", false, fields)
  }

  implicit def mleapToAvroField(field: StructField): Schema.Field = new Schema.Field(field.name, mleapToAvroType(field.dataType), "", null: AnyRef)

  def maybeNullableAvroType(base: Schema, isNullable: Boolean): Schema = {
    if(isNullable) {
      Schema.createUnion(base, Schema.create(Schema.Type.NULL))
    } else { base }
  }

  implicit def mleapBasicToAvroType(basicType: BasicType): Schema = basicType match {
    case BooleanType(isNullable) => Schema.create(Schema.Type.BOOLEAN)
    case StringType(isNullable) => Schema.create(Schema.Type.STRING)
    case ByteType(isNullable) => Schema.create(Schema.Type.INT)
    case ShortType(isNullable) => Schema.create(Schema.Type.INT)
    case IntegerType(isNullable) => Schema.create(Schema.Type.INT)
    case LongType(isNullable) => Schema.create(Schema.Type.LONG)
    case FloatType(isNullable) => Schema.create(Schema.Type.FLOAT)
    case DoubleType(isNullable) => Schema.create(Schema.Type.DOUBLE)
    case ByteStringType(isNullable) => Schema.create(Schema.Type.BYTES)
  }

  implicit def mleapToAvroType(dataType: DataType): Schema = dataType match {
    case basicType: BasicType => maybeNullableAvroType(mleapBasicToAvroType(basicType), basicType.isNullable)
    case lt: ListType => maybeNullableAvroType(Schema.createArray(mleapToAvroType(lt.base)), lt.isNullable)
    case tt: TensorType =>
      val ts = tt.base match {
        case BooleanType(_) => booleanTensorSchema
        case StringType(_) => stringTensorSchema
        case ByteType(_) => byteTensorSchema
        case ShortType(_) => shortTensorSchema
        case IntegerType(_) => integerTensorSchema
        case LongType(_) => longTensorSchema
        case FloatType(_) => floatTensorSchema
        case DoubleType(_) => doubleTensorSchema
        case _ => throw new IllegalArgumentException(s"invalid type ${tt.base}")
      }
      maybeNullableAvroType(ts, tt.isNullable)
    case AnyType(false) => throw new IllegalArgumentException(s"invalid data type: $dataType")
    case _ => throw new IllegalArgumentException(s"invalid data type: $dataType")
  }

  implicit def avroToMleap(schema: Schema)
                          (implicit context: MleapContext): StructType = schema.getType match {
    case Schema.Type.RECORD =>
      val fields = schema.getFields.asScala.map(avroToMleapField)
      StructType(fields).get
    case _ => throw new IllegalArgumentException("invalid avro record type")
  }

  implicit def avroToMleapField(field: Schema.Field)
                               (implicit context: MleapContext): StructField = StructField(field.name(), avroToMleapType(field.schema()))

  def maybeNullableMleapType(schema: Schema): DataType = {
    val types = schema.getTypes.asScala
    assert(types.size == 2, "only nullable unions supported (2 type unions)")

    types.find(_.getType == Schema.Type.NULL).flatMap {
      _ => types.find(_.getType != Schema.Type.NULL)
    }.map(avroToMleapType).getOrElse {
      throw new IllegalArgumentException(s"unsupported schema: $schema")
    }.asNullable
  }

  implicit def avroToMleapType(schema: Schema)
                              (implicit context: MleapContext): DataType = schema.getType match {
    case Schema.Type.BOOLEAN => BooleanType(false)
    case Schema.Type.STRING => StringType(false)
    case Schema.Type.INT => IntegerType(false)
    case Schema.Type.LONG => LongType(false)
    case Schema.Type.FLOAT => FloatType(false)
    case Schema.Type.DOUBLE => DoubleType(false)
    case Schema.Type.BYTES => ByteStringType(false)
    case Schema.Type.ARRAY => ListType(avroToMleapType(schema.getElementType))
    case Schema.Type.UNION => maybeNullableMleapType(schema)
    case Schema.Type.RECORD =>
      schema.getName match {
        case "BooleanTensor" => TensorType(BooleanType(false))
        case "StringTensor" => TensorType(StringType(false))
        case "ByteTensor" => TensorType(ByteType(false))
        case "ShortTensor" => TensorType(ShortType(false))
        case "IntTensor" => TensorType(IntegerType(false))
        case "LongTensor" => TensorType(LongType(false))
        case "FloatTensor" => TensorType(FloatType(false))
        case "DoubleTensor" => TensorType(DoubleType(false))
        case _ => throw new IllegalArgumentException("invalid avro record")
      }
    case _ => throw new IllegalArgumentException("invalid avro record")
  }
}
