package ml.combust.mleap.avro

import java.nio.charset.Charset

import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types._
import org.apache.avro.Schema
import ml.combust.mleap.json.JsonSupport._
import spray.json._

import scala.language.implicitConversions
import scala.collection.JavaConverters._

/**
  * Created by hollinwilkins on 10/31/16.
  */
object SchemaConverter {
  val sparseSchema = Schema.createRecord("SparseTensor", "", "ml.combust.mleap.avro", false, Seq(new Schema.Field("size", Schema.create(Schema.Type.INT), "", null: AnyRef),
    new Schema.Field("indices", Schema.createArray(Schema.create(Schema.Type.INT)), "", null: AnyRef),
    new Schema.Field("values", Schema.createArray(Schema.create(Schema.Type.DOUBLE)), "", null: AnyRef)).asJava)
  val denseSchema = Schema.createRecord("DenseTensor", "", "ml.combust.mleap.avro", false, Seq(new Schema.Field("values", Schema.createArray(Schema.create(Schema.Type.DOUBLE)), "", null: AnyRef)).asJava)
  def tensorSchema(tt: TensorType): Schema = {
    val union = Schema.createUnion(denseSchema, sparseSchema)
    Schema.createRecord("Tensor", mleapTensorTypeFormat.write(tt).compactPrint, "ml.combust.mleap.avro", false, Seq(
      new Schema.Field("tensor", union, "", null: AnyRef)
    ).asJava)
  }

  val bytesCharset = Charset.forName("UTF-8")

  def customSchema(ct: CustomType): Schema = Schema.createRecord("Custom", ct.name, "ml.combust.mleap.avro", false, Seq(new Schema.Field("data", Schema.create(Schema.Type.BYTES), "", null: AnyRef)).asJava)

  val sparseSchemaSizeIndex = 0
  val sparseSchemaIndicesIndex = 1
  val sparseSchemaValuesIndex = 2

  val denseSchemaValuesIndex = 0

  val tensorSchemaIndex = 0
  val customSchemaIndex = 0

  implicit def mleapToAvro(schema: StructType): Schema = {
    val fields = schema.fields.map(mleapToAvroField).asJava
    Schema.createRecord("LeapFrame", "", "ml.combust.mleap.avro", false, fields)
  }

  implicit def mleapToAvroField(field: StructField): Schema.Field = new Schema.Field(field.name, mleapToAvroType(field.dataType), "", null: AnyRef)

  implicit def mleapToAvroType(dataType: DataType): Schema = dataType match {
    case DoubleType => Schema.create(Schema.Type.DOUBLE)
    case StringType => Schema.create(Schema.Type.STRING)
    case LongType => Schema.create(Schema.Type.LONG)
    case IntegerType => Schema.create(Schema.Type.INT)
    case BooleanType => Schema.create(Schema.Type.BOOLEAN)
    case lt: ListType => Schema.createArray(mleapToAvroType(lt.base))
    case tt: TensorType => tensorSchema(tt)
    case ct: CustomType => customSchema(ct)
    case AnyType => throw new IllegalArgumentException(s"invalid data type: $dataType")
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

  implicit def avroToMleapType(schema: Schema)
                              (implicit context: MleapContext): DataType = schema.getType match {
    case Schema.Type.DOUBLE => DoubleType
    case Schema.Type.STRING => StringType
    case Schema.Type.LONG => LongType
    case Schema.Type.INT => IntegerType
    case Schema.Type.BOOLEAN => BooleanType
    case Schema.Type.ARRAY => ListType(avroToMleapType(schema.getElementType))
    case Schema.Type.RECORD =>
      schema.getName match {
        case "Tensor" => mleapTensorTypeFormat.read(schema.getDoc.parseJson)
        case "Custom" => context.customTypes(schema.getDoc)
        case _ => throw new IllegalArgumentException("invalid avro record")
      }
    case _ => throw new IllegalArgumentException("invalid avro record")
  }
}
