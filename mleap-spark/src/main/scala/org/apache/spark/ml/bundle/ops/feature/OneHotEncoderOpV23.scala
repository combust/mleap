package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.attribute.{Attribute, BinaryAttribute, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.OneHotEncoderModel
import org.apache.spark.sql.types.StructField

import scala.util.{Failure, Try}

object OneHotEncoderV23Op {
  def sizeForField(field: StructField): Int = {
    val attr = Attribute.fromStructField(field)

    (attr match {
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          Try(nominal.values.get.length)
        } else if (nominal.numValues.isDefined) {
          Try(nominal.numValues.get)
        } else {
          Failure(new RuntimeException(s"invalid nominal value for field ${field.name}"))
        }
      case binary: BinaryAttribute =>
        Try(2)
      case _: NumericAttribute =>
        Failure(new RuntimeException(s"invalid numeric attribute for field ${field.name}"))
      case _ =>
        Failure(new RuntimeException(s"unsupported attribute for field ${field.name}")) // optimistic about unknown attributes
    }).get
  }
}

class OneHotEncoderOpV23 extends SimpleSparkOp[OneHotEncoderModel] {
  override val Model: OpModel[SparkBundleContext, OneHotEncoderModel] = new OpModel[SparkBundleContext, OneHotEncoderModel] {
    override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(model: Model, obj: OneHotEncoderModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val df = context.context.dataset.get
      val categorySizes = obj.getInputCols.map { f ⇒ OneHotEncoderV23Op.sizeForField(df.schema(f)) }
      val dropLast = obj.getDropLast

      model.withValue("category_sizes", Value.intList(categorySizes))
        .withValue("drop_last", Value.boolean(obj.getDropLast))
        .withValue("handle_invalid", Value.string(obj.getHandleInvalid))

    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): OneHotEncoderModel = {
      new OneHotEncoderModel(uid = "", categorySizes = model.value("category_sizes").getIntList.toArray)
          .setDropLast(model.value("drop_last").getBoolean)
          .setHandleInvalid(model.value("handle_invalid").getString)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: OneHotEncoderModel): OneHotEncoderModel = {
    new OneHotEncoderModel(uid = uid, categorySizes = model.categorySizes)
      .setDropLast(model.getDropLast)
      .setHandleInvalid(model.getHandleInvalid)
  }

  override def sparkInputs(obj: OneHotEncoderModel): Seq[ParamSpec] = Seq(ParamSpec("input", obj.inputCols))

  override def sparkOutputs(obj: OneHotEncoderModel): Seq[ParamSpec] = Seq(ParamSpec("output", obj.outputCols))

}
