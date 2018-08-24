package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import org.apache.spark.ml.attribute.{Attribute, BinaryAttribute, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.sql.types.StructField

import scala.util.{Failure, Try}

/**
  * Created by hollinwilkins on 8/21/16.
  */
object OneHotEncoderOp {
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

class OneHotEncoderOp extends SimpleSparkOp[OneHotEncoder] {
  override val Model: OpModel[SparkBundleContext, OneHotEncoder] = new OpModel[SparkBundleContext, OneHotEncoder] {
    override val klazz: Class[OneHotEncoder] = classOf[OneHotEncoder]

    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(model: Model, obj: OneHotEncoder)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val df = context.context.dataset.get
      val size = OneHotEncoderOp.sizeForField(df.schema(obj.getInputCol))
      val dropLast = obj.getDropLast

      model.withValue("size", Value.long(size)).
        withValue("drop_last", Value.boolean(dropLast))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): OneHotEncoder = {
      new OneHotEncoder(uid = "")
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: OneHotEncoder): OneHotEncoder = {
    new OneHotEncoder(uid = uid)
  }

  override def sparkInputs(obj: OneHotEncoder): Seq[ParamSpec] = {
    Seq("input0" -> obj.inputCol)
  }

  override def sparkOutputs(obj: OneHotEncoder): Seq[SimpleParamSpec] = {
    Seq("output0" -> obj.outputCol)
  }
}
