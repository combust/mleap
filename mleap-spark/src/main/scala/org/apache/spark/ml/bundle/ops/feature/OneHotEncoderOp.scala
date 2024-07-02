package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.attribute.{Attribute, BinaryAttribute, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.OneHotEncoderModel
import org.apache.spark.sql.types.StructField

import scala.util.{Failure, Try}

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

class OneHotEncoderOp extends SimpleSparkOp[OneHotEncoderModel] {
  override val Model: OpModel[SparkBundleContext, OneHotEncoderModel] = new OpModel[SparkBundleContext, OneHotEncoderModel] {
    override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(model: Model, obj: OneHotEncoderModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))
      assert(!(obj.isSet(obj.inputCol) && obj.isSet(obj.inputCols)), "OneHotEncoderModel cannot have both inputCol and inputCols set")
      assert(!(obj.isSet(obj.outputCol) && obj.isSet(obj.outputCols)), "OneHotEncoderModel cannot have both outputCol and outputCols set")
      val inputCols = if (obj.isSet(obj.inputCol)) Array(obj.getInputCol) else obj.getInputCols
      val df = context.context.dataset.get
      val categorySizes = inputCols.map { f ⇒ OneHotEncoderOp.sizeForField(df.schema(f)) }
      var m = model.withValue("category_sizes", Value.intList(categorySizes))
        .withValue("drop_last", Value.boolean(obj.getDropLast))
        .withValue("handle_invalid", Value.string(obj.getHandleInvalid))
      if (obj.isSet(obj.inputCol)) {
        m = m.withValue("inputCol", Value.string(obj.getInputCol))
      }
      if (obj.isSet(obj.inputCols)) {
        m = m.withValue("inputCols", Value.stringList(obj.getInputCols))
      }
      if (obj.isSet(obj.outputCol)) {
        m = m.withValue("outputCol", Value.string(obj.getOutputCol))
      }
      if (obj.isSet(obj.outputCols)) {
        m = m.withValue("outputCols", Value.stringList(obj.getOutputCols))
      }
      m
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): OneHotEncoderModel = {
      val m = new OneHotEncoderModel(uid = "", categorySizes = model.value("category_sizes").getIntList.toArray)
        .setDropLast(model.value("drop_last").getBoolean)
        .setHandleInvalid(model.value("handle_invalid").getString)
      if (model.getValue("inputCol").isDefined) {
        m.setInputCol(model.value("inputCol").getString)
      }
      if (model.getValue("inputCols").isDefined) {
        m.setInputCols(model.value("inputCols").getStringList.toArray)
      }
      if (model.getValue("outputCol").isDefined) {
        m.setOutputCol(model.value("outputCol").getString)
      }
      if (model.getValue("outputCols").isDefined) {
        m.setOutputCols(model.value("outputCols").getStringList.toArray)
      }
      m
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: OneHotEncoderModel): OneHotEncoderModel = {
    val m = new OneHotEncoderModel(uid = uid, categorySizes = model.categorySizes)
      .setDropLast(model.getDropLast)
      .setHandleInvalid(model.getHandleInvalid)
    if (model.isSet(model.inputCol)) {
      m.setInputCol(model.getInputCol)
    }
    if (model.isSet(model.inputCols)) {
      m.setInputCols(model.getInputCols)
    }
    if (model.isSet(model.outputCol)) {
      m.setOutputCol(model.getOutputCol)
    }
    if (model.isSet(model.outputCols)) {
      m.setOutputCols(model.getOutputCols)
    }
    m
  }

  override def sparkInputs(obj: OneHotEncoderModel): Seq[ParamSpec] = {
    obj.isSet(obj.inputCol) match {
      case true => Seq(ParamSpec("input", obj.inputCol))
      case false => Seq(ParamSpec("input", obj.inputCols))
    }
    
  }

  override def sparkOutputs(obj: OneHotEncoderModel): Seq[ParamSpec] = {
    obj.isSet(obj.outputCol) match {
      case true => Seq(ParamSpec("output", obj.outputCol))
      case false => Seq(ParamSpec("output", obj.outputCols))
    }
    
  }

}
