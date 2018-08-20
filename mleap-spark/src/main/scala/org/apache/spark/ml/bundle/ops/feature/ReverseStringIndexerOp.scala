package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import ml.combust.mleap.core.types.{DataShape, ScalarShape}
import org.apache.spark.ml.attribute.{Attribute, BinaryAttribute, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.sql.types.StructField
import ml.combust.mleap.runtime.types.BundleTypeConverters._

import scala.util.{Failure, Try}

/**
  * Created by hollinwilkins on 8/21/16.
  */
object ReverseStringIndexerOp {
  def labelsForField(field: StructField): Array[String] = {
    val attr = Attribute.fromStructField(field)

    (attr match {
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          Try(nominal.values.get)
        } else {
          Failure(new RuntimeException(s"invalid nominal value for field ${field.name}"))
        }
      case _: BinaryAttribute =>
        Failure(new RuntimeException(s"invalid binary attribute for field ${field.name}"))
      case _: NumericAttribute =>
        Failure(new RuntimeException(s"invalid numeric attribute for field ${field.name}"))
      case _ =>
        Failure(new RuntimeException(s"unsupported attribute for field ${field.name}")) // optimistic about unknown attributes
    }).get
  }
}

class ReverseStringIndexerOp extends SimpleSparkOp[IndexToString] {
  override val Model: OpModel[SparkBundleContext, IndexToString] = new OpModel[SparkBundleContext, IndexToString] {
    override val klazz: Class[IndexToString] = classOf[IndexToString]

    override def opName: String = Bundle.BuiltinOps.feature.reverse_string_indexer

    override def store(model: Model, obj: IndexToString)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val labels = obj.get(obj.labels).getOrElse {
        assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))
        val df = context.context.dataset.get
        ReverseStringIndexerOp.labelsForField(df.schema(obj.getInputCol))
      }

      model.withValue("labels", Value.stringList(labels)).
        withValue("input_shape", Value.dataShape(ScalarShape(false)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): IndexToString = {
      model.getValue("input_shape").map(_.getDataShape: DataShape).foreach {
        shape =>
          require(shape.isScalar, "cannot deserialize non-scalar input to Spark IndexToString model")
      }
      
      new IndexToString(uid = "").setLabels(model.value("labels").getStringList.toArray)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: IndexToString): IndexToString = {
    new IndexToString(uid = uid)
  }

  override def sparkInputs(obj: IndexToString): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: IndexToString): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
