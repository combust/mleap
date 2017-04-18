package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.attribute.{Attribute, BinaryAttribute, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.sql.types.StructField

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

class ReverseStringIndexerOp extends OpNode[SparkBundleContext, IndexToString, IndexToString] {
  override val Model: OpModel[SparkBundleContext, IndexToString] = new OpModel[SparkBundleContext, IndexToString] {
    override val klazz: Class[IndexToString] = classOf[IndexToString]

    override def opName: String = Bundle.BuiltinOps.feature.reverse_string_indexer

    override def store(model: Model, obj: IndexToString)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val labels = obj.get(obj.labels).getOrElse {
        assert(context.context.dataset.isDefined, "must supply a transformed data frame to serialize IndexToString if labels parameter is not set")
        val df = context.context.dataset.get
        ReverseStringIndexerOp.labelsForField(df.schema(obj.getInputCol))
      }

      model.withAttr("labels", Value.stringList(labels))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): IndexToString = {
      new IndexToString(uid = "").setLabels(model.value("labels").getStringList.toArray)
    }
  }

  override val klazz: Class[IndexToString] = classOf[IndexToString]

  override def name(node: IndexToString): String = node.uid

  override def model(node: IndexToString): IndexToString = node

  override def load(node: Node, model: IndexToString)
                   (implicit context: BundleContext[SparkBundleContext]): IndexToString = {
    new IndexToString(uid = node.name).setLabels(model.getLabels)
  }

  override def shape(node: IndexToString)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    Shape().withStandardIO(node.getInputCol, node.getOutputCol)
  }
}
