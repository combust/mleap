package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.bundle.{BundleHelper, SparkBundleContext}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField

/**
  * Created by hollinwilkins on 12/28/16.
  */
class VectorSlicerOp extends OpNode[SparkBundleContext, VectorSlicer, VectorSlicer] {
  override val Model: OpModel[SparkBundleContext, VectorSlicer] = new OpModel[SparkBundleContext, VectorSlicer] {
    override val klazz: Class[VectorSlicer] = classOf[VectorSlicer]

    override def opName: String = Bundle.BuiltinOps.feature.vector_slicer

    override def store(model: Model, obj: VectorSlicer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val namedIndicesMap: Array[(String, Int)] = if(obj.getNames.nonEmpty) {
        assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))
        val dataset = context.context.dataset.get
        extractNamedIndices(obj.getInputCol, obj.getNames, dataset)
      } else { Array() }
      val (names, namedIndices) = namedIndicesMap.unzip

      model.withValue("indices", Value.longList(obj.getIndices.map(_.toLong).toSeq)).
        withValue("names", Value.stringList(names)).
        withValue("named_indices", Value.intList(namedIndices))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): VectorSlicer = {
      val names = model.value("names").getStringList
      new VectorSlicer(uid = "").setIndices(model.value("indices").getLongList.map(_.toInt).toArray).
        setNames(names.toArray)
    }

    private def extractNamedIndices(inputCol: String,
                                    names: Array[String],
                                    dataset: DataFrame): Array[(String, Int)] = {
      names.zip(getFeatureIndicesFromNames(dataset.schema(inputCol), names))
    }

    private def getFeatureIndicesFromNames(col: StructField, names: Array[String]): Array[Int] = {
      require(col.dataType.isInstanceOf[VectorUDT], s"getFeatureIndicesFromNames expected column $col"
        + s" to be Vector type, but it was type ${col.dataType} instead.")
      val inputAttr = AttributeGroup.fromStructField(col)
      names.map { name =>
        require(inputAttr.hasAttr(name),
          s"getFeatureIndicesFromNames found no feature with name $name in column $col.")
        inputAttr.getAttr(name).index.get
      }
    }
  }

  override val klazz: Class[VectorSlicer] = classOf[VectorSlicer]

  override def name(node: VectorSlicer): String = node.uid

  override def model(node: VectorSlicer): VectorSlicer = node

  override def load(node: Node, model: VectorSlicer)
                   (implicit context: BundleContext[SparkBundleContext]): VectorSlicer = {
    new VectorSlicer(uid = node.name).setIndices(model.getIndices).
      setNames(model.getNames).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: VectorSlicer): NodeShape = NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
