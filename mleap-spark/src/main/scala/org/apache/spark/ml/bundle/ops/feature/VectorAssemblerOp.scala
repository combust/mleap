package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.DataShape
import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.mleap.TypeConverters._
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 8/21/16.
  */
class VectorAssemblerOp extends SimpleSparkOp[VectorAssembler] {
  override val Model: OpModel[SparkBundleContext, VectorAssembler] = new OpModel[SparkBundleContext, VectorAssembler] {
    override val klazz: Class[VectorAssembler] = classOf[VectorAssembler]

    override def opName: String = Bundle.BuiltinOps.feature.vector_assembler

    override def store(model: Model, obj: VectorAssembler)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val dataset = context.context.dataset.get
      val inputShapes = obj.getInputCols.map(i => sparkToMleapDataShape(dataset.schema(i), dataset): DataShape)

      model.withValue("input_shapes", Value.dataShapeList(inputShapes))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): VectorAssembler = { new VectorAssembler(uid = "") }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: VectorAssembler): VectorAssembler = {
    new VectorAssembler(uid = uid)
  }

  override def sparkInputs(obj: VectorAssembler): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCols)
  }

  override def sparkOutputs(obj: VectorAssembler): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
