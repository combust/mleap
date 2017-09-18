package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.DataShape
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.Interaction
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.mleap.TypeConverters._
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import org.apache.spark.sql.types.{BooleanType, NumericType}

/**
  * Created by hollinwilkins on 4/26/17.
  */
class InteractionOp extends SimpleSparkOp[Interaction] {
  override val Model: OpModel[SparkBundleContext, Interaction] = new OpModel[SparkBundleContext, Interaction] {
    override val klazz: Class[Interaction] = classOf[Interaction]

    override def opName: String = Bundle.BuiltinOps.feature.interaction

    override def store(model: Model, obj: Interaction)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val dataset = context.context.dataset.get
      val spec = buildSpec(obj.getInputCols, dataset)
      val inputShapes = obj.getInputCols.map(v => sparkToMleapDataShape(dataset.schema(v), dataset): DataShape)

      val m = model.withValue("num_inputs", Value.int(spec.length)).
        withValue("input_shapes", Value.dataShapeList(inputShapes))

      spec.zipWithIndex.foldLeft(m) {
        case (m2, (numFeatures, index)) => m2.withValue(s"num_features$index", Value.intList(numFeatures))
      }
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Interaction = {
      // No need to do anything here, everything is handled through Spark meta data
      new Interaction()
    }

    @SparkCode(uri = "https://github.com/apache/spark/blob/branch-2.1/mllib/src/main/scala/org/apache/spark/ml/feature/Interaction.scala")
    private def buildSpec(inputCols: Array[String], dataset: DataFrame): Array[Array[Int]] = {
      def getNumFeatures(attr: Attribute): Int = {
        attr match {
          case nominal: NominalAttribute =>
            math.max(1, nominal.getNumValues.getOrElse(
              throw new IllegalArgumentException("Nominal features must have attr numValues defined.")))
          case _ =>
            1  // numeric feature
        }
      }

      inputCols.map(dataset.schema.apply).map { f =>
        f.dataType match {
          case _: NumericType | BooleanType =>
            Array(getNumFeatures(Attribute.fromStructField(f)))
          case _: VectorUDT =>
            val attrs = AttributeGroup.fromStructField(f).attributes.getOrElse(
              throw new IllegalArgumentException("Vector attributes must be defined for interaction."))
            attrs.map(getNumFeatures)
        }
      }
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: Interaction): Interaction = {
    new Interaction(uid = uid)
  }

  override def sparkInputs(obj: Interaction): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCols)
  }

  override def sparkOutputs(obj: Interaction): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
