package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.Transformer

trait MultiInOutTransformer extends  Transformer {
  override def inputSchema: StructType = {
   if (shape.getInput("input").isDefined) {
      val fields = model.inputSchema.getField("input0").map {
        case StructField(_, dataType) => StructField(shape.input("input").name, dataType)
      }.toSeq
      StructType(fields).get
    } else {
      super.inputSchema
    }
  }

  override def outputSchema: StructType = {
   if (shape.getOutput("output").isDefined) {
     val fields = model.outputSchema.getField("output0").map {
        case StructField(_, dataType) => StructField(shape.output("output").name, dataType)
      }.toSeq
     StructType(fields).get
    } else {
     super.outputSchema
    }
  }
}
