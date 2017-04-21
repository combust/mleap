package ml.combust.bundle.test.ops

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Shape, Value}
import ml.combust.bundle.dsl
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.test.MyCustomObject

/**
  * Created by hollinwilkins on 10/30/16.
  */
case class MyCustomTransformer(custom: MyCustomObject) extends Transformer {
  override val uid: String = java.util.UUID.randomUUID().toString
}

class MyCustomOp extends OpNode[Any, MyCustomTransformer, MyCustomTransformer] {
  override val Model: OpModel[Any, MyCustomTransformer] = new OpModel[Any, MyCustomTransformer] {
    override val klazz: Class[MyCustomTransformer] = classOf[MyCustomTransformer]

    override def opName: String = "my_custom_transformer"


    override def store(model: Model, obj: MyCustomTransformer)
                      (implicit context: BundleContext[Any]): Model = {
      model.withAttr("my_custom_attr", Value.custom(obj.custom))
    }


    override def load(model: Model)
                     (implicit context: BundleContext[Any]): MyCustomTransformer = {
      MyCustomTransformer(model.value("my_custom_attr").getCustom[MyCustomObject])
    }
  }

  override val klazz: Class[MyCustomTransformer] = classOf[MyCustomTransformer]

  override def name(node: MyCustomTransformer): String = node.uid

  override def model(node: MyCustomTransformer): MyCustomTransformer = node

  override def load(node: dsl.Node, model: MyCustomTransformer)
                   (implicit context: BundleContext[Any]): MyCustomTransformer = {
    MyCustomTransformer(model.custom)
  }

  override def shape(node: MyCustomTransformer)(implicit context: BundleContext[Any]): Shape = Shape()
}
