package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, Node, Shape}
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.TokenizerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Tokenizer

/**
  * Created by hollinwilkins on 10/30/16.
  */
class TokenizerOp extends OpNode[MleapContext, Tokenizer, TokenizerModel] {
  override val Model: OpModel[MleapContext, TokenizerModel] = new OpModel[MleapContext, TokenizerModel] {
    override val klazz: Class[TokenizerModel] = classOf[TokenizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.tokenizer

    override def store(model: Model, obj: TokenizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = model

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): TokenizerModel = TokenizerModel()
  }

  override val klazz: Class[Tokenizer] = classOf[Tokenizer]

  override def name(node: Tokenizer): String = node.uid

  override def model(node: Tokenizer): TokenizerModel = TokenizerModel.defaultTokenizer

  override def load(node: Node, model: TokenizerModel)
                   (implicit context: BundleContext[MleapContext]): Tokenizer = {
    Tokenizer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name)
  }

  override def shape(node: Tokenizer)(implicit context: BundleContext[MleapContext]): Shape = {
    Shape().withStandardIO(node.inputCol, node.outputCol)
  }
}
