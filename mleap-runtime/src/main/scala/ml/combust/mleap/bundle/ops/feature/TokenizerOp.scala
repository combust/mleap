package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, Node, NodeShape}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.TokenizerModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Tokenizer

/**
  * Created by hollinwilkins on 10/30/16.
  */
class TokenizerOp extends MleapOp[Tokenizer, TokenizerModel] {
  override val Model: OpModel[MleapContext, TokenizerModel] = new OpModel[MleapContext, TokenizerModel] {
    override val klazz: Class[TokenizerModel] = classOf[TokenizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.tokenizer

    override def store(model: Model, obj: TokenizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = model

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): TokenizerModel = TokenizerModel()
  }

  override def model(node: Tokenizer): TokenizerModel = TokenizerModel.defaultTokenizer
}
