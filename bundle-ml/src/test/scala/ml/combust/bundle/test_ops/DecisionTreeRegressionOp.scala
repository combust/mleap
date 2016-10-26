package ml.combust.bundle.test_ops

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.tree.{NodeWrapper, TreeSerializer}
import ml.combust.bundle.dsl._
import ml.combust.bundle.dsl

/**
  * Created by hollinwilkins on 8/22/16.
  */
sealed trait Split
case class CategoricalSplit(featureIndex: Int,
                            isLeft: Boolean,
                            numCategories: Int,
                            categories: Seq[Double]) extends Split
case class ContinuousSplit(featureIndex: Int, threshold: Double) extends Split

sealed trait Node
case class InternalNode(split: Split, left: Node, right: Node) extends Node
case class LeafNode(prediction: Double, impurities: Option[Seq[Double]]) extends Node

case class DecisionTreeRegressionModel(root: Node)
case class DecisionTreeRegression(uid: String,
                                  input: String,
                                  output: String,
                                  model: DecisionTreeRegressionModel) extends Transformer

object MyNodeWrapper extends NodeWrapper[Node] {
  override def node(node: Node, withImpurities: Boolean): ml.bundle.tree.Node.Node = node match {
    case node: InternalNode =>
      val split = node.split match {
        case split: CategoricalSplit =>
          val s = ml.bundle.tree.Split.Split.CategoricalSplit(split.featureIndex,
            split.isLeft,
            split.numCategories,
            split.categories)
          ml.bundle.tree.Split.Split(ml.bundle.tree.Split.Split.S.Categorical(s))
        case split: ContinuousSplit =>
          val s = ml.bundle.tree.Split.Split.ContinuousSplit(split.featureIndex, split.threshold)
          ml.bundle.tree.Split.Split(ml.bundle.tree.Split.Split.S.Continuous(s))
      }
      ml.bundle.tree.Node.Node(ml.bundle.tree.Node.Node.N.Internal(ml.bundle.tree.Node.Node.InternalNode(Some(split))))
    case node: LeafNode =>
      val impurities = if(withImpurities) {
        node.impurities.get
      } else { Seq() }
      ml.bundle.tree.Node.Node(ml.bundle.tree.Node.Node.N.Leaf(ml.bundle.tree.Node.Node.LeafNode(node.prediction, impurities)))
  }

  override def isInternal(node: Node): Boolean = node.isInstanceOf[InternalNode]

  override def leaf(node: ml.bundle.tree.Node.Node.LeafNode, withImpurities: Boolean): Node = {
    val impurities = if(withImpurities) {
      Some(node.impurities)
    } else { None }
    LeafNode(node.prediction, impurities)
  }

  override def internal(node: ml.bundle.tree.Node.Node.InternalNode, left: Node, right: Node): Node = {
    val split = if(node.split.get.s.isCategorical) {
      val s = node.split.get.getCategorical
      CategoricalSplit(s.featureIndex,
        s.isLeft,
        s.numCategories,
        s.categories)
    } else if(node.split.get.s.isContinuous) {
      val s = node.split.get.getContinuous
      ContinuousSplit(s.featureIndex, s.threshold)
    } else { throw new IllegalArgumentException("invalid split") }
    InternalNode(split, left, right)
  }

  override def left(node: Node): Node = node match {
    case node: InternalNode => node.left
    case _ => throw new IllegalArgumentException("not an internal node")
  }
  override def right(node: Node): Node = node match {
    case node: InternalNode => node.right
    case _ => throw new IllegalArgumentException("not an internal node")
  }
}

class DecisionTreeRegressionOp extends OpNode[Any, DecisionTreeRegression, DecisionTreeRegressionModel] {
  implicit val wrapper = MyNodeWrapper
  override val Model: OpModel[Any, DecisionTreeRegressionModel] = new OpModel[Any, DecisionTreeRegressionModel] {
    override val klazz: Class[DecisionTreeRegressionModel] = classOf[DecisionTreeRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.decision_tree_regression

    override def store(context: BundleContext[Any], model: Model, obj: DecisionTreeRegressionModel): Model = {
      TreeSerializer[Node](context.file("nodes"), withImpurities = true).write(obj.root)
      model
    }

    override def load(context: BundleContext[Any], model: Model): DecisionTreeRegressionModel = {
      val root = TreeSerializer[Node](context.file("nodes"), withImpurities = true).read()
      DecisionTreeRegressionModel(root)
    }
  }

  override val klazz: Class[DecisionTreeRegression] = classOf[DecisionTreeRegression]

  override def name(node: DecisionTreeRegression): String = node.uid

  override def model(node: DecisionTreeRegression): DecisionTreeRegressionModel = node.model

  override def load(context: BundleContext[Any], node: dsl.Node, model: DecisionTreeRegressionModel): DecisionTreeRegression = {
    DecisionTreeRegression(uid = node.name,
      input = node.shape.standardInput.name,
      output = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: DecisionTreeRegression): Shape = Shape().withStandardIO(node.input, node.output)
}
