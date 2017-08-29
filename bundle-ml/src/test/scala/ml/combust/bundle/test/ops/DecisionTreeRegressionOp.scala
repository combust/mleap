package ml.combust.bundle.test.ops

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.tree.decision.{NodeWrapper, TreeSerializer}
import ml.combust.bundle.{BundleContext, dsl}
import ml.bundle.dtree

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
case class LeafNode(values: Seq[Double]) extends Node

case class DecisionTreeRegressionModel(root: Node)
case class DecisionTreeRegression(uid: String,
                                  input: String,
                                  output: String,
                                  model: DecisionTreeRegressionModel) extends Transformer

object MyNodeWrapper extends NodeWrapper[Node] {
  override def node(node: Node, withImpurities: Boolean): dtree.Node = node match {
    case node: InternalNode =>
      val split = node.split match {
        case split: CategoricalSplit =>
          val s = dtree.Split.CategoricalSplit(split.featureIndex,
            split.isLeft,
            split.numCategories,
            split.categories)
          dtree.Split(dtree.Split.S.Categorical(s))
        case split: ContinuousSplit =>
          val s = dtree.Split.ContinuousSplit(split.featureIndex, split.threshold)
          dtree.Split(dtree.Split.S.Continuous(s))
      }
      dtree.Node(ml.bundle.dtree.Node.N.Internal(dtree.Node.InternalNode(Some(split))))
    case node: LeafNode =>
      dtree.Node(dtree.Node.N.Leaf(dtree.Node.LeafNode(values = node.values)))
  }

  override def isInternal(node: Node): Boolean = node.isInstanceOf[InternalNode]

  override def leaf(node: dtree.Node.LeafNode, withImpurities: Boolean): Node = {
    LeafNode(values = node.values)
  }

  override def internal(node: dtree.Node.InternalNode, left: Node, right: Node): Node = {
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


    override def store(model: Model, obj: DecisionTreeRegressionModel)
                      (implicit context: BundleContext[Any]): Model = {
      TreeSerializer[Node](context.file("tree"), withImpurities = true).write(obj.root)
      model
    }


    override def load(model: Model)
                     (implicit context: BundleContext[Any]): DecisionTreeRegressionModel = {
      val root = TreeSerializer[Node](context.file("tree"), withImpurities = true).read().get
      DecisionTreeRegressionModel(root)
    }
  }

  override val klazz: Class[DecisionTreeRegression] = classOf[DecisionTreeRegression]

  override def name(node: DecisionTreeRegression): String = node.uid

  override def model(node: DecisionTreeRegression): DecisionTreeRegressionModel = node.model


  override def load(node: dsl.Node, model: DecisionTreeRegressionModel)
                   (implicit context: BundleContext[Any]): DecisionTreeRegression = {
    DecisionTreeRegression(uid = node.name,
      input = node.shape.standardInput.name,
      output = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: DecisionTreeRegression)(implicit context: BundleContext[Any]): NodeShape =
    NodeShape().withStandardIO(node.input, node.output)
}
