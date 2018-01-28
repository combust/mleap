package ml.combust.bundle.v07.tree

import ml.bundle.tree.clustering.Node.{Node => ClusterNode}
import ml.bundle.tree.decision.Node.{Node => DecisionNode}
import ml.bundle.tree.decision.Node.Node.{InternalNode, LeafNode}
import ml.bundle.tree.decision.Split.Split
import ml.bundle.tree.decision.Split.Split.{CategoricalSplit, ContinuousSplit}
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * Created by hollinwilkins on 12/24/16.
  */
trait JsonSupport {
  implicit val bundleTreeCategoricalSplitFormat: RootJsonFormat[CategoricalSplit] = jsonFormat4(CategoricalSplit.apply)
  implicit val bundleTreeContinuousSplitFormat: RootJsonFormat[ContinuousSplit] = jsonFormat2(ContinuousSplit.apply)

  implicit val bundleTreeSplitFormat: RootJsonFormat[Split] = new RootJsonFormat[Split] {
    override def write(obj: Split): JsValue = {
      val (tpe, json) = if(obj.s.isCategorical) {
        ("categorical", obj.getCategorical.toJson.asJsObject)
      } else if(obj.s.isContinuous) {
        ("continuous", obj.getContinuous.toJson.asJsObject)
      } else {
        serializationError("invalid split")
      }

      JsObject(("type" -> JsString(tpe)) +: json.fields.toSeq: _*)
    }

    override def read(json: JsValue): Split = json match {
      case json: JsObject =>
        json.fields("type") match {
          case JsString("categorical") => Split(Split.S.Categorical(json.convertTo[CategoricalSplit]))
          case JsString("continuous") => Split(Split.S.Continuous(json.convertTo[ContinuousSplit]))
          case _ => deserializationError("invalid split")
        }
      case _ => deserializationError("invalid split")
    }
  }

  implicit val bundleTreeInternalNodeFormat: RootJsonFormat[InternalNode] = jsonFormat1(InternalNode.apply)
  implicit val bundleTreeLeafNodeFormat: RootJsonFormat[LeafNode] = jsonFormat1(LeafNode.apply)

  implicit val bundleTreeNodeFormat: RootJsonFormat[DecisionNode] = new RootJsonFormat[DecisionNode] {
    override def write(obj: DecisionNode): JsValue = {
      val (tpe, json) = if(obj.n.isInternal) {
        ("internal", obj.getInternal.toJson.asJsObject)
      } else if(obj.n.isLeaf) {
        ("leaf", obj.getLeaf.toJson.asJsObject)
      } else {
        serializationError("invalid node")
      }

      JsObject(("type" -> JsString(tpe)) +: json.fields.toSeq: _*)
    }

    override def read(json: JsValue): DecisionNode = json match {
      case json: JsObject =>
        json.fields("type") match {
          case JsString("internal") => DecisionNode(DecisionNode.N.Internal(json.convertTo[InternalNode]))
          case JsString("leaf") => DecisionNode(DecisionNode.N.Leaf(json.convertTo[LeafNode]))
          case _ => deserializationError("invalid node")
        }
      case _ => deserializationError("invalid node")
    }
  }

  implicit val bundleClusteringTreeNodeFormat: RootJsonFormat[ClusterNode] = {
    jsonFormat4(ClusterNode.apply)
  }
}
object JsonSupport extends JsonSupport
