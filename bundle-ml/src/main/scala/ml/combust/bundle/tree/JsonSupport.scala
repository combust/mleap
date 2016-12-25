package ml.combust.bundle.tree

import ml.bundle.tree.Node.Node
import ml.bundle.tree.Node.Node.{InternalNode, LeafNode}
import ml.bundle.tree.Split.Split
import ml.bundle.tree.Split.Split.{CategoricalSplit, ContinuousSplit}
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

  implicit val bundleTreeNodeFormat: RootJsonFormat[Node] = new RootJsonFormat[Node] {
    override def write(obj: Node): JsValue = {
      val (tpe, json) = if(obj.n.isInternal) {
        ("internal", obj.getInternal.toJson.asJsObject)
      } else if(obj.n.isLeaf) {
        ("leaf", obj.getLeaf.toJson.asJsObject)
      } else {
        serializationError("invalid node")
      }

      JsObject(("type" -> JsString(tpe)) +: json.fields.toSeq: _*)
    }

    override def read(json: JsValue): Node = json match {
      case json: JsObject =>
        json.fields("type") match {
          case JsString("internal") => Node(Node.N.Internal(json.convertTo[InternalNode]))
          case JsString("leaf") => Node(Node.N.Leaf(json.convertTo[LeafNode]))
          case _ => deserializationError("invalid node")
        }
      case _ => deserializationError("invalid node")
    }
  }
}
object JsonSupport extends JsonSupport
