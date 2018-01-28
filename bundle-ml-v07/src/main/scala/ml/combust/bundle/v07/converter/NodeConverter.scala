package ml.combust.bundle.v07.converter

import ml.combust.bundle.v07
import ml.bundle.{v07 => bv07}
import java.nio.file.Files

import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.bundle.v07.json.JsonSupport._

import scala.util.Try
import spray.json._

/**
  * Created by hollinwilkins on 1/27/18.
  */
class NodeConverter {
  def convert(context: ConverterContext): Try[Unit] = {
    val tryCopyNode = Try {
      if (Files.exists(context.in.file("node.json"))) {
        Files.copy(context.in.file("node.json"), context.out.file("node.json"))
      } else if (Files.exists(context.in.file("node.pb"))) {
        Files.copy(context.in.file("node.pb"), context.out.file("node.pb"))
      } else {
        throw new IllegalArgumentException(s"invalid input model, no node found at ${context.in.path}")
      }
    }

    (for (_ <- tryCopyNode;
         (format, model) <- readModel(context);
         converter = context.registry.modelConverter(model.op);
         newModel = converter.convert(model);
         _ <- converter.convertModelFiles(context, format, newModel)) yield {
      Try {
        format match {
          case SerializationFormat.Json =>
            Files.write(context.out.file("model.json"), newModel.toJson.prettyPrint.getBytes)
          case SerializationFormat.Protobuf =>
            Files.write(context.out.file("model.pb"), newModel.asBundle.toByteArray)
        }

        ()
      }
    }).flatMap(identity)
  }

  def readModel(context: ConverterContext): Try[(SerializationFormat, v07.dsl.Model)] = {
    Try {
      (Files.exists(context.in.file("model.json")), Files.exists(context.in.file("model.pb"))) match {
        case (true, true) =>
          val m = new String(Files.readAllBytes(context.in.file("model.json"))).parseJson.convertTo[v07.dsl.Model]
          val attrs = v07.dsl.AttributeList.fromBundle(bv07.AttributeList.parseFrom(Files.readAllBytes(context.in.file("model.pb"))))
          (SerializationFormat.Protobuf, v07.dsl.Model(m.op, m.attributes.map(_.lookup ++ attrs.lookup).flatMap(v07.dsl.AttributeList.option).orElse(Some(attrs))))
        case (true, false) =>
          (SerializationFormat.Json, new String(Files.readAllBytes(context.in.file("model.json"))).parseJson.convertTo[v07.dsl.Model])
        case (false, true) =>
          (SerializationFormat.Protobuf, v07.dsl.Model.fromBundle(bv07.ModelDef.parseFrom(Files.readAllBytes(context.in.file("model.pb")))))
        case (false, false) =>
          throw new IllegalArgumentException(s"invalid, no model.json or model.pb found at ${context.in.path}")
      }
    }
  }
}
