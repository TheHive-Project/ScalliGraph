package org.thp.scalligraph.query

import scala.reflect.runtime.{universe => ru}
import scala.util.Try

import play.api.libs.json.JsObject

import gremlin.scala.{Graph, Vertex}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.steps.{BaseVertexSteps, Traversal}

class PublicProperty[D, G](
    val stepType: ru.Type,
    val propertyName: String,
    val mapping: Mapping[_, D, G],
    val definition: Seq[BaseVertexSteps => Traversal[D, G]],
    val fieldsParser: FieldsParser[D],
    val updateFieldsParser: Option[FieldsParser[PropertyUpdater]]
) {

  def get(steps: BaseVertexSteps, authContext: AuthContext): Traversal[D, G] =
    if (definition.lengthCompare(1) == 0)
      definition.head.apply(steps)
    else
      steps.coalesce[D, G](mapping, definition: _*)
}

object PropertyUpdater {

  def apply[D, V](fieldsParser: FieldsParser[V], propertyName: String)(
      f: (FPath, V, Vertex, Database, Graph, AuthContext) => Try[JsObject]
  ): FieldsParser[PropertyUpdater] =
    new FieldsParser(
      fieldsParser.formatName,
      fieldsParser.acceptedInput.map(propertyName + "/" + _), {
        case (path, field) =>
          fieldsParser(path, field).map(
            fieldValue =>
              new PropertyUpdater(propertyName /: path, fieldValue) {
                override def apply(vertex: Vertex, db: Database, graph: Graph, authContext: AuthContext): Try[JsObject] =
                  f(path, fieldValue, vertex, db, graph, authContext)
              }
          )
      }
    )

  def apply(path: FPath, value: Any)(
      f: (Vertex, Database, Graph, AuthContext) => Try[JsObject]
  ): PropertyUpdater = new PropertyUpdater(path, value) {
    override def apply(v: Vertex, db: Database, graph: Graph, authContext: AuthContext): Try[JsObject] = f(v, db, graph, authContext)
  }

  def unapply(updater: PropertyUpdater): Option[(FPath, Any, (Vertex, Database, Graph, AuthContext) => Try[JsObject])] =
    Some((updater.path, updater.value, updater.apply))
}

abstract class PropertyUpdater(val path: FPath, val value: Any) extends ((Vertex, Database, Graph, AuthContext) => Try[JsObject]) {
  override def toString(): String =
    s"PropertyUpdater($path, $value)"
}
