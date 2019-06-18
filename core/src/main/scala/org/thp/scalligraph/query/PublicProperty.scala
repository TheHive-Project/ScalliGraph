package org.thp.scalligraph.query

import scala.reflect.runtime.{universe ⇒ ru}
import scala.util.Try

import play.api.libs.json.JsObject

import gremlin.scala.{Graph, GremlinScala, Vertex}
import org.thp.scalligraph.FPath
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.FieldsParser
import org.thp.scalligraph.models.{Database, Mapping}

class PublicProperty[D, G](
    val stepType: ru.Type,
    val propertyName: String,
    val mapping: Mapping[D, _, G],
    val definition: Seq[GremlinScala[Vertex] ⇒ GremlinScala[G]],
    updateFieldsParserBuilder: PublicProperty[D, G] ⇒ Option[FieldsParser[PropertyUpdater]]
) {

  def updateFieldsParser: Option[FieldsParser[PropertyUpdater]] = updateFieldsParserBuilder(this)

  def get(elementTraversal: GremlinScala[Vertex], authContext: AuthContext): GremlinScala[G] =
    if (definition.lengthCompare(1) == 0)
      definition.head.apply(elementTraversal)
    else
      elementTraversal.coalesce(definition: _*)
}

object PropertyUpdater {

  def apply[D, V](fieldsParser: FieldsParser[V], publicProperty: PublicProperty[D, _])(
      f: (PublicProperty[D, _], FPath, V, Vertex, Database, Graph, AuthContext) ⇒ Try[JsObject]
  ): FieldsParser[PropertyUpdater] =
    new FieldsParser(
      fieldsParser.formatName,
      fieldsParser.acceptedInput.map(publicProperty.propertyName + "/" + _), {
        case (path, field) ⇒
          fieldsParser(path, field).map(
            fieldValue ⇒
              new PropertyUpdater(publicProperty, path, fieldValue) {
                override def apply(vertex: Vertex, db: Database, graph: Graph, authContext: AuthContext): Try[JsObject] =
                  f(publicProperty, path, fieldValue, vertex, db, graph, authContext)
              }
          )
      }
    )

  def unapply(updater: PropertyUpdater): Option[(PublicProperty[_, _], FPath, Any, (Vertex, Database, Graph, AuthContext) ⇒ Try[JsObject])] =
    Some((updater.property, updater.path, updater.value, updater.apply))
}

abstract class PropertyUpdater(val property: PublicProperty[_, _], val path: FPath, val value: Any)
    extends ((Vertex, Database, Graph, AuthContext) ⇒ Try[JsObject]) {
  override def toString(): String =
    s"PropertyUpdater(${property.stepType}:${property.propertyName}:${property.mapping.domainTypeClass.getSimpleName}, $path, $value)"
}
