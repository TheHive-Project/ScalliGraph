package org.thp.scalligraph.query

import gremlin.scala.{Graph, Vertex}
import org.thp.scalligraph.BadRequestError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{Converter, Traversal, UntypedTraversal}
import play.api.libs.json.JsObject

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.Try

class PublicProperty[D, G, C <: Converter[D, G]](
    val stepType: ru.Type,
    val propertyName: String,
    val mapping: Mapping[D, _, G],
    val noValue: NoValue[G],
    definition: Seq[(FPath, Traversal[_, _, _]) => Traversal[D, G, C]],
    val fieldsParser: FieldsParser[D],
    val updateFieldsParser: Option[FieldsParser[PropertyUpdater]]
) {
  type Graph  = G
  type Domain = D

  lazy val propertyPath: FPath = FPath(propertyName)

  def get(steps: Traversal[_, _, _], path: FPath): Traversal[D, G, C] =
    if (definition.lengthCompare(1) == 0)
      definition.head.apply(path, steps)
    else
      steps
        .coalesce(t => definition.map(d => d.apply(path, t)): _*)
        .typed[D, G](ClassTag(mapping.graphTypeClass)) // (mapping.toDomain) FIXME add mapping
}

object PublicProperty {

  def getPropertyTraversal(
      properties: Seq[PublicProperty[_, _, _]],
      stepType: ru.Type,
      step: UntypedTraversal,
      fieldName: String,
      authContext: AuthContext
  ): UntypedTraversal = {
    val path = FPath(fieldName)
    properties
      .iterator
      .collectFirst {
        case p if p.stepType =:= stepType && path.startsWith(p.propertyPath).isDefined => p.get(step, path).untyped
      }
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $stepType not found"))
  }

  def getProperty(
      properties: Seq[PublicProperty[_, _, _]],
      stepType: ru.Type,
      fieldName: String
  ): PublicProperty[_, _, _] = {
    val path = FPath(fieldName)
    properties
      .iterator
      .collectFirst {
        case p if p.stepType =:= stepType && path.startsWith(p.propertyPath).isDefined => p
      }
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $stepType not found"))
  }
}

object PropertyUpdater {

  def apply[D, V](fieldsParser: FieldsParser[V], propertyName: String)(
      f: (FPath, V, Vertex, Database, Graph, AuthContext) => Try[JsObject]
  ): FieldsParser[PropertyUpdater] =
    new FieldsParser(
      fieldsParser.formatName,
      fieldsParser.acceptedInput.map(propertyName + "/" + _), {
        case (path, field) =>
          fieldsParser(path, field).map(fieldValue =>
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
