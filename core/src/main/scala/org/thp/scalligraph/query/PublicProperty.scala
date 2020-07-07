package org.thp.scalligraph.query

import gremlin.scala.{Graph, Vertex}
import org.thp.scalligraph.BadRequestError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.Traversal
import play.api.libs.json.JsObject

import scala.reflect.runtime.{universe => ru}
import scala.util.Try

class PublicProperty[D, G](
    val stepType: ru.Type,
    val propertyName: String,
    val mapping: Mapping[_, D, G],
    val noValue: NoValue[G],
    definition: Seq[(FPath, Traversal[_, Vertex]) => Traversal[D, G]],
    val fieldsParser: FieldsParser[D],
    val updateFieldsParser: Option[FieldsParser[PropertyUpdater]]
) {
  type Graph  = G
  type Domain = D

  lazy val propertyPath: FPath = FPath(propertyName)

  def get(steps: Traversal[_, Vertex], path: FPath): Traversal[D, G] =
    if (definition.lengthCompare(1) == 0)
      definition.head.apply(path, steps)
    else
      steps.coalesce(mapping)(definition.map(d => d.apply(path, _)): _*)
}

object PublicProperty {

  def getPropertyTraversal(
      properties: Seq[PublicProperty[_, _]],
      stepType: ru.Type,
      step: Traversal[_, Vertex],
      fieldName: String,
      authContext: AuthContext
  ): Traversal[Any, Any] = {
    val path = FPath(fieldName)
    properties
      .iterator
      .collectFirst {
        case p if p.stepType =:= stepType && path.startsWith(p.propertyPath).isDefined => p.get(step, path)
      }
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $stepType not found"))
  }

  def getProperty(
      properties: Seq[PublicProperty[_, _]],
      stepType: ru.Type,
      fieldName: String
  ): PublicProperty[_, _] = {
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
