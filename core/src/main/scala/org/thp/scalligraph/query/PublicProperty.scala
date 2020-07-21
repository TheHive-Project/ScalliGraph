package org.thp.scalligraph.query

import gremlin.scala.{Graph, Vertex}
import org.thp.scalligraph.BadRequestError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{Converter, Traversal}
import play.api.libs.json.JsObject

import scala.reflect.runtime.{universe => ru}
import scala.util.Try

class PublicProperty[P, U](
    val traversalType: ru.Type,
    val propertyName: String,
    val mapping: Mapping[P, U, _],
    definition: Seq[(FPath, Traversal.ANY) => Traversal.Domain[U]],
    val fieldsParser: FieldsParser[U],
    val updateFieldsParser: Option[FieldsParser[PropertyUpdater]]
) {
  lazy val propertyPath: FPath = FPath(propertyName)

  def select(traversal: Traversal.ANY, path: FPath): Traversal.Domain[U] =
    if (definition.lengthCompare(1) == 0)
      definition.head(path, traversal)
    else
      traversal
        .coalesce[U, Any, Converter[U, Any]](definition.map(t => t(path, _: Traversal.ANY).cast[U, Any]): _*)

  def get(traversal: Traversal.ANY, path: FPath): Traversal[P, Any, Converter[P, Any]] =
    select(traversal, path).cast[U, Any].fold.map(x => mapping.wrap(x)).cast[P, Any]
}

object PublicProperty {
  def getProperty(
      properties: Seq[PublicProperty[_, _]],
      traversalType: ru.Type,
      fieldName: String
  ): PublicProperty[Any, Any] = {
    val path = FPath(fieldName)
    properties
      .iterator
      .collectFirst {
        case p if path.startsWith(p.propertyPath).isDefined && traversalType <:< p.traversalType =>
          p.asInstanceOf[PublicProperty[Any, Any]]
      }
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $traversalType not found"))
  }

//  def getPropertyTraversal(
//      properties: Seq[PublicProperty[_, _]],
//      traversalType: ru.Type,
//      step: UntypedTraversal,
//      fieldName: String,
//      authContext: AuthContext
//  ): UntypedTraversal = {
//    val path = FPath(fieldName)
//    properties
//      .iterator
//      .collectFirst {
//        case p if p.traversalType =:= traversalType && path.startsWith(p.propertyPath).isDefined => p.get(step, path).untyped
//      }
//      .getOrElse(throw BadRequestError(s"Property $fieldName for type $traversalType not found"))
//  }

//  def getProperty(
//      properties: Seq[PublicProperty[_, _]],
//      traversalType: ru.Type,
//      fieldName: String
//  ): PublicProperty[_, _] = {
//    val path = FPath(fieldName)
//    properties
//      .iterator
//      .collectFirst {
//        case p if p.traversalType =:= traversalType && path.startsWith(p.propertyPath).isDefined => p
//      }
//      .getOrElse(throw BadRequestError(s"Property $fieldName for type $traversalType not found"))
//  }
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
