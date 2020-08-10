package org.thp.scalligraph.query

import org.apache.tinkerpop.gremlin.structure.{Graph, Vertex}
import org.thp.scalligraph.BadRequestError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Traversal}
import play.api.libs.json.JsObject

import scala.reflect.runtime.{universe => ru}
import scala.util.Try

class PublicProperty[P, U](
    val traversalType: ru.Type,
    val propertyName: String,
    val mapping: Mapping[P, U, _],
    definition: Seq[(FPath, Traversal.Unk) => Traversal.Domain[U]],
    val fieldsParser: FieldsParser[U],
    val updateFieldsParser: Option[FieldsParser[PropertyUpdater]],
    val filterSelect: (FPath, Traversal.Unk) => Traversal.Unk,
    val filterConverter: FPath => Converter[Traversal.UnkG, Traversal.UnkD]
) {
  lazy val propertyPath: FPath = FPath(propertyName)

  def select(path: FPath, traversal: Traversal.Unk): Traversal.Domain[U] =
    if (definition.lengthCompare(1) == 0)
      definition.head(path, traversal)
    else
      traversal
        .coalesce[U, Traversal.UnkG, Converter[U, Traversal.UnkG]](definition.map(t => t(path, _: Traversal.Unk).castDomain[U]): _*)

  def get(path: FPath, traversal: Traversal.Unk): Traversal[P, Any, Converter[P, Any]] =
    select(path, traversal).cast[U, Any].fold.domainMap(x => mapping.wrap(x)).cast[P, Any]
}

object PublicProperty {

  def getProperty(
      properties: Seq[PublicProperty[_, _]],
      traversalType: ru.Type,
      fieldName: String
  ): PublicProperty[Traversal.UnkD, Traversal.UnkDU] = getProperty(properties, traversalType, FPath(fieldName))

  def getProperty(
      properties: Seq[PublicProperty[_, _]],
      traversalType: ru.Type,
      fieldPath: FPath
  ): PublicProperty[Traversal.UnkD, Traversal.UnkDU] =
    properties
      .iterator
      .collectFirst {
        case p if fieldPath.startsWith(p.propertyPath).isDefined && traversalType <:< p.traversalType =>
          p.asInstanceOf[PublicProperty[Traversal.UnkD, Traversal.UnkDU]]
      }
      .getOrElse(throw BadRequestError(s"Property $fieldPath for type $traversalType not found"))

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
