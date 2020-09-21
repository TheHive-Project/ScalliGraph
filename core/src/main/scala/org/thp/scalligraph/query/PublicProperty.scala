package org.thp.scalligraph.query

import org.apache.tinkerpop.gremlin.structure.{Graph, Vertex}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Traversal}
import play.api.libs.json.JsObject

import scala.reflect.runtime.{universe => ru}
import scala.util.Try

class PublicProperty[P, U](
    val isApplicableFn: ru.Type => Boolean,
    val propertyName: String,
    val mapping: Mapping[P, U, _],
    definition: (FPath, Traversal.Unk) => Traversal.Domain[U],
    val fieldsParser: FieldsParser[U],
    val updateFieldsParser: Option[FieldsParser[PropertyUpdater]],
    val filterSelect: (FPath, Traversal.Unk) => Traversal.Unk,
    val filterConverter: FPath => Converter[Traversal.UnkG, Traversal.UnkD]
) {
  lazy val propertyPath: FPath = FPath(propertyName)

  def select(path: FPath, traversal: Traversal.Unk): Traversal.Domain[U] =
    definition(path, traversal)

  def get(path: FPath, traversal: Traversal.Unk): Traversal[P, Any, Converter[P, Any]] =
    select(path, traversal).cast[U, Any].fold.domainMap(x => mapping.wrap(x)).cast[P, Any]

  def isApplicable(traversalType: ru.Type): Boolean = isApplicableFn(traversalType)
}

object PropertyUpdater {

  def apply[D, V](fieldsParser: FieldsParser[V], propertyName: String)(
      f: (FPath, V, Vertex, Database, Graph, AuthContext) => Try[JsObject]
  ): FieldsParser[PropertyUpdater] =
    new FieldsParser(
      fieldsParser.formatName,
      fieldsParser.acceptedInput.map(propertyName + "/" + _),
      {
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
  ): PropertyUpdater =
    new PropertyUpdater(path, value) {
      override def apply(v: Vertex, db: Database, graph: Graph, authContext: AuthContext): Try[JsObject] = f(v, db, graph, authContext)
    }

  def unapply(updater: PropertyUpdater): Option[(FPath, Any, (Vertex, Database, Graph, AuthContext) => Try[JsObject])] =
    Some((updater.path, updater.value, updater.apply))
}

abstract class PropertyUpdater(val path: FPath, val value: Any) extends ((Vertex, Database, Graph, AuthContext) => Try[JsObject]) {
  override def toString(): String =
    s"PropertyUpdater($path, $value)"
}

class PublicProperties(private val map: Map[String, Seq[PublicProperty[_, _]]]) {
  def get[P, U](propertyName: String): Option[PublicProperty[P, U]] =
    map.get(propertyName).flatMap(_.headOption).asInstanceOf[Option[PublicProperty[P, U]]]
  def get[P, U](propertyName: String, tpe: ru.Type): Option[PublicProperty[P, U]] =
    map.get(propertyName).flatMap(_.find(_.isApplicable(tpe))).asInstanceOf[Option[PublicProperty[P, U]]]
  def get[P, U](propertyPath: FPath, tpe: ru.Type): Option[PublicProperty[P, U]] =
    propertyPath.headOption.flatMap(get(_, tpe))
  def list: Seq[PublicProperty[_, _]] = map.flatMap(_._2.headOption).toSeq
  def :+[P, U](property: PublicProperty[P, U]): PublicProperties =
    new PublicProperties(map + (property.propertyName -> (map.getOrElse(property.propertyName, Nil) :+ property)))
  def ++(properties: PublicProperties): PublicProperties = {
    val newMap = (map.keySet ++ properties.map.keySet).map(k => k -> (map.getOrElse(k, Nil) ++ properties.map.getOrElse(k, Nil))).toMap
    new PublicProperties(newMap)
  }
}

object PublicProperties {
  def empty: PublicProperties = new PublicProperties(Map.empty)
}
