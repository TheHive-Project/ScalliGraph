package org.thp.scalligraph.query

import gremlin.scala.{Graph, Vertex}
import org.thp.scalligraph.BadRequestError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping, SingleMapping}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{Converter, Traversal}
import play.api.libs.json.JsObject

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.Try

class PublicProperty[PD, PG](
    val traversalType: ru.Type,
    val propertyName: String,
    val mapping: Mapping[PD, _, PG],
    val noValue: NoValue[PG],
    definition: Seq[(FPath, Traversal[_, _, _]) => Traversal[PD, PG, Converter[PD, PG]]],
    val fieldsParser: FieldsParser[PD],
    val updateFieldsParser: Option[FieldsParser[PropertyUpdater]]
) {
  type Graph  = PG
  type Domain = PD

  lazy val propertyPath: FPath = FPath(propertyName)

  def get[D, G](traversal: Traversal[D, G, Converter[D, G]], path: FPath): Traversal[PD, PG, Converter[PD, PG]] =
    if (definition.lengthCompare(1) == 0)
      definition.head.apply(path, traversal)
    else {
      val subTraversal = definition.asInstanceOf[Seq[(FPath, Traversal[D, G, Converter[D, G]]) => Traversal[PD, PG, Converter[PD, PG]]]]
      traversal
        .coalesce[PD, PG, Converter[PD, PG]](subTraversal.map(t => t(path, _)): _*)
    }
}

object PublicProperty {
  def getProperty[PD, PG](
      properties: Seq[PublicProperty[_, _]],
      traversalType: ru.Type,
      fieldName: String
  ): PublicProperty[PD, PG] = {
    val path = FPath(fieldName)
    properties
      .iterator
      .collectFirst {
        case p if p.traversalType =:= traversalType && path.startsWith(p.propertyPath).isDefined =>
          p.asInstanceOf[PublicProperty[PD, PG]]
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
//  ): PublicProperty[_, _, _] = {
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
