package org.thp.scalligraph.query

import gremlin.scala.{Graph, Vertex}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.Traversal
import play.api.libs.json.{JsObject, Json}

import scala.reflect.runtime.{universe => ru}
import scala.util.{Success, Try}

class PropertyBuilder[E <: Product, P, U](traversalType: ru.Type, propertyName: String, mapping: Mapping[P, U, Any]) {

  def field =
    new SimpleUpdatePropertyBuilder[P, U](
      traversalType,
      propertyName,
      propertyName,
      mapping,
      Seq((_, t) => t.asInstanceOf[Traversal.V[E]].property(propertyName, mapping))
    )

  def rename(newName: String) =
    new SimpleUpdatePropertyBuilder[P, U](
      traversalType,
      propertyName,
      newName,
      mapping,
      Seq((_, t) => t.asInstanceOf[Traversal.V[E]].property(newName, mapping))
    )

  def select(definition: (Traversal.V[E] => Traversal.Domain[U])*) =
    new UpdatePropertyBuilder[P, U](
      traversalType,
      propertyName,
      mapping,
      definition.map(d => (_: FPath, t: Traversal.ANY) => d(t.asInstanceOf[Traversal.V[E]]))
    )

  def subSelect[D](definition: ((FPath, Traversal.V[E]) => Traversal.Domain[U])*) =
    new UpdatePropertyBuilder[P, U](
      traversalType,
      propertyName,
      mapping,
      definition.asInstanceOf[Seq[(FPath, Traversal.ANY) => Traversal.Domain[U]]]
    )
}

class SimpleUpdatePropertyBuilder[P, U](
    traversalType: ru.Type,
    propertyName: String,
    fieldName: String,
    mapping: Mapping[P, U, _],
    definition: Seq[(FPath, Traversal.ANY) => Traversal.Domain[U]]
) extends UpdatePropertyBuilder[P, U](traversalType, propertyName, mapping, definition) {

  def updatable(implicit fieldsParser: FieldsParser[U], updateFieldsParser: FieldsParser[P]): PublicProperty[P, U] =
    new PublicProperty[P, U](
      traversalType,
      propertyName,
      mapping,
      definition,
      fieldsParser,
      Some(PropertyUpdater(updateFieldsParser, propertyName) { (_: FPath, value: P, vertex: Vertex, db: Database, _: Graph, _: AuthContext) =>
        mapping.setProperty(vertex, fieldName, value)
        Success(Json.obj(fieldName -> value.toString))
      })
    )
}

class UpdatePropertyBuilder[P, U](
    traversalType: ru.Type,
    propertyName: String,
    mapping: Mapping[P, U, _],
    definition: Seq[(FPath, Traversal.ANY) => Traversal.Domain[U]]
) {

  def readonly(implicit fieldsParser: FieldsParser[U]): PublicProperty[P, U] =
    new PublicProperty[P, U](
      traversalType,
      propertyName,
      mapping,
      definition,
      fieldsParser,
      None
    )

  def custom(
      f: (FPath, P, Vertex, Database, Graph, AuthContext) => Try[JsObject]
  )(implicit fieldsParser: FieldsParser[U], updateFieldsParser: FieldsParser[P]): PublicProperty[P, U] =
    new PublicProperty[P, U](
      traversalType,
      propertyName,
      mapping,
      definition,
      fieldsParser,
      Some(PropertyUpdater(updateFieldsParser, propertyName)(f))
    )
}

class PublicPropertyListBuilder[E <: Product: ru.TypeTag](properties: List[PublicProperty[_, _]]) {
  def build: List[PublicProperty[_, _]] = properties

  def property[P, U](
      name: String,
      mapping: Mapping[P, U, _]
  )(prop: PropertyBuilder[E, P, U] => PublicProperty[P, U]): PublicPropertyListBuilder[E] =
    new PublicPropertyListBuilder[E](
      prop(new PropertyBuilder[E, P, U](ru.typeOf[Traversal.V[E]], name, mapping.asInstanceOf[Mapping[P, U, Any]])) :: properties
    )
}

object PublicPropertyListBuilder {
  def apply[E <: Product: ru.TypeTag] = new PublicPropertyListBuilder[E](Nil)
}
