package org.thp.scalligraph.query

import org.apache.tinkerpop.gremlin.structure.{Graph, Vertex}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Traversal}
import play.api.libs.json.{JsObject, Json}

import scala.reflect.runtime.{universe => ru}
import scala.util.{Success, Try}

class PropertyBuilder[E <: Product, P, U](isApplicableFn: ru.Type => Boolean, propertyName: String, mapping: Mapping[P, U, Traversal.UnkG]) {

  def field =
    new SimpleUpdatePropertyBuilder[E, P, U](
      isApplicableFn,
      propertyName,
      propertyName,
      mapping,
      (_, t, _) => t.asInstanceOf[Traversal.V[E]].property(propertyName, mapping)
    )

  def rename(newName: String) =
    new SimpleUpdatePropertyBuilder[E, P, U](
      isApplicableFn,
      propertyName,
      newName,
      mapping,
      (_, t, _) => t.asInstanceOf[Traversal.V[E]].property(newName, mapping)
    )

  def select(definition: Traversal.V[E] => Traversal[U, _, _]) =
    new UpdatePropertyBuilder[E, P, U](
      isApplicableFn,
      propertyName,
      mapping,
      (_, t, _) => definition(t.asInstanceOf[Traversal.V[E]]).asInstanceOf[Traversal.Domain[U]],
      (_, t, _) => definition(t.asInstanceOf[Traversal.V[E]]).asInstanceOf[Traversal.Domain[U]],
      _ => mapping.reverse.asInstanceOf[Converter[Traversal.UnkG, Traversal.UnkD]]
    )

  def authSelect(definition: (Traversal.V[E], AuthContext) => Traversal[U, _, _]) =
    new UpdatePropertyBuilder[E, P, U](
      isApplicableFn,
      propertyName,
      mapping,
      (_, t, a) => definition(t.asInstanceOf[Traversal.V[E]], a).asInstanceOf[Traversal.Domain[U]],
      (_, t, a) => definition(t.asInstanceOf[Traversal.V[E]], a).asInstanceOf[Traversal.Domain[U]],
      _ => mapping.reverse.asInstanceOf[Converter[Traversal.UnkG, Traversal.UnkD]]
    )

  def subSelect[D](definition: (FPath, Traversal.V[E]) => Traversal[U, _, _]) =
    new UpdatePropertyBuilder[E, P, U](
      isApplicableFn,
      propertyName,
      mapping,
      (p, t, _) => definition(p, t.asInstanceOf[Traversal.V[E]]).asInstanceOf[Traversal.Domain[U]],
      (p, t, _) => definition(p, t.asInstanceOf[Traversal.V[E]]).asInstanceOf[Traversal.Unk],
      _ => mapping.reverse.asInstanceOf[Converter[Traversal.UnkG, Traversal.UnkD]]
    )
}

class SimpleUpdatePropertyBuilder[E <: Product, P, U](
    isApplicableFn: ru.Type => Boolean,
    propertyName: String,
    fieldName: String,
    mapping: Mapping[P, U, _],
    definition: (FPath, Traversal.Unk, AuthContext) => Traversal.Domain[U]
) extends UpdatePropertyBuilder[E, P, U](
      isApplicableFn,
      propertyName,
      mapping,
      definition,
      definition,
      _ => mapping.reverse.asInstanceOf[Converter[Traversal.UnkG, Traversal.UnkD]]
    ) {

  def updatable(implicit fieldsParser: FieldsParser[U], updateFieldsParser: FieldsParser[P]): PublicProperty[P, U] =
    new PublicProperty[P, U](
      isApplicableFn,
      propertyName,
      mapping,
      definition,
      fieldsParser,
      Some(PropertyUpdater(updateFieldsParser, propertyName) { (_: FPath, value: P, vertex: Vertex, _: Database, _: Graph, _: AuthContext) =>
        mapping.setProperty(vertex, fieldName, value)
        Success(Json.obj(fieldName -> value.toString))
      }),
      filterSelect,
      filterConverter
    )
}

class PropertyFilter[E <: Product, P, U](
    updatePropertyBuilder: UpdatePropertyBuilder[E, P, U],
    select: (FPath, Traversal.Unk, AuthContext) => Traversal.Unk
) {
  def converter(c: FPath => Converter[Any, U]) =
    new UpdatePropertyBuilder[E, P, U](
      updatePropertyBuilder.isApplicableFn,
      updatePropertyBuilder.propertyName,
      updatePropertyBuilder.mapping,
      updatePropertyBuilder.definition,
      select,
      c.asInstanceOf[FPath => Converter[Traversal.UnkG, Traversal.UnkD]]
    )
}

class UpdatePropertyBuilder[E <: Product, P, U](
    private[query] val isApplicableFn: ru.Type => Boolean,
    private[query] val propertyName: String,
    private[query] val mapping: Mapping[P, U, _],
    private[query] val definition: (FPath, Traversal.Unk, AuthContext) => Traversal.Domain[U],
    private[query] val filterSelect: (FPath, Traversal.Unk, AuthContext) => Traversal.Unk,
    private[query] val filterConverter: FPath => Converter[Traversal.UnkG, Traversal.UnkD]
) {
  def filter(select: (FPath, Traversal.V[E]) => Traversal[_, _, _]) =
    new PropertyFilter[E, P, U](
      this,
      (path: FPath, t: Traversal.Unk, _: AuthContext) => select(path, t.asInstanceOf[Traversal.V[E]]).asInstanceOf[Traversal.Unk]
    )

  def authFilter(select: (FPath, Traversal.V[E], AuthContext) => Traversal[_, _, _]) =
    new PropertyFilter[E, P, U](this, select.asInstanceOf[(FPath, Traversal.Unk, AuthContext) => Traversal.Unk])

  def readonly(implicit fieldsParser: FieldsParser[U]): PublicProperty[P, U] =
    new PublicProperty[P, U](
      isApplicableFn,
      propertyName,
      mapping,
      definition,
      fieldsParser,
      None,
      filterSelect,
      filterConverter
    )

  def custom(
      f: (FPath, P, Vertex, Database, Graph, AuthContext) => Try[JsObject]
  )(implicit fieldsParser: FieldsParser[U], updateFieldsParser: FieldsParser[P]): PublicProperty[P, U] =
    new PublicProperty[P, U](
      isApplicableFn,
      propertyName,
      mapping,
      definition,
      fieldsParser,
      Some(PropertyUpdater(updateFieldsParser, propertyName)(f)),
      filterSelect,
      filterConverter
    )
}

class PublicPropertyListBuilder[E <: Product](isApplicableFn: ru.Type => Boolean, properties: PublicProperties) {
  def build: PublicProperties = properties

  def property[P, U](
      name: String,
      mapping: Mapping[P, U, _]
  )(prop: PropertyBuilder[E, P, U] => PublicProperty[P, U]): PublicPropertyListBuilder[E] =
    new PublicPropertyListBuilder[E](
      isApplicableFn,
      properties :+ prop(new PropertyBuilder[E, P, U](isApplicableFn, name, mapping.asInstanceOf[Mapping[P, U, Traversal.UnkG]]))
    )
}

object PublicPropertyListBuilder {
  class IsApplicable(tpe: ru.Type) extends (ru.Type => Boolean) {
    override def apply(t: ru.Type): Boolean = t <:< tpe
  }
  def apply[E <: Product: ru.TypeTag]: PublicPropertyListBuilder[E] = forType(new IsApplicable(ru.typeOf[Traversal.V[E]]))
  def forType[E <: Product](isApplicable: ru.Type => Boolean): PublicPropertyListBuilder[E] =
    new PublicPropertyListBuilder[E](isApplicable, PublicProperties.empty)
}
