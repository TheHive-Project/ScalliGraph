package org.thp.scalligraph.query

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{IndexType, Mapping, Model}
import org.thp.scalligraph.traversal.{Graph, Traversal}
import play.api.libs.json.{JsObject, Json}

import scala.reflect.runtime.{universe => ru}
import scala.util.{Success, Try}

class PropertyBuilder[E <: Product, M, D](propertyOwner: PropertyOwner, propertyName: String, mapping: Mapping[M, D, Traversal.UnkG]) {

  def field(implicit fieldsParser: FieldsParser[D]): FieldBasedProperty =
    propertyOwner.mappingOf(propertyName) match {
      case None => throw InternalError(s"The model ${propertyOwner.name} doesn't contain property $propertyName")
      case Some(m) if !m.isCompatibleWith(mapping) =>
        throw InternalError(
          s"The type of the property $propertyName in the model ${propertyOwner.name} " +
            s"is not compatible with the database definition (${m.graphTypeClass}/${m.cardinality}, ${mapping.graphTypeClass}/${mapping.cardinality})"
        )
      case _ =>
        new FieldBasedProperty(
          propertyOwner,
          propertyName,
          mapping,
          propertyName,
          new FieldPropertyFilter[E, D](propertyName, mapping),
          propertyOwner.indexType(propertyName)
        )
    }

  def rename(newName: String)(implicit fieldsParser: FieldsParser[D]): FieldBasedProperty =
    propertyOwner.mappingOf(newName) match {
      case None => throw InternalError(s"The model ${propertyOwner.name} doesn't contain property $newName ($propertyName)")
      case Some(m) if !m.isCompatibleWith(mapping) =>
        throw InternalError(
          s"The type of the property $newName ($propertyName) in the model ${propertyOwner.name} " +
            s"is not compatible with the database definition (${m.graphTypeClass}/${m.cardinality}, ${mapping.graphTypeClass}/${mapping.cardinality})"
        )
      case _ =>
        new FieldBasedProperty(
          propertyOwner,
          propertyName,
          mapping,
          newName,
          new FieldPropertyFilter[E, D](newName, mapping),
          propertyOwner.indexType(newName)
        )
    }

  def select(definition: Traversal.V[E] => Traversal[D, _, _])(implicit fieldsParser: FieldsParser[D]): TraversalBasedProperty = {
    val select: TraversalSelect = (_: FPath, t: Traversal.Unk, _: AuthContext) =>
      definition(t.asInstanceOf[Traversal.V[E]]).asInstanceOf[Traversal.Unk]
    new TraversalBasedProperty(
      propertyOwner,
      propertyName,
      mapping,
      select,
      new TraversalPropertyFilter[D](select, mapping)
    )
  }

  def authSelect(definition: (Traversal.V[E], AuthContext) => Traversal[D, _, _])(implicit fieldsParser: FieldsParser[D]): TraversalBasedProperty = {
    val select: TraversalSelect = (_: FPath, t: Traversal.Unk, a: AuthContext) =>
      definition(t.asInstanceOf[Traversal.V[E]], a).asInstanceOf[Traversal.Unk]
    new TraversalBasedProperty(
      propertyOwner,
      propertyName,
      mapping,
      select,
      new TraversalPropertyFilter[D](select, mapping)
    )
  }

  def subSelect(definition: (FPath, Traversal.V[E]) => Traversal[D, _, _])(implicit fieldsParser: FieldsParser[D]): TraversalBasedProperty = {
    val select: TraversalSelect = (p: FPath, t: Traversal.Unk, _: AuthContext) =>
      definition(p, t.asInstanceOf[Traversal.V[E]]).asInstanceOf[Traversal.Unk]
    new TraversalBasedProperty(
      propertyOwner,
      propertyName,
      mapping,
      select,
      new TraversalPropertyFilter[D](select, mapping)
    )
  }

  class FieldBasedProperty(
      propertyOwner: PropertyOwner,
      propertyName: String,
      mapping: Mapping[M, D, _],
      fieldName: String,
      filter: PropertyFilter[D],
      indexType: IndexType
  ) {
    def readonly: PublicProperty =
      PublicProperty(propertyOwner, propertyName, mapping, new FieldSelect(fieldName), None, filter, new FieldPropertyOrder(fieldName), indexType)
    def updatable(implicit updateFieldsParser: FieldsParser[M]): PublicProperty =
      PublicProperty(
        propertyOwner,
        propertyName,
        mapping,
        new FieldSelect(fieldName),
        Some(PropertyUpdater(updateFieldsParser, propertyName) { (_: FPath, value: M, vertex: Vertex, _: Graph, _: AuthContext) =>
          mapping.setProperty(vertex, fieldName, value)
          Success(Json.obj(fieldName -> mapping.getRenderer.toJson(value)))
        }),
        filter,
        new FieldPropertyOrder(fieldName),
        indexType
      )
    def custom(
        f: (FPath, M, Vertex, Graph, AuthContext) => Try[JsObject]
    )(implicit updateFieldsParser: FieldsParser[M]): PublicProperty =
      PublicProperty(
        propertyOwner,
        propertyName,
        mapping,
        new FieldSelect(fieldName),
        Some(PropertyUpdater(updateFieldsParser, propertyName)(f)),
        filter,
        new FieldPropertyOrder(fieldName),
        indexType
      )
  }

  class TraversalBasedProperty(
      propertyOwner: PropertyOwner,
      propertyName: String,
      mapping: Mapping[M, D, _],
      select: TraversalSelect,
      filter: PropertyFilter[_],
      indexType: IndexType = IndexType.none
  ) {
    def filter[A](
        newIndexType: IndexType
    )(f: (FPath, Traversal.V[E], AuthContext, Either[Boolean, P[A]]) => Traversal.V[E])(implicit fp: FieldsParser[A]) =
      new TraversalBasedProperty(
        propertyOwner,
        propertyName,
        mapping,
        select,
        new PropertyFilter[A] {
          override val fieldsParser: FieldsParser[A] = fp
          override def apply(path: FPath, traversal: Traversal.Unk, authContext: AuthContext, predicate: P[_]): Traversal.Unk =
            f(
              path,
              traversal.asInstanceOf[Traversal.V[E]],
              authContext,
              Right(predicate.asInstanceOf[P[A]])
            ).asInstanceOf[Traversal.Unk]

          override def existenceTest(path: FPath, traversal: Traversal.Unk, authContext: AuthContext, exist: Boolean): Traversal.Unk =
            f(path, traversal.asInstanceOf[Traversal.V[E]], authContext, Left(exist)).asInstanceOf[Traversal.Unk]

        },
        newIndexType
      )

    def readonly: PublicProperty =
      PublicProperty(propertyOwner, propertyName, mapping, select, None, filter, new TraversalPropertyOrder(select), indexType)
    def custom(
        f: (FPath, M, Vertex, Graph, AuthContext) => Try[JsObject]
    )(implicit updateFieldsParser: FieldsParser[M]): PublicProperty =
      PublicProperty(
        propertyOwner,
        propertyName,
        mapping,
        select,
        Some(PropertyUpdater(updateFieldsParser, propertyName)(f)),
        filter,
        new TraversalPropertyOrder(select),
        indexType
      )
  }
}

class PublicPropertyListBuilder[E <: Product](propertyOwner: PropertyOwner, properties: PublicProperties) {
  def build: PublicProperties = properties

  def property[M, D](
      name: String,
      mapping: Mapping[M, D, _]
  )(prop: PropertyBuilder[E, M, D] => PublicProperty): PublicPropertyListBuilder[E] =
    new PublicPropertyListBuilder[E](
      propertyOwner,
      properties :+ prop(new PropertyBuilder[E, M, D](propertyOwner, name, mapping.asInstanceOf[Mapping[M, D, Traversal.UnkG]]))
    )
}

object PublicPropertyListBuilder {
  def apply[E <: Product: ru.TypeTag](implicit model: Model.Vertex[E]): PublicPropertyListBuilder[E] =
    new PublicPropertyListBuilder[E](new VertexPropertyOwner[E], PublicProperties.empty)

  def metadata: PublicPropertyListBuilder[Product] =
    new PublicPropertyListBuilder[Product](new MetadataPropertyOwner, PublicProperties.empty)
}
