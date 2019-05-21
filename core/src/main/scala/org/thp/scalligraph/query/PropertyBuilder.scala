package org.thp.scalligraph.query

import scala.reflect.runtime.{universe ⇒ ru}
import scala.util.{Success, Try}

import gremlin.scala.{Graph, GremlinScala, Vertex}
import org.thp.scalligraph.FPath
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.FieldsParser
import org.thp.scalligraph.models.{BaseVertexSteps, Database, Mapping, UniMapping}

class PropertyBuilder[S <: BaseVertexSteps[_, S]: ru.TypeTag, D](propertyName: String, mapping: UniMapping[D]) {

  def simple[G] = new SimpleUpdatePropertyBuilder[S, D, G](propertyName, propertyName, mapping, Seq((_: GremlinScala[Vertex]).value[G](propertyName)))

  def rename[G](newName: String) =
    new SimpleUpdatePropertyBuilder[S, D, G](propertyName, newName, mapping, Seq((_: GremlinScala[Vertex]).value[G](newName)))
  def derived[G](definition: (GremlinScala[Vertex] ⇒ GremlinScala[G])*) = new UpdatePropertyBuilder[S, D, G](propertyName, mapping, definition)
}

class SimpleUpdatePropertyBuilder[S <: BaseVertexSteps[_, S]: ru.TypeTag, D, G](
    propertyName: String,
    fieldName: String,
    mapping: UniMapping[D],
    definition: Seq[GremlinScala[Vertex] ⇒ GremlinScala[G]]
) extends UpdatePropertyBuilder[S, D, G](propertyName, mapping, definition) {

  def updatable(implicit fieldsParser: FieldsParser[D]): PublicProperty[D, G] =
    new PublicProperty[D, G](
      ru.typeOf[S],
      propertyName,
      mapping.asInstanceOf[Mapping[D, _, G]],
      definition,
      (property: PublicProperty[D, G]) ⇒
        Some(PropertyUpdater(fieldsParser, property) {
          (property: PublicProperty[D, _], _: FPath, value: D, vertex: Vertex, db: Database, _: Graph, _: AuthContext) ⇒
            db.setProperty(vertex, fieldName, value, property.mapping)
            Success(())
        })
    )
}

class UpdatePropertyBuilder[S <: BaseVertexSteps[_, S]: ru.TypeTag, D, G](
    propertyName: String,
    mapping: UniMapping[D],
    definition: Seq[GremlinScala[Vertex] ⇒ GremlinScala[G]]
) {

  def readonly: PublicProperty[D, G] =
    new PublicProperty[D, G](
      ru.typeOf[S],
      propertyName,
      mapping.asInstanceOf[Mapping[D, _, G]],
      definition,
      _ ⇒ None
    )

  def custom[V](
      f: (PublicProperty[D, _], FPath, V, Vertex, Database, Graph, AuthContext) ⇒ Try[Unit]
  )(implicit fieldsParser: FieldsParser[V]): PublicProperty[D, G] =
    new PublicProperty[D, G](
      ru.typeOf[S],
      propertyName,
      mapping.asInstanceOf[Mapping[D, _, G]],
      definition,
      (property: PublicProperty[D, G]) ⇒ Some(PropertyUpdater(fieldsParser, property)(f))
    )
}

class PublicPropertyListBuilder[S <: BaseVertexSteps[_, S]: ru.TypeTag](properties: List[PublicProperty[_, _]]) {
  def build: List[PublicProperty[_, _]] = properties

  def property[D](name: String)(prop: PropertyBuilder[S, D] ⇒ PublicProperty[_, _])(implicit mapping: UniMapping[D]): PublicPropertyListBuilder[S] =
    new PublicPropertyListBuilder(prop(new PropertyBuilder[S, D](name, mapping)) :: properties)
}

object PublicPropertyListBuilder {
  def apply[S <: BaseVertexSteps[_, S]: ru.TypeTag] = new PublicPropertyListBuilder[S](Nil)
}
