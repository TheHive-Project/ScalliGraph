package org.thp.scalligraph.query

import scala.reflect.runtime.{universe => ru}
import scala.util.{Success, Try}

import play.api.libs.json.{JsObject, Json}

import gremlin.scala.{Graph, Vertex}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.steps.{BaseVertexSteps, Traversal}

class PropertyBuilder[S <: BaseVertexSteps, D, SD, G](stepType: ru.Type, propertyName: String, mapping: Mapping[D, SD, G]) {

  def simple =
    new SimpleUpdatePropertyBuilder[D, SD, G](
      stepType,
      propertyName,
      propertyName,
      mapping,
      Seq((_: BaseVertexSteps).property(propertyName, mapping)) // FIXME doesn't work with optional properties ?
    )

  def rename(newName: String) =
    new SimpleUpdatePropertyBuilder[D, SD, G](
      stepType,
      propertyName,
      newName,
      mapping,
      Seq((_: BaseVertexSteps).property(newName, mapping)) // FIXME doesn't work with optional properties ?
    )

  def derived(definition: (S => Traversal[SD, G])*) =
    new UpdatePropertyBuilder[D, SD, G](stepType, propertyName, mapping, definition.asInstanceOf[Seq[BaseVertexSteps => Traversal[SD, G]]])
}

class SimpleUpdatePropertyBuilder[D, SD, G](
    stepType: ru.Type,
    propertyName: String,
    fieldName: String,
    val mapping: Mapping[D, SD, G],
    definition: Seq[BaseVertexSteps => Traversal[SD, G]]
) extends UpdatePropertyBuilder[D, SD, G](stepType, propertyName, mapping, definition) {

  def updatable(implicit fieldsParser: FieldsParser[SD], updateFieldsParser: FieldsParser[D]): PublicProperty[SD, G] =
    new PublicProperty[SD, G](
      stepType,
      propertyName,
      mapping,
      definition,
      fieldsParser,
      Some(PropertyUpdater(updateFieldsParser, propertyName) { (_: FPath, value: D, vertex: Vertex, db: Database, _: Graph, _: AuthContext) =>
        db.setProperty(vertex, fieldName, value, mapping)
        Success(Json.obj(fieldName -> value.toString))
      })
    )
}

class UpdatePropertyBuilder[D, SD, G](
    stepType: ru.Type,
    propertyName: String,
    mapping: Mapping[D, SD, G],
    definition: Seq[BaseVertexSteps => Traversal[SD, G]]
) {

  def readonly(implicit fieldsParser: FieldsParser[SD]): PublicProperty[SD, G] =
    new PublicProperty[SD, G](
      stepType,
      propertyName,
      mapping,
      definition,
      fieldsParser,
      None
    )

  def custom(
      f: (FPath, D, Vertex, Database, Graph, AuthContext) => Try[JsObject]
  )(implicit fieldsParser: FieldsParser[SD], updateFieldsParser: FieldsParser[D]): PublicProperty[SD, G] =
    new PublicProperty[SD, G](
      stepType,
      propertyName,
      mapping,
      definition,
      fieldsParser,
      Some(PropertyUpdater(updateFieldsParser, propertyName)(f))
    )
}

class PublicPropertyListBuilder[S <: BaseVertexSteps: ru.TypeTag](properties: List[PublicProperty[_, _]]) {
  def build: List[PublicProperty[_, _]] = properties

  def property[D, SD, G](
      name: String,
      mapping: Mapping[D, SD, G]
  )(prop: PropertyBuilder[S, D, SD, G] => PublicProperty[SD, G]): PublicPropertyListBuilder[S] =
    new PublicPropertyListBuilder(
      prop(new PropertyBuilder(ru.typeOf[S], name, mapping)) :: properties
    )
}

object PublicPropertyListBuilder {
  def apply[S <: BaseVertexSteps: ru.TypeTag] = new PublicPropertyListBuilder[S](Nil)
}
