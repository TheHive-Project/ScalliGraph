package org.thp.scalligraph.query

import gremlin.scala.{Graph, Vertex}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{BaseVertexSteps, Traversal}
import play.api.libs.json.{JsObject, Json}

import scala.reflect.runtime.{universe => ru}
import scala.util.{Success, Try}

class PropertyBuilder[S <: BaseVertexSteps, D, SD, G](stepType: ru.Type, propertyName: String, mapping: Mapping[D, SD, G], noValue: NoValue[G]) {

  def field =
    new SimpleUpdatePropertyBuilder[D, SD, G](
      stepType,
      propertyName,
      propertyName,
      mapping,
      noValue,
      Seq((_, s: BaseVertexSteps) => s.property(propertyName, mapping))
    )

  def rename(newName: String) =
    new SimpleUpdatePropertyBuilder[D, SD, G](
      stepType,
      propertyName,
      newName,
      mapping,
      noValue,
      Seq((_, s: BaseVertexSteps) => s.property(newName, mapping))
    )

  def select(definition: (S => Traversal[SD, G])*) =
    new UpdatePropertyBuilder[D, SD, G](
      stepType,
      propertyName,
      mapping,
      noValue,
      definition.map(d => (_: FPath, s: BaseVertexSteps) => d(s.asInstanceOf[S]))
    )

  def subSelect(definition: ((FPath, S) => Traversal[SD, G])*) =
    new UpdatePropertyBuilder[D, SD, G](
      stepType,
      propertyName,
      mapping,
      noValue,
      definition.asInstanceOf[Seq[(FPath, BaseVertexSteps) => Traversal[SD, G]]]
    )
}

class SimpleUpdatePropertyBuilder[D, SD, G](
    stepType: ru.Type,
    propertyName: String,
    fieldName: String,
    mapping: Mapping[D, SD, G],
    noValue: NoValue[G],
    definition: Seq[(FPath, BaseVertexSteps) => Traversal[SD, G]]
) extends UpdatePropertyBuilder[D, SD, G](stepType, propertyName, mapping, noValue, definition) {

  def updatable(implicit fieldsParser: FieldsParser[SD], updateFieldsParser: FieldsParser[D]): PublicProperty[SD, G] =
    new PublicProperty[SD, G](
      stepType,
      propertyName,
      mapping,
      noValue,
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
    noValue: NoValue[G],
    definition: Seq[(FPath, BaseVertexSteps) => Traversal[SD, G]]
) {

  def readonly(implicit fieldsParser: FieldsParser[SD]): PublicProperty[SD, G] =
    new PublicProperty[SD, G](
      stepType,
      propertyName,
      mapping,
      noValue,
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
      noValue,
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
  )(prop: PropertyBuilder[S, D, SD, G] => PublicProperty[SD, G])(implicit noValue: NoValue[G]): PublicPropertyListBuilder[S] =
    new PublicPropertyListBuilder(
      prop(new PropertyBuilder(ru.typeOf[S], name, mapping, noValue)) :: properties
    )
}

object PublicPropertyListBuilder {
  def apply[S <: BaseVertexSteps: ru.TypeTag] = new PublicPropertyListBuilder[S](Nil)
}
