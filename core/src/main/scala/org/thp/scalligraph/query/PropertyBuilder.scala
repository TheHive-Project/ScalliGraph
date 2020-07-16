package org.thp.scalligraph.query

import gremlin.scala.{Graph, Vertex}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FieldsParser}
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{Converter, Traversal}
import play.api.libs.json.{JsObject, Json}

import scala.reflect.runtime.{universe => ru}
import scala.util.{Success, Try}

class PropertyBuilder[PD, PG](traversalType: ru.Type, propertyName: String, mapping: Mapping[PD, _, PG], noValue: NoValue[PG]) {

  def field[D] =
    new SimpleUpdatePropertyBuilder[PD, PG](
      traversalType,
      propertyName,
      propertyName,
      mapping,
      noValue,
      Seq((_, t: Traversal[_, _, _]) => t.asInstanceOf[Traversal[D, Vertex, Converter[D, Vertex]]] property (propertyName, mapping.converter))
    )

  def rename[D](newName: String) =
    new SimpleUpdatePropertyBuilder[PD, PG](
      traversalType,
      propertyName,
      newName,
      mapping,
      noValue,
      Seq((_, t: Traversal[_, _, _]) => t.asInstanceOf[Traversal[D, Vertex, Converter[D, Vertex]]].property(newName, mapping.converter))
    )

  def select[D](definition: (Traversal[D, Vertex, Converter[D, Vertex]] => Traversal[PD, PG, Converter[PD, PG]])*) =
    new UpdatePropertyBuilder[PD, PG](
      traversalType,
      propertyName,
      mapping,
      noValue,
      definition.map(d => (_: FPath, t: Traversal[_, _, _]) => d(t.asInstanceOf[Traversal[D, Vertex, Converter[D, Vertex]]]))
    )

  def subSelect[D](definition: ((FPath, Traversal[D, Vertex, Converter[D, Vertex]]) => Traversal[PD, PG, Converter[PD, PG]])*) =
    new UpdatePropertyBuilder[PD, PG](
      traversalType,
      propertyName,
      mapping,
      noValue,
      definition.asInstanceOf[Seq[(FPath, Traversal[_, _, _]) => Traversal[PD, PG, Converter[PD, PG]]]]
    )
}

class SimpleUpdatePropertyBuilder[PD, PG](
    traversalType: ru.Type,
    propertyName: String,
    fieldName: String,
    mapping: Mapping[PD, _, PG],
    noValue: NoValue[PG],
    definition: Seq[(FPath, Traversal[_, _, _]) => Traversal[PD, PG, Converter[PD, PG]]]
) extends UpdatePropertyBuilder[PD, PG](traversalType, propertyName, mapping, noValue, definition) {

  def updatable(implicit fieldsParser: FieldsParser[PD], updateFieldsParser: FieldsParser[PD]): PublicProperty[PD, PG] =
    new PublicProperty[PD, PG](
      traversalType,
      propertyName,
      mapping,
      noValue,
      definition,
      fieldsParser,
      Some(PropertyUpdater(updateFieldsParser, propertyName) { (_: FPath, value: PD, vertex: Vertex, db: Database, _: Graph, _: AuthContext) =>
        db.setProperty(vertex, fieldName, value, mapping)
        Success(Json.obj(fieldName -> value.toString))
      })
    )
}

class UpdatePropertyBuilder[PD, PG](
    traversalType: ru.Type,
    propertyName: String,
    mapping: Mapping[PD, _, PG],
    noValue: NoValue[PG],
    definition: Seq[(FPath, Traversal[_, _, _]) => Traversal[PD, PG, Converter[PD, PG]]]
) {

  def readonly(implicit fieldsParser: FieldsParser[PD]): PublicProperty[PD, PG] =
    new PublicProperty[PD, PG](
      traversalType,
      propertyName,
      mapping,
      noValue,
      definition,
      fieldsParser,
      None
    )

  def custom(
      f: (FPath, PD, Vertex, Database, Graph, AuthContext) => Try[JsObject]
  )(implicit fieldsParser: FieldsParser[PD], updateFieldsParser: FieldsParser[PD]): PublicProperty[PD, PG] =
    new PublicProperty[PD, PG](
      traversalType,
      propertyName,
      mapping,
      noValue,
      definition,
      fieldsParser,
      Some(PropertyUpdater(updateFieldsParser, propertyName)(f))
    )
}

class PublicPropertyListBuilder[T <: Traversal[_, _, _]: ru.TypeTag](properties: List[PublicProperty[_, _]]) {
  def build: List[PublicProperty[_, _]] = properties

  def property[PD, PG](
      name: String,
      mapping: Mapping[PD, _, PG]
  )(prop: PropertyBuilder[PD, PG] => PublicProperty[PD, PG])(implicit noValue: NoValue[PG]): PublicPropertyListBuilder[T] =
    new PublicPropertyListBuilder[T](
      prop(new PropertyBuilder(ru.typeOf[T], name, mapping, noValue)) :: properties
    )
}

object PublicPropertyListBuilder {
  def apply[D: ru.TypeTag] = new PublicPropertyListBuilder[Traversal[D, Vertex, Converter[D, Vertex]]](Nil)
}
