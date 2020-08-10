package org.thp.scalligraph.query

import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.scalactic.Accumulation._
import org.scalactic.{Bad, Good, One}
import org.thp.scalligraph.InvalidFormatAttributeError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FSeq, FString, FieldsParser}
import org.thp.scalligraph.models.{Database, MappingCardinality}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, SortBySelector, Traversal}

import scala.reflect.runtime.{universe => ru}

case class InputSort(fieldOrder: (String, Order)*) extends InputQuery[Traversal.Unk, Traversal.Unk] {
  override def apply(
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      traversalType: ru.Type,
      traversal: Traversal.Unk,
      authContext: AuthContext
  ): Traversal.Unk = {
    val orderBys = fieldOrder.map {
      case (fieldName, order) =>
        val property = PublicProperty.getProperty(publicProperties, traversalType, fieldName)
        if (property.mapping.cardinality == MappingCardinality.single) {
          (_: SortBySelector[Traversal.UnkD, Traversal.UnkG, Converter[Traversal.UnkD, Traversal.UnkG]])
            .by(property.select(FPath(fieldName), _), order)
        } else {
          (_: SortBySelector[Traversal.UnkD, Traversal.UnkG, Converter[Traversal.UnkD, Traversal.UnkG]])
            .by(
              _.coalesce(
                property.select(FPath(fieldName), _),
                _.constant[Any](property.mapping.noValue).castDomain[Traversal.UnkDU]
              ),
              order
            )
        }
    }
    traversal.sort(orderBys: _*)
  }
}

object InputSort {
  implicit val fieldsParser: FieldsParser[InputSort] = FieldsParser("sort-f") {
    case (_, FObjOne("_fields", FSeq(f))) =>
      f.validatedBy {
          case FObjOne(name, FString(order)) =>
            try Good(new InputSort(name -> Order.valueOf(order)))
            catch {
              case _: IllegalArgumentException =>
                Bad(One(InvalidFormatAttributeError("order", "order", Order.values().map(o => s"field: '$o'").toSet, FString(order))))
            }
          case FString(name) if name(0) == '-' => Good(new InputSort(name -> Order.desc))
          case FString(name) if name(0) == '+' => Good(new InputSort(name -> Order.asc))
          case FString(name)                   => Good(new InputSort(name -> Order.asc))
          case other                           => Bad(One(InvalidFormatAttributeError("order", "order", Order.values.map(o => s"field: '$o'").toSet, other)))
        }
        .map(x => new InputSort(x.flatMap(_.fieldOrder): _*))
  }
}
