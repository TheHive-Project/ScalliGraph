package org.thp.scalligraph.query

import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.scalactic.Accumulation._
import org.scalactic.{Bad, Good, One}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FSeq, FString, FieldsParser}
import org.thp.scalligraph.models.{Database, MappingCardinality}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, SortBySelector, Traversal}
import org.thp.scalligraph.{BadRequestError, InvalidFormatAttributeError}

import scala.reflect.runtime.{universe => ru}

case class InputSort(fieldOrder: (String, Order)*) extends InputQuery[Traversal.Unk, Traversal.Unk] {
  override def apply(
      db: Database,
      publicProperties: PublicProperties,
      traversalType: ru.Type,
      traversal: Traversal.Unk,
      authContext: AuthContext
  ): Traversal.Unk =
    fieldOrder.foldLeft(traversal.onRaw(_.order)) {
      case (t, (fieldName, order)) =>
        val fieldPath = FPath(fieldName)
        val property = publicProperties
          .get[Traversal.UnkD, Traversal.UnkDU](fieldPath, traversalType)
          .getOrElse(throw BadRequestError(s"Property $fieldName for type $traversalType not found"))
//        if (property.mapping.cardinality == MappingCardinality.single)
        property.sort(fieldPath, t, authContext, order)
//        ???
    }

//    val orderBys = fieldOrder.map {
//      case (fieldName, order) =>
//        val fieldPath = FPath(fieldName)
//        val property = publicProperties
//          .get[Traversal.UnkD, Traversal.UnkDU](fieldPath, traversalType)
//          .getOrElse(throw BadRequestError(s"Property $fieldName for type $traversalType not found"))
//        if (property.mapping.cardinality == MappingCardinality.single)
//          (_: SortBySelector[Traversal.UnkD, Traversal.UnkG, Converter[Traversal.UnkD, Traversal.UnkG]])
//            .by(property.select(fieldPath, _, authContext), order)
//        else
//          (_: SortBySelector[Traversal.UnkD, Traversal.UnkG, Converter[Traversal.UnkD, Traversal.UnkG]])
//            .by(
//              _.coalesceIdent(
//                property.select(FPath(fieldName), _, authContext),
//                _.constant(property.mapping.noValue.asInstanceOf[Traversal.UnkDU])
//              ),
//              order
//            )
//    }
//    traversal.sort(orderBys: _*)
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
      }.map(x => new InputSort(x.flatMap(_.fieldOrder): _*))
  }
}
