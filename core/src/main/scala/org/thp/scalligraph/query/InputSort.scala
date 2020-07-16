package org.thp.scalligraph.query

import gremlin.scala.{__, GremlinScala, OrderBy, Vertex}
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.scalactic.Accumulation._
import org.scalactic.{Bad, Good, One}
import org.thp.scalligraph.InvalidFormatAttributeError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FPath, FSeq, FString, FieldsParser}
import org.thp.scalligraph.models.{Database, MappingCardinality}
import org.thp.scalligraph.steps.{ByResult, Converter, IdentityConverter, SortBySelector, Traversal}
import org.thp.scalligraph.steps.StepsOps._

import scala.reflect.runtime.{universe => ru}

case class InputSort(fieldOrder: (String, Order)*) extends InputQuery {

  def orderby[A](f: GremlinScala[Vertex] => GremlinScala[_], order: Order): OrderBy[A] = new OrderBy[A] {
    override def apply[End](traversal: GraphTraversal[_, End]): GraphTraversal[_, End] =
      traversal.by(f(__[Vertex]).traversal, order)
  }

  override def apply[D, G](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      traversalType: ru.Type,
      traversal: Traversal[_, _, _],
      authContext: AuthContext
  ): Traversal[D, G, Converter[D, G]] = {
    val orderBys = fieldOrder.map {
      case (fieldName, order) =>
        def compute[PD, PG]: SortBySelector[D, G, Converter[D, G]] => ByResult[G, G, G, IdentityConverter[G]] = {
          val property = PublicProperty.getProperty[PD, PG](publicProperties, traversalType, fieldName)
          if (property.mapping.cardinality == MappingCardinality.single) {
            (_: SortBySelector[D, G, Converter[D, G]]).by(property.get(_, FPath(fieldName)), order)
          } else {
            (_: SortBySelector[D, G, Converter[D, G]])
              .by(
                _.coalesce[PD, PG, Converter[PD, PG]](
                  property.get(_, FPath(fieldName)),
                  _.constant[PG](null.asInstanceOf[PG]).map(property.mapping.converter) // FIXME check if mapping can accept null
                ),
                order
              )
          }
        }
        compute
    }
    traversal.cast[D, G].sort(orderBys: _*)
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
