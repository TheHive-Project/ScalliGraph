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
import org.thp.scalligraph.steps.{UntypedSortBySelector, UntypedTraversal}
import org.thp.scalligraph.steps.StepsOps._

import scala.reflect.runtime.{universe => ru}

case class InputSort(fieldOrder: (String, Order)*) extends InputQuery {

  def orderby[A](f: GremlinScala[Vertex] => GremlinScala[_], order: Order): OrderBy[A] = new OrderBy[A] {
    override def apply[End](traversal: GraphTraversal[_, End]): GraphTraversal[_, End] =
      traversal.by(f(__[Vertex]).traversal, order)
  }
  override def apply(
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: UntypedTraversal,
      authContext: AuthContext
  ): UntypedTraversal = {
    val orderBys = fieldOrder.map {
      case (fieldName, order) =>
        val property = PublicProperty.getProperty(publicProperties, stepType, fieldName)
        if (property.mapping.cardinality == MappingCardinality.single) {
          (_: UntypedSortBySelector).by(property.get(_, FPath(fieldName)).untyped, order)
        } else {
          (_: UntypedSortBySelector).by(_.coalesce(property.get(_, FPath(fieldName)).untyped, _.constant(property.noValue())), order)
        }
    }
    step.sort(orderBys: _*)
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
