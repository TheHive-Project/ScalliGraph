package org.thp.scalligraph.query

import scala.reflect.runtime.{universe ⇒ ru}

import gremlin.scala.{__, GremlinScala, OrderBy}
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.scalactic.Accumulation._
import org.scalactic.{Bad, Good, One}
import org.thp.scalligraph.InvalidFormatAttributeError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FSeq, FString, FieldsParser}
import org.thp.scalligraph.models.ScalliSteps

case class InputSort(fieldOrder: (String, Order)*) extends InputQuery {
  def orderby[A, F, T](f: GremlinScala[F] ⇒ GremlinScala[T], order: Order): OrderBy[A] = new OrderBy[A] {
    override def apply[End](traversal: GraphTraversal[_, End]): GraphTraversal[_, End] =
      traversal.by(f(__[F]).traversal, order)
  }
  override def apply[S <: ScalliSteps[_, _, _]](
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext): S = {
    val orderBys = fieldOrder.flatMap {
      case (fieldName, order) ⇒
        val property = getProperty(publicProperties, stepType, fieldName)
        property.definition
          .map {
            case f: (GremlinScala[a] ⇒ GremlinScala[b]) ⇒
              orderby[Any, a, b](_.coalesce(f, _.constant(property.mapping.noValue.asInstanceOf[b])), order)
          }
    }
    step
      .asInstanceOf[ScalliSteps[_, _, S]]
      .sort(orderBys: _*)
  }
}

object InputSort {
  implicit val fieldsParser: FieldsParser[InputSort] = FieldsParser("sort-f") {
    case (_, FObjOne("_fields", FSeq(f))) ⇒
      f.validatedBy {
          case FObjOne(name, FString(order)) ⇒
            try Good(new InputSort(name → Order.valueOf(order)))
            catch {
              case _: IllegalArgumentException ⇒
                Bad(One(InvalidFormatAttributeError("order", "order", Set("field: 'incr", "field: 'decr", "field: 'shuffle"), FString(order))))
            }
        }
        .map(x ⇒ new InputSort(x.flatMap(_.fieldOrder): _*))
  }
}
