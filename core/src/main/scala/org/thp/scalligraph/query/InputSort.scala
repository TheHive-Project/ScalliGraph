package org.thp.scalligraph.query

import scala.reflect.runtime.{universe => ru}

import gremlin.scala.{__, GremlinScala, OrderBy, Vertex}
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.scalactic.Accumulation._
import org.scalactic.{Bad, Good, One}
import org.thp.scalligraph.InvalidFormatAttributeError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.{FSeq, FString, FieldsParser}
import org.thp.scalligraph.models.Database
import org.thp.scalligraph.steps.BaseVertexSteps

case class InputSort(fieldOrder: (String, Order)*) extends InputQuery {

  def orderby[A](f: GremlinScala[Vertex] => GremlinScala[_], order: Order): OrderBy[A] = new OrderBy[A] {
    override def apply[End](traversal: GraphTraversal[_, End]): GraphTraversal[_, End] =
      traversal.by(f(__[Vertex]).traversal, order)
  }
  override def apply[S <: BaseVertexSteps](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S = {
    val orderBys = fieldOrder.map {
      case (fieldName, order) =>
        val property = getProperty(publicProperties, stepType, fieldName)

        val noValue: GremlinScala[Vertex] => GremlinScala[property.Graph] =
          (_: GremlinScala[Vertex]).constant(property.noValue().asInstanceOf[property.Graph])
        val orderDefs: Seq[GremlinScala[Vertex] => GremlinScala[property.Graph]] = property
          .definition
          .map(d => (g: GremlinScala[Vertex]) => d(step.newInstance(g).asInstanceOf[S]).raw.asInstanceOf[GremlinScala[property.Graph]]) :+ noValue
        val orderDef = (_: GremlinScala[Vertex]).coalesce(orderDefs: _*)
        orderby(orderDef, order)
    }
    step
      .sort(orderBys: _*)
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
        }
        .map(x => new InputSort(x.flatMap(_.fieldOrder): _*))
  }
}
