package org.thp.scalligraph.query

import scala.reflect.runtime.{universe ⇒ ru}

import gremlin.scala.{__, Element, GremlinScala, OrderBy, P}
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.scalactic.Accumulation._
import org.scalactic.{Bad, Good, One}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.ScalliSteps
import org.thp.scalligraph.{BadRequestError, InvalidFormatAttributeError}

trait InputQuery {
  def apply[S <: ScalliSteps[_, _, _]](
      publicProperties: List[PublicProperty[_ <: Element, _]],
      stepType: ru.Type,
      step: S,
      authContext: Option[AuthContext]): S

  def getProperty(properties: Seq[PublicProperty[_, _]], stepType: ru.Type, fieldName: String): PublicProperty[_, _] =
    properties
      .find(p ⇒ stepType =:= stepType && p.propertyName == fieldName)
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $stepType not found"))
}

case class InputSort(fieldOrder: (String, Order)*) extends InputQuery {
  def orderby[A, F, T](f: GremlinScala[F] ⇒ GremlinScala[T], order: Order): OrderBy[A] = new OrderBy[A] {
    override def apply[End](traversal: GraphTraversal[_, End]): GraphTraversal[_, End] =
      traversal.by(f(__[F]).traversal, order)
  }
  override def apply[S <: ScalliSteps[_, _, _]](
      publicProperties: List[PublicProperty[_ <: Element, _]],
      stepType: ru.Type,
      step: S,
      authContext: Option[AuthContext]): S = {
    val orderBys = fieldOrder.flatMap {
      case (fieldName, order) ⇒
        getProperty(publicProperties, stepType, fieldName)
          .get(authContext)
          .map(f ⇒ orderby(f, order))
    }
    step
      .asInstanceOf[ScalliSteps[_, _, S]]
      .sort(orderBys: _*)
  }
}

object InputSort {
  implicit val fieldsParser: FieldsParser[InputSort] = FieldsParser("sort") {
    case (_, FSeq(fields)) ⇒
      fields
        .validatedBy(fieldsParser.apply)
        .map(x ⇒ new InputSort(x.flatMap(_.fieldOrder): _*))
    case (_, FObjOne(name, FString(order))) ⇒
      try Good(new InputSort(name → Order.valueOf(order)))
      catch {
        case _: IllegalArgumentException ⇒
          Bad(One(InvalidFormatAttributeError("order", "order", Seq("field: 'incr", "field: 'decr", "field: 'shuffle"), FString(order))))
      }
  }
}

trait InputFilter extends InputQuery {
  def apply[S <: ScalliSteps[_, _, _]](
      publicProperties: List[PublicProperty[_ <: Element, _]],
      stepType: ru.Type,
      step: S,
      authContext: Option[AuthContext]): S
}

case class PredicateFilter(fieldName: String, predicate: P[_]) extends InputFilter {
  override def apply[S <: ScalliSteps[_, _, _]](
      publicProperties: List[PublicProperty[_ <: Element, _]],
      stepType: ru.Type,
      step: S,
      authContext: Option[AuthContext]): S = {
    val filter = getProperty(publicProperties, stepType, fieldName)
      .get(authContext)
      .map(f ⇒ f.andThen(_.is(predicate)))
      .asInstanceOf[Seq[GremlinScala[_] ⇒ GremlinScala[_]]]
    step
      .asInstanceOf[ScalliSteps[_, _, S]]
      .where(_.or(filter: _*))
  }
}

case class OrFilter(inputFilters: Seq[InputFilter]) extends InputFilter {
  override def apply[S <: ScalliSteps[_, _, _]](
      publicProperties: List[PublicProperty[_ <: Element, _]],
      stepType: ru.Type,
      step: S,
      authContext: Option[AuthContext]): S =
    step match {
      case s: ScalliSteps[_, gt, S] ⇒
        val filters = inputFilters.map { ff ⇒ (g: GremlinScala[gt]) ⇒
          ff[S](publicProperties, stepType, s.newInstance(g), authContext).raw
        }
        s.asInstanceOf[ScalliSteps[_, gt, S]]
          .where(_.or(filters: _*))
    }
}

case class AndFilter(inputFilters: Seq[InputFilter]) extends InputFilter {
  override def apply[S <: ScalliSteps[_, _, _]](
      publicProperties: List[PublicProperty[_ <: Element, _]],
      stepType: ru.Type,
      step: S,
      authContext: Option[AuthContext]): S =
    step match {
      case s: ScalliSteps[_, gt, S] ⇒
        val filters = inputFilters.map { ff ⇒ (g: GremlinScala[gt]) ⇒
          ff[S](publicProperties, stepType, s.newInstance(g), authContext).raw
        }
        s.asInstanceOf[ScalliSteps[_, gt, S]]
          .where(_.and(filters: _*))
    }
}

case class NotFilter(inputFilter: InputFilter) extends InputFilter {
  override def apply[S <: ScalliSteps[_, _, _]](
      publicProperties: List[PublicProperty[_ <: Element, _]],
      stepType: ru.Type,
      step: S,
      authContext: Option[AuthContext]): S =
    step match {
      case s: ScalliSteps[_, gt, S] ⇒
        val filter = (g: GremlinScala[gt]) ⇒ inputFilter[S](publicProperties, stepType, s.newInstance(g), authContext).raw
        s.asInstanceOf[ScalliSteps[_, gt, S]]
          .where(_.not(filter))
    }
}

object InputFilter {
  def stringContains(value: String): P[String]   = P.fromPredicate[String]((v, r) ⇒ v contains r, value)
  def stringStartsWith(value: String): P[String] = P.fromPredicate[String]((v, r) ⇒ v startsWith r, value)
  def stringEndsWith(value: String): P[String]   = P.fromPredicate[String]((v, r) ⇒ v endsWith r, value)

  def is(field: String, value: Any): PredicateFilter              = PredicateFilter(field, P.is(value))
  def neq(field: String, value: Any): PredicateFilter             = PredicateFilter(field, P.neq(value))
  def lt(field: String, value: Any): PredicateFilter              = PredicateFilter(field, P.lt(value))
  def gt(field: String, value: Any): PredicateFilter              = PredicateFilter(field, P.gt(value))
  def lte(field: String, value: Any): PredicateFilter             = PredicateFilter(field, P.lte(value))
  def gte(field: String, value: Any): PredicateFilter             = PredicateFilter(field, P.gte(value))
  def between(field: String, from: Any, to: Any): PredicateFilter = PredicateFilter(field, P.between(from, to))
  def inside(field: String, from: Any, to: Any): PredicateFilter  = PredicateFilter(field, P.inside(from, to))
  def in(field: String, values: Any*): PredicateFilter            = PredicateFilter(field, P.within(values))
  def contains(field: String, value: String): PredicateFilter     = PredicateFilter(field, stringContains(value))
  def startsWith(field: String, value: String): PredicateFilter   = PredicateFilter(field, stringStartsWith(value))
  def endsWith(field: String, value: String): PredicateFilter     = PredicateFilter(field, stringEndsWith(value))
  def or(filters: Seq[InputFilter]): OrFilter                     = OrFilter(filters)
  def and(filters: Seq[InputFilter]): AndFilter                   = AndFilter(filters)
  def not(filter: InputFilter): NotFilter                         = NotFilter(filter)

  implicit val fieldsParser: FieldsParser[InputFilter] = FieldsParser("query") {
    case (_, FObjOne("_and", FSeq(fields)))                        ⇒ fields.validatedBy(field ⇒ fieldsParser(field)).map(and)
    case (_, FObjOne("_or", FSeq(fields)))                         ⇒ fields.validatedBy(field ⇒ fieldsParser(field)).map(or)
    case (_, FObjOne("_not", field))                               ⇒ fieldsParser(field).map(not)
    case (_, FObjOne("_any", _))                                   ⇒ Good(and(Nil))
    case (_, FObjOne("_lt", FObjOne(key, FNative(value))))         ⇒ Good(lt(key, value))
    case (_, FObjOne("_gt", FObjOne(key, FNative(value))))         ⇒ Good(gt(key, value))
    case (_, FObjOne("_lte", FObjOne(key, FNative(value))))        ⇒ Good(lte(key, value))
    case (_, FObjOne("_gte", FObjOne(key, FNative(value))))        ⇒ Good(gte(key, value))
    case (_, FObjOne("_is", FObjOne(key, FNative(value))))         ⇒ Good(is(key, value))
    case (_, FObjOne("_contains", FObjOne(key, FString(value))))   ⇒ Good(contains(key, value))
    case (_, FObjOne("_startsWith", FObjOne(key, FString(value)))) ⇒ Good(startsWith(key, value))
    case (_, FObjOne("_endsWith", FObjOne(key, FString(value))))   ⇒ Good(endsWith(key, value))
  }
}
