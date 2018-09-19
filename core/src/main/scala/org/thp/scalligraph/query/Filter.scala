package org.thp.scalligraph.query

import scala.reflect.runtime.{currentMirror ⇒ rm, universe ⇒ ru}

import gremlin.scala.{By, GremlinScala, P}
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.scalactic.Accumulation._
import org.scalactic.{Bad, Good, One}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.ScalliSteps
import org.thp.scalligraph.{BadRequestError, InvalidFormatAttributeError}

object FObjOne {
  def unapply(field: Field): Option[(String, Field)] = field match {
    case FObject(f) if f.size == 1 ⇒ Some(f.head)
    case _                         ⇒ None
  }
}

case class PublicProperty[S <: ScalliSteps[_, E, S], E](
    stepType: ru.Type,
    fieldName: String,
    typeName: String,
    fn: Option[AuthContext] ⇒ Seq[GremlinScala[E] ⇒ GremlinScala[_]])

object PublicProperty {
  def apply[S <: ScalliSteps[_, E, S]: ru.TypeTag, E](name: String, `type`: String): PublicProperty[S, E] =
    new PublicProperty[S, E](ru.typeOf[S], name, `type`, _ ⇒ Seq(g ⇒ g))

  protected def fromTypeTag[S <: ScalliSteps[_, E, S], E](
      typeTag: ru.TypeTag[S],
      name: String,
      typeName: String,
      fn: Option[AuthContext] ⇒ Seq[GremlinScala[E] ⇒ GremlinScala[_]]): PublicProperty[S, E] =
    new PublicProperty[S, E](typeTag.tpe, name, typeName, fn)

  def apply[S <: ScalliSteps[_, E, S], E](fieldName: String, typeName: String, fn: Option[AuthContext] ⇒ GremlinScala[E] ⇒ GremlinScala[_])(
      implicit tt: ru.TypeTag[S]): PublicProperty[S, E] =
    fromTypeTag(tt, fieldName, typeName, fn.andThen(Seq(_)))

  def seq[S <: ScalliSteps[_, E, S], E](fieldName: String, typeName: String, fn: Option[AuthContext] ⇒ Seq[GremlinScala[E] ⇒ GremlinScala[_]])(
      implicit tt: ru.TypeTag[S]): PublicProperty[S, E] =
    fromTypeTag(tt, fieldName, typeName, fn)
}

trait InputQuery {
  def toQuery[S <: ScalliSteps[_, E, S], E](properties: Seq[PublicProperty[_, _]]): GenericQuery
  def getProperty[S <: ScalliSteps[_, E, S], E](properties: Seq[PublicProperty[_, _]], stepType: ru.Type, fieldName: String): PublicProperty[S, E] =
    properties
      .find(p ⇒ stepType =:= stepType && p.fieldName == fieldName)
      .getOrElse(throw BadRequestError(""))
      .asInstanceOf[PublicProperty[S, E]]
}

abstract class TraversalQuery[E](name: String) extends GenericQuery(name) {
  override def checkFrom(t: ru.Type): Boolean = t <:< ru.typeOf[ScalliSteps[_, _, _]]
  override def toType(t: ru.Type): ru.Type    = t
  def fromTraversal(stepType: ru.Type, t: GremlinScala[E], authContext: Option[AuthContext]): GremlinScala[_]
}

case class InputSort(fieldOrder: (String, Order)*) extends InputQuery {
  def toQuery[S <: ScalliSteps[_, E, S], E](properties: Seq[PublicProperty[_, _]]): TraversalQuery[E] =
    new TraversalQuery[E]("sort") {
      override def apply(step: Any)(implicit authContext: Option[AuthContext]): Any = {
        val stepType = rm.classSymbol(step.getClass).toType
        val orderBys = fieldOrder.flatMap {
          case (fieldName, order) ⇒
            getProperty[S, E](properties, stepType, fieldName)
              .fn(authContext)
              .map(f ⇒ By(f, order))
        }
        step.asInstanceOf[S].sort(orderBys: _*)
      }

      // Should not be used
      override def fromTraversal(stepType: ru.Type, t: GremlinScala[E], authContext: Option[AuthContext]): GremlinScala[_] = {
        val orderBys = fieldOrder.flatMap {
          case (fieldName, order) ⇒
            getProperty[S, E](properties, stepType, fieldName)
              .fn(authContext)
              .map(f ⇒ By(f, order))
        }
        t.order(orderBys: _*)
      }
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
  def toQuery[S <: ScalliSteps[_, E, S], E](properties: Seq[PublicProperty[_, _]]): TraversalQuery[E]
}

case class PredicateFilter(fieldName: String, predicate: P[_]) extends InputFilter {
  override def toQuery[S <: ScalliSteps[_, E, S], E](properties: Seq[PublicProperty[_, _]]): TraversalQuery[E] =
    new TraversalQuery[E]("") {
      override def apply(step: Any)(implicit authContext: Option[AuthContext]): Any = {
        val stepType = rm.classSymbol(step.getClass).toType
        step.asInstanceOf[S].where(t => fromTraversal(stepType, t, authContext))
      }

      override def fromTraversal(stepType: ru.Type, t: GremlinScala[E], authContext: Option[AuthContext]): GremlinScala[_] = {
        val filter: Seq[GremlinScala[E] ⇒ GremlinScala[_]] = getProperty[S, E](properties, stepType, fieldName)
          .fn(authContext)
          .map(_.andThen(_.is(predicate)))
        t.or(filter: _*)
      }
    }
}

case class OrFilter(inputFilters: Seq[InputFilter]) extends InputFilter {
  override def toQuery[S <: ScalliSteps[_, E, S], E](properties: Seq[PublicProperty[_, _]]): TraversalQuery[E] =
    new TraversalQuery[E]("or") {
      override def apply(step: Any)(implicit authContext: Option[AuthContext]): Any = {
        val stepType = rm.classSymbol(step.getClass).toType
        step
          .asInstanceOf[S]
          .where(t ⇒ fromTraversal(stepType, t, authContext))
      }
      override def fromTraversal(stepType: ru.Type, t: GremlinScala[E], authContext: Option[AuthContext]): GremlinScala[_] = {
        val subQueries = inputFilters
          .map(f ⇒ f.toQuery[S, E](properties))
          .map(tq ⇒ tq.fromTraversal(stepType, _: GremlinScala[E], authContext))
        t.or(subQueries: _*)
      }
    }
}

case class AndFilter(inputFilters: Seq[InputFilter]) extends InputFilter {
  override def toQuery[S <: ScalliSteps[_, E, S], E](properties: Seq[PublicProperty[_, _]]): TraversalQuery[E] =
    new TraversalQuery[E]("and") {
      override def apply(step: Any)(implicit authContext: Option[AuthContext]): Any = {
        val stepType = rm.classSymbol(step.getClass).toType
        step
          .asInstanceOf[S]
          .where(t ⇒ fromTraversal(stepType, t, authContext))
      }
      override def fromTraversal(stepType: ru.Type, t: GremlinScala[E], authContext: Option[AuthContext]): GremlinScala[_] = {
        val subQueries = inputFilters
          .map(f ⇒ f.toQuery[S, E](properties))
          .map(tq ⇒ tq.fromTraversal(stepType, _: GremlinScala[E], authContext))
        t.and(subQueries: _*)
      }
    }
}

case class NotFilter(inputFilter: InputFilter) extends InputFilter {
  override def toQuery[S <: ScalliSteps[_, E, S], E](properties: Seq[PublicProperty[_, _]]): TraversalQuery[E] =
    new TraversalQuery[E]("not") {
      override def apply(step: Any)(implicit authContext: Option[AuthContext]): Any = {
        val stepType = rm.classSymbol(step.getClass).toType
        step
          .asInstanceOf[S]
          .where(t ⇒ fromTraversal(stepType, t, authContext))
      }
      override def fromTraversal(stepType: ru.Type, t: GremlinScala[E], authContext: Option[AuthContext]): GremlinScala[_] = {
        val subQuery = inputFilter
          .toQuery[S, E](properties)
          .fromTraversal(stepType, _: GremlinScala[E], authContext)
        t.not(subQuery)
      }
    }
}

object InputFilter {
  def is(field: String, value: Any): PredicateFilter = PredicateFilter(field, P.is(value))
  def neq(field: String, value: Any): PredicateFilter             = PredicateFilter(field, P.neq(value))
  def lt(field: String, value: Any): PredicateFilter              = PredicateFilter(field, P.lt(value))
  def gt(field: String, value: Any): PredicateFilter              = PredicateFilter(field, P.gt(value))
  def lte(field: String, value: Any): PredicateFilter             = PredicateFilter(field, P.lte(value))
  def gte(field: String, value: Any): PredicateFilter             = PredicateFilter(field, P.gte(value))
  def between(field: String, from: Any, to: Any): PredicateFilter = PredicateFilter(field, P.between(from, to))
  def inside(field: String, from: Any, to: Any): PredicateFilter  = PredicateFilter(field, P.inside(from, to))
  def in(field: String, values: Any*): PredicateFilter            = PredicateFilter(field, P.within(values))
  def or(filters: Seq[InputFilter]): OrFilter   = OrFilter(filters)
  def and(filters: Seq[InputFilter]): AndFilter = AndFilter(filters)
  def not(filter: InputFilter): NotFilter       = NotFilter(filter)

  implicit val fieldsParser: FieldsParser[InputFilter] = FieldsParser("query") {
    case (_, FObjOne("_and", FSeq(fields)))                 ⇒ fields.validatedBy(field ⇒ fieldsParser(field)).map(InputFilter.and)
    case (_, FObjOne("_or", FSeq(fields)))                  ⇒ fields.validatedBy(field ⇒ fieldsParser(field)).map(InputFilter.or)
    case (_, FObjOne("_not", field))                        ⇒ fieldsParser(field).map(InputFilter.not)
    case (_, FObjOne("_any", _))                            ⇒ Good(InputFilter.and(Nil))
    case (_, FObjOne("_lt", FObjOne(key, FNative(value))))  ⇒ Good(InputFilter.lt(key, value))
    case (_, FObjOne("_gt", FObjOne(key, FNative(value))))  ⇒ Good(InputFilter.gt(key, value))
    case (_, FObjOne("_lte", FObjOne(key, FNative(value)))) ⇒ Good(InputFilter.lte(key, value))
    case (_, FObjOne("_gte", FObjOne(key, FNative(value)))) ⇒ Good(InputFilter.gte(key, value))
    case (_, FObjOne("_is", FObjOne(key, FNative(value))))  ⇒ Good(InputFilter.is(key, value))
  }
}