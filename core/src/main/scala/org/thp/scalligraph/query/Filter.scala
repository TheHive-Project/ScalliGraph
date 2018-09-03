package org.thp.scalligraph.query

import java.util.function.BiPredicate

import gremlin.scala.{Element, GremlinScala, Key, P}
import org.apache.tinkerpop.gremlin.process.traversal.{P ⇒ JavaP}
import org.scalactic.Accumulation._
import org.scalactic.Good
import org.thp.scalligraph.controllers.{FSeq, FieldsParser}
import org.thp.scalligraph.models.{ElementSteps, VertexOrEdge}
import scala.reflect.runtime.{universe ⇒ ru}

import org.thp.scalligraph.BadRequestError

abstract class Filter[E <: Element] extends (GremlinScala[E] ⇒ GremlinScala[E]) { thisFilter ⇒
  def compose[T](q: InitQuery[T]): InitQuery[T] =
    if (q.toType <:< ru.typeOf[ElementSteps[_, _, _]]) new InitQuery[T](s"${q.name}_filter", q.toType) {
      override def apply(g: AuthGraph): T = q(g).asInstanceOf[ElementSteps[_, E, _]].filter(thisFilter).asInstanceOf[T]
    } else throw BadRequestError(s"Invalid result type for ${q.name}. Expected ElementSteps, found ${q.toType}")
}

object Filter {
  def apply[E <: Element](f: GremlinScala[E] ⇒ GremlinScala[E]): Filter[E] = (t: GremlinScala[E]) ⇒ f(t)

  def is[E <: Element](field: String, value: Any): PredicateFilter[E] = PredicateFilter[E](field, P.is(value))
  //case class Like(field: String, value: String) extends QueryOnField(field, _.where(_.map(x ⇒ x.value[String](field)).filterOnEnd(_.contains(value))))
  def neq[E <: Element](field: String, value: Any): PredicateFilter[E] = PredicateFilter[E](field, P.neq(value))

  def lt[E <: Element](field: String, value: Any): PredicateFilter[E] = PredicateFilter[E](field, P.lt(value))

  def gt[E <: Element](field: String, value: Any): PredicateFilter[E] = PredicateFilter[E](field, P.gt(value))

  def lte[E <: Element](field: String, value: Any): PredicateFilter[E] = PredicateFilter[E](field, P.lte(value))

  def gte[E <: Element](field: String, value: Any): PredicateFilter[E] = PredicateFilter[E](field, P.gte(value))

  def between[E <: Element](field: String, from: Any, to: Any): PredicateFilter[E] = PredicateFilter[E](field, P.between(from, to))

  def inside[E <: Element](field: String, from: Any, to: Any): PredicateFilter[E] = PredicateFilter[E](field, P.inside(from, to))

  def in[E <: Element](field: String, values: Any*): PredicateFilter[E] = PredicateFilter[E](field, P.within(values))

  //def ofType(label: String) = Query(_.hasLabel(label))

  def or[E <: Element](filters: Seq[Filter[E]]): CompositeFilter[E] =
    CompositeFilter[E]("or", filters, f ⇒ _.or(f: _*))

  def and[E <: Element](filters: Seq[Filter[E]]): CompositeFilter[E] =
    CompositeFilter[E]("and", filters, f ⇒ _.and(f: _*))

  def not[E <: Element](filter: Filter[E]): CompositeFilter[E] =
    CompositeFilter[E]("not", Seq(filter), f ⇒ _.not(f.head))

  //def child(childType: String, query: Query) = Query(_.out.hasLabel(childType).where(query))
  //
  //def parent(parentType: String, query: Query) = Query(_.in.hasLabel(parentType).where(query))

  //def withParent(parentType: String, parentId: String) = Query(_.out.hasLabel(parentType).hasId(parentId))

  //  def withId(entityIds: String*): QueryDef = QueryDef(idsQuery(entityIds))
  //  def contains(field: String)

  val fieldsParser: FieldsParser[Filter[VertexOrEdge]] = FieldsParser("query") {
    case (_, FObjOne("_and", FSeq(fields)))                 ⇒ fields.validatedBy(field ⇒ fieldsParser(field)).map(Filter.and)
    case (_, FObjOne("_or", FSeq(fields)))                  ⇒ fields.validatedBy(field ⇒ fieldsParser(field)).map(Filter.or)
    case (_, FObjOne("_not", field))                        ⇒ fieldsParser(field).map(Filter.not)
    case (_, FObjOne("_any", _))                            ⇒ Good(Filter.and(Nil))
    case (_, FObjOne("_lt", FObjOne(key, FNative(value))))  ⇒ Good(Filter.lt(key, value))
    case (_, FObjOne("_gt", FObjOne(key, FNative(value))))  ⇒ Good(Filter.gt(key, value))
    case (_, FObjOne("_lte", FObjOne(key, FNative(value)))) ⇒ Good(Filter.lte(key, value))
    case (_, FObjOne("_gte", FObjOne(key, FNative(value)))) ⇒ Good(Filter.gte(key, value))
    case (_, FObjOne("_is", FObjOne(key, FNative(value))))  ⇒ Good(Filter.is(key, value))
    /*
    case JsObjOne(("_contains", JsString(v)))       ⇒ JsSuccess(contains(v))
    case j: JsObject if j.fields.isEmpty            ⇒ JsSuccess(any)

    case JsObjOne(("_between", JsRange(n, f, t)))   ⇒ JsSuccess(n ~<> (f → t))
    case JsObjOne(("_parent", JsParent(p, q)))      ⇒ JsSuccess(parent(p, q))
    case JsObjOne(("_parent", JsParentId(p, i)))    ⇒ JsSuccess(withParent(p, i))
    case JsObjOne(("_id", JsString(id)))            ⇒ JsSuccess(withId(id))
    case JsField(field, value)                      ⇒ JsSuccess(field ~= value)
    case JsObjOne(("_child", JsParent(p, q)))       ⇒ JsSuccess(child(p, q))
    case JsObjOne(("_string", JsString(s)))         ⇒ JsSuccess(string(s))
    case JsObjOne(("_in", JsFieldIn(f, v)))         ⇒ JsSuccess(f in (v: _*))
    case JsObjOne(("_type", JsString(v)))           ⇒ JsSuccess(ofType(v))
    case JsObjOne(("_like", JsField(field, value))) ⇒ JsSuccess(field like value)
    case JsObjOne((n, JsVal(v))) ⇒
      if (n.startsWith("_")) logger.warn(s"""Potentially invalid search query : {"$n": "$v"}"""); JsSuccess(n ~= v)
case other ⇒ JsError(s"Invalid query: unexpected $other")
     */
//    case (FPath(path), field) ⇒ Bad(One(InvalidFormatAttributeError("path", "filter", field)))
  }
}

case class PredicateFilter[E <: Element](fieldName: String, predicate: P[_]) extends Filter[E] {
  override def apply(raw: GremlinScala[E]): GremlinScala[E] = predicate match {
    case p: P[a] ⇒ raw.has(Key[a](fieldName), p)
  }
}

case class CompositeFilter[E <: Element](name: String, filters: Seq[Filter[E]], fn: Seq[Filter[E]] ⇒ GremlinScala[E] ⇒ GremlinScala[E])
    extends Filter[E] {
  override def apply(t: GremlinScala[E]): GremlinScala[E]           = fn(filters)(t)
  def updateFilters(newFilters: Seq[Filter[E]]): CompositeFilter[E] = CompositeFilter(name, newFilters, fn)
  override def toString(): String                                   = s"$name(${filters.mkString(",")})"
}

case class FilterBuilder[V](fieldName: String, predicate: V ⇒ P[_]) {
  def apply[E <: Element](v: V): Filter[E] = PredicateFilter[E](fieldName, predicate(v))
}

object FilterBuilder {
  def fromPredicate[A](name: String, predicate: (A, A) ⇒ Boolean, value: A): P[A] = {
    val biPredicate = new BiPredicate[A, A] {
      override def test(t: A, u: A): Boolean = predicate(t, u)
      override def toString: String          = name
    }
    new JavaP[A](biPredicate, value)
  }
  def startsWith(value: String): P[String] = fromPredicate[String]("startsWith", _ startsWith _, value) // FIXME between("value", "value" + 1)
  def endsWith(value: String): P[String]   = fromPredicate[String]("endsWith", _ endsWith _, value)
  def contains(value: String): P[String]   = fromPredicate[String]("contains", _ contains _, value)
  def string(fieldName: String): List[FilterBuilder[Any]] =
    List(
      FilterBuilder[String](fieldName, v ⇒ P.is(v)),
      FilterBuilder[String](fieldName, v ⇒ P.neq(v)),
      FilterBuilder[Seq[String]](fieldName, v ⇒ P.within(v)),
      FilterBuilder[Seq[String]](fieldName, v ⇒ P.without(v)),
      FilterBuilder[String](fieldName, v ⇒ P.lt(v)),
      FilterBuilder[String](fieldName, v ⇒ P.lte(v)),
      FilterBuilder[String](fieldName, v ⇒ P.gt(v)),
      FilterBuilder[String](fieldName, v ⇒ P.gte(v)),
      FilterBuilder[String](fieldName, v ⇒ startsWith(v)),
      FilterBuilder[String](fieldName, v ⇒ contains(v)),
      FilterBuilder[String](fieldName, v ⇒ endsWith(v))
    ).asInstanceOf[List[FilterBuilder[Any]]]
}

//class QueryFilter[E <: Element, S <: NodeSteps[_, E]: ru.WeakTypeTag](filter: Filter[E]) extends Query[S, S] {
//  override def apply(s: S): S = s.filter(filter).asInstanceOf[S]
//}
