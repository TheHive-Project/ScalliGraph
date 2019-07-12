package org.thp.scalligraph.query

import scala.reflect.runtime.{universe => ru}

import play.api.Logger

import gremlin.scala.{GremlinScala, P, Vertex}
import org.scalactic.Accumulation._
import org.scalactic.Good
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.{BaseVertexSteps, Mapping}

trait InputFilter extends InputQuery {
  def apply[S <: BaseVertexSteps[_, S]](publicProperties: List[PublicProperty[_, _]], stepType: ru.Type, step: S, authContext: AuthContext): S
}

case class PredicateFilter(fieldName: String, predicate: P[_]) extends InputFilter {
  override def apply[S <: BaseVertexSteps[_, S]](
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S =
    getProperty(publicProperties, stepType, fieldName)
      .definition
      .map(f => f.andThen(_.is(predicate)))
      .asInstanceOf[Seq[GremlinScala[_] => GremlinScala[_]]] match {
      case Seq()  => step.filter(_.is(null)) // TODO need checks
      case Seq(f) => step.filter(f)
      case f      => step.filter(_.or(f: _*))
    }
}

case class IsDefinedFilter(fieldName: String) extends InputFilter {
  override def apply[S <: BaseVertexSteps[_, S]](
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S = {
    val prop = getProperty(publicProperties, stepType, fieldName)
    step.filter(prop.get(_, authContext))
  }
}

case class OrFilter(inputFilters: Seq[InputFilter]) extends InputFilter {
  override def apply[S <: BaseVertexSteps[_, S]](
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S =
    inputFilters.map(ff => (g: GremlinScala[Vertex]) => ff(publicProperties, stepType, step.newInstance(g), authContext).raw) match {
      case Seq()  => step.filter(_.is(null)) // TODO need checks
      case Seq(f) => step.filter(f)
      case f      => step.filter(_.or(f: _*))
    }
}

case class AndFilter(inputFilters: Seq[InputFilter]) extends InputFilter {
  override def apply[S <: BaseVertexSteps[_, S]](
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S =
    inputFilters.map(ff => (g: GremlinScala[Vertex]) => ff(publicProperties, stepType, step.newInstance(g), authContext).raw) match {
      case Seq()       => step
      case Seq(f)      => step.filter(f)
      case Seq(f @ _*) => step.filter(_.and(f: _*))

    }
}

case class NotFilter(inputFilter: InputFilter) extends InputFilter {
  override def apply[S <: BaseVertexSteps[_, S]](
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S = {
    val criteria = (g: GremlinScala[Vertex]) => inputFilter(publicProperties, stepType, step.newInstance(g), authContext).raw
    step.filter(_.not(criteria))
  }
}

object YesFilter extends InputFilter {
  override def apply[S <: BaseVertexSteps[_, S]](
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S = step
}

class IdFilter(id: String) extends InputFilter {
  override def apply[S <: BaseVertexSteps[_, S]](
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S = step.get(id)
}

object InputFilter {
  lazy val logger = Logger(getClass)

  def is(field: String, value: Any): PredicateFilter = PredicateFilter(field, P.is(value))

  def neq(field: String, value: Any): PredicateFilter = PredicateFilter(field, P.neq(value))

  def lt(field: String, value: Any): PredicateFilter = PredicateFilter(field, P.lt(value))

  def gt(field: String, value: Any): PredicateFilter = PredicateFilter(field, P.gt(value))

  def lte(field: String, value: Any): PredicateFilter = PredicateFilter(field, P.lte(value))

  def gte(field: String, value: Any): PredicateFilter = PredicateFilter(field, P.gte(value))

  def isDefined(field: String): IsDefinedFilter = IsDefinedFilter(field)

  def between(field: String, from: Any, to: Any): PredicateFilter = PredicateFilter(field, P.between(from, to))

  def inside(field: String, from: Any, to: Any): PredicateFilter = PredicateFilter(field, P.inside(from, to))

  def in(field: String, values: Any*): PredicateFilter = PredicateFilter(field, P.within(values))

  def has(field: String, value: String): PredicateFilter = PredicateFilter(field, stringContains(value))

  def stringContains(value: String): P[String] = P.fromPredicate[String]((v, r) => v contains r, value)

  def startsWith(field: String, value: String): PredicateFilter = PredicateFilter(field, stringStartsWith(value))

  def stringStartsWith(value: String): P[String] = P.fromPredicate[String]((v, r) => v startsWith r, value)

  def endsWith(field: String, value: String): PredicateFilter = PredicateFilter(field, stringEndsWith(value))

  def stringEndsWith(value: String): P[String] = P.fromPredicate[String]((v, r) => v endsWith r, value)

  def or(filters: Seq[InputFilter]): OrFilter = OrFilter(filters)

  def and(filters: Seq[InputFilter]): AndFilter = AndFilter(filters)

  def not(filter: InputFilter): NotFilter = NotFilter(filter)

  def yes: YesFilter.type = YesFilter

  def withId(id: String): InputFilter = new IdFilter(id)

  //  def in(field: String, values: Seq[Any]) = OrFilter(values.map(v ⇒ PredicateFilter(field, P.is(v))))

  def fieldsParser(tpe: ru.Type, properties: Seq[PublicProperty[_, _]]): FieldsParser[InputFilter] = {
    val props = properties.filter(_.stepType =:= tpe)
    def propParser(name: String): FieldsParser[Any] =
      props
        .find(_.propertyName == name)
        .fold(FieldsParser.unknownAttribute[Any](name)) { prop =>
          prop.fieldsParser.map(prop.fieldsParser.formatName)(v => prop.mapping.asInstanceOf[Mapping[_, Any, Any]].toGraph(v))
        }
    FieldsParser("query") {
      case (path, FObjOne("_and", FSeq(fields))) =>
        fields.zipWithIndex.validatedBy { case (field, index) => fieldsParser(tpe, properties)((path :/ "_and").toSeq(index), field) }.map(and)
      case (path, FObjOne("_or", FSeq(fields))) =>
        fields.zipWithIndex.validatedBy { case (field, index) => fieldsParser(tpe, properties)((path :/ "_or").toSeq(index), field) }.map(or)
      case (path, FObjOne("_not", field))                            => fieldsParser(tpe, properties)(path :/ "_not", field).map(not)
      case (_, FObjOne("_any", _))                                   => Good(yes)
      case (_, FObjOne("_lt", FObjOne(key, field)))                  => propParser(key)(field).map(value => lt(key, value))
      case (_, FObjOne("_gt", FObjOne(key, field)))                  => propParser(key)(field).map(value => gt(key, value))
      case (_, FObjOne("_lte", FObjOne(key, field)))                 => propParser(key)(field).map(value => lte(key, value))
      case (_, FObjOne("_gte", FObjOne(key, field)))                 => propParser(key)(field).map(value => gte(key, value))
      case (_, FObjOne("_is", FObjOne(key, field)))                  => propParser(key)(field).map(value => is(key, value))
      case (_, FObjOne("_has", FObjOne(key, FString(value))))        => Good(has(key, value))
      case (_, FObjOne("_startsWith", FObjOne(key, FString(value)))) => Good(startsWith(key, value))
      case (_, FObjOne("_endsWith", FObjOne(key, FString(value))))   => Good(endsWith(key, value))
      case (_, FObjOne("_id", FString(id)))                          => Good(withId(id))
      case (_, FObjOne("_between", FFieldFromTo(key, fromField, toField))) =>
        withGood(propParser(key)(fromField), propParser(key)(toField))(between(key, _, _))
      case (_, FObjOne("_string", _)) =>
        logger.warn("string filter is not supported, it is ignored")
        Good(yes)
      case (_, FObjOne("_in", o: FObject)) =>
        for {
          key <- FieldsParser.string(o.get("_field"))
          s   <- FSeq.parser(o.get("_values"))
          valueParser = propParser(key)
          values <- s.values.validatedBy(valueParser.apply)
        } yield in(key, values: _*)
      case (_, FObjOne("_contains", FString(path))) => Good(isDefined(path))
      case (_, FFieldValue(key, field))             => propParser(key)(field).map(value => is(key, value))
      case (_, FObjOne(key, field)) =>
        logger.warn(s"""Use of filter {"$key": "$field"} is deprecated. Please use {"_is":{"$key":"$field"}}""")
        propParser(key)(field).map(value => is(key, value))
      case (_, FObject(kv)) if kv.isEmpty => Good(yes)
    }
  }
}
