package org.thp.scalligraph.query

import scala.reflect.runtime.{universe => ru}

import play.api.Logger

import java.util.{Collection => JCollection}
import gremlin.scala.P
import scala.collection.JavaConverters._
import org.apache.tinkerpop.gremlin.process.traversal.TextP
import org.scalactic.Accumulation._
import org.scalactic.Good
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.{Database, Mapping}
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{BaseVertexSteps, IdMapping, Traversal}

trait InputFilter extends InputQuery {

  def apply[S <: BaseVertexSteps](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S
}

case class PredicateFilter(fieldName: String, predicate: P[_]) extends InputFilter {

  override def apply[S <: BaseVertexSteps](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S =
    step.filter { s =>
      val property: Traversal[Any, Any] =
        PublicProperty.getPropertyTraversal(publicProperties, stepType, s, fieldName, authContext).asInstanceOf[Traversal[Any, Any]]
      if (property.mapping == IdMapping) {
        val newValue = predicate.getValue match {
          case c: JCollection[_] => c.asScala.map(db.toId).asJavaCollection
          case other             => db.toId(other)
        }
        predicate.asInstanceOf[P[Any]].setValue(newValue)
      }
      property.is(db.mapPredicate(predicate.asInstanceOf[P[Any]]))
    }
}

case class IsDefinedFilter(fieldName: String) extends InputFilter {

  override def apply[S <: BaseVertexSteps](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S = {
    val filter = (s: BaseVertexSteps) => PublicProperty.getPropertyTraversal(publicProperties, stepType, s, fieldName, authContext)
    step.filter(filter)
  }
}

case class OrFilter(inputFilters: Seq[InputFilter]) extends InputFilter {

  override def apply[S <: BaseVertexSteps](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S =
    inputFilters.map(ff => (s: S) => ff(db, publicProperties, stepType, s, authContext)) match {
      case Seq(f) => step.filter(f)
      case Seq()  => step.filter(_.not(identity))
      case f      => step.filter(_.or(f: _*))
    }
}

case class AndFilter(inputFilters: Seq[InputFilter]) extends InputFilter {

  override def apply[S <: BaseVertexSteps](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S =
    inputFilters.map(ff => (s: S) => ff(db, publicProperties, stepType, s, authContext)) match {
      case Seq(f)      => step.filter(f)
      case Seq()       => step.filter(_.not(identity))
      case Seq(f @ _*) => step.filter(_.and(f: _*))
    }
}

case class NotFilter(inputFilter: InputFilter) extends InputFilter {

  override def apply[S <: BaseVertexSteps](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S = {
    val criteria: BaseVertexSteps => BaseVertexSteps = (s: BaseVertexSteps) => inputFilter(db, publicProperties, stepType, s, authContext)
    step.filter(_.not(criteria))
  }
}

object YesFilter extends InputFilter {

  override def apply[S <: BaseVertexSteps](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S = step
}

class IdFilter(id: String) extends InputFilter {

  override def apply[S <: BaseVertexSteps](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S = step.getByIds(id)
}

object InputFilter {
  lazy val logger: Logger = Logger(getClass)

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

  def startsWith(field: String, value: String): PredicateFilter = PredicateFilter(field, TextP.startingWith(value))

  def endsWith(field: String, value: String): PredicateFilter = PredicateFilter(field, TextP.endingWith(value))

  def or(filters: Seq[InputFilter]): OrFilter = OrFilter(filters)

  def and(filters: Seq[InputFilter]): AndFilter = AndFilter(filters)

  def not(filter: InputFilter): NotFilter = NotFilter(filter)

  def yes: YesFilter.type = YesFilter

  def withId(id: String): InputFilter = new IdFilter(id)

  def like(field: String, value: String): PredicateFilter = {
    val s = value.headOption.contains('*')
    val e = value.lastOption.contains('*')
    if (s && e) PredicateFilter(field, TextP.containing(value.tail.dropRight(1)))
    else if (s) PredicateFilter(field, TextP.endingWith(value.tail))
    else if (e) PredicateFilter(field, TextP.startingWith(value.dropRight(1)))
    else PredicateFilter(field, P.eq(value))
  }

  def fieldsParser(
      tpe: ru.Type,
      properties: Seq[PublicProperty[_, _]],
      globalParser: ru.Type => FieldsParser[InputFilter]
  ): FieldsParser[InputFilter] = {
    def propParser(name: String): FieldsParser[Any] = {
      val prop = PublicProperty.getProperty(properties, tpe, name)
      prop.fieldsParser.map(prop.fieldsParser.formatName)(v => prop.mapping.asInstanceOf[Mapping[_, Any, Any]].toGraph(v))
    }

    FieldsParser("query") {
      case (path, FObjOne("_and", FSeq(fields))) =>
        fields.zipWithIndex.validatedBy { case (field, index) => globalParser(tpe)((path :/ "_and").toSeq(index), field) }.map(and)
      case (path, FObjOne("_or", FSeq(fields))) =>
        fields.zipWithIndex.validatedBy { case (field, index) => globalParser(tpe)((path :/ "_or").toSeq(index), field) }.map(or)
      case (path, FObjOne("_not", field))                                      => globalParser(tpe)(path :/ "_not", field).map(not)
      case (_, FObjOne("_any", _))                                             => Good(yes)
      case (_, FObjOne("_lt", FFieldValue(key, field)))                        => propParser(key)(field).map(value => lt(key, value))
      case (_, FObjOne("_lt", FDeprecatedObjOne(key, field)))                  => propParser(key)(field).map(value => lt(key, value))
      case (_, FObjOne("_gt", FFieldValue(key, field)))                        => propParser(key)(field).map(value => gt(key, value))
      case (_, FObjOne("_gt", FDeprecatedObjOne(key, field)))                  => propParser(key)(field).map(value => gt(key, value))
      case (_, FObjOne("_lte", FFieldValue(key, field)))                       => propParser(key)(field).map(value => lte(key, value))
      case (_, FObjOne("_lte", FDeprecatedObjOne(key, field)))                 => propParser(key)(field).map(value => lte(key, value))
      case (_, FObjOne("_gte", FFieldValue(key, field)))                       => propParser(key)(field).map(value => gte(key, value))
      case (_, FObjOne("_gte", FDeprecatedObjOne(key, field)))                 => propParser(key)(field).map(value => gte(key, value))
      case (_, FObjOne("_is", FFieldValue(key, field)))                        => propParser(key)(field).map(value => is(key, value))
      case (_, FObjOne("_is", FDeprecatedObjOne(key, field)))                  => propParser(key)(field).map(value => is(key, value))
      case (_, FObjOne("_startsWith", FFieldValue(key, FString(value))))       => Good(startsWith(key, value))
      case (_, FObjOne("_startsWith", FDeprecatedObjOne(key, FString(value)))) => Good(startsWith(key, value))
      case (_, FObjOne("_endsWith", FFieldValue(key, FString(value))))         => Good(endsWith(key, value))
      case (_, FObjOne("_endsWith", FDeprecatedObjOne(key, FString(value))))   => Good(endsWith(key, value))
      case (_, FObjOne("_id", FString(id)))                                    => Good(withId(id))
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
      case (_, FObjOne("_contains", FString(path)))                            => Good(isDefined(path))
      case (_, FObjOne("_like", FFieldValue(key, FString(value))))             => Good(like(key, value))
      case (_, FObjOne("_like", FDeprecatedObjOne(key, FString(value))))       => Good(like(key, value))
      case (_, FObjOne("_wildcard", FFieldValue(key, FString(value))))         => Good(like(key, value))
      case (_, FObjOne("_wildcard", FDeprecatedObjOne(key, FString(value))))   => Good(like(key, value))
      case (_, FFieldValue(key, field))                                        => propParser(key)(field).map(value => is(key, value))
      case (_, FDeprecatedObjOne(key, field)) if !key.headOption.contains('_') => propParser(key)(field).map(value => is(key, value))
      case (_, FObject(kv)) if kv.isEmpty                                      => Good(yes)
    }
  }
}
