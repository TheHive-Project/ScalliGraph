package org.thp.scalligraph.query

import org.apache.tinkerpop.gremlin.process.traversal.{P, TextP}
import org.apache.tinkerpop.gremlin.structure.Element
import org.scalactic.Accumulation._
import org.scalactic.{Bad, Good, One}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.IndexType
import org.thp.scalligraph.traversal.{Traversal, TraversalOps}
import org.thp.scalligraph.{BadRequestError, EntityId, EntityIdOrName, InvalidFormatAttributeError}
import play.api.Logger

import scala.reflect.runtime.{universe => ru}

case class PredicateFilter(fieldName: String, predicate: P[_]) extends InputFilter {
  override def apply(
      publicProperties: PublicProperties,
      traversalType: ru.Type,
      traversal: Traversal.Unk,
      authContext: AuthContext
  ): Traversal.Unk = {
    val propertyPath = FPath(fieldName)
    val property =
      publicProperties
        .get[Traversal.UnkD, Traversal.UnkDU](propertyPath, traversalType)
        .getOrElse(throw BadRequestError(s"Property $fieldName for type $traversalType not found"))
    property.filter(propertyPath, traversal, authContext, if (isNegate) predicate.negate() else predicate)
  }
}

case class IsDefinedFilter(fieldName: String) extends InputFilter with TraversalOps {
  override def apply(
      publicProperties: PublicProperties,
      traversalType: ru.Type,
      traversal: Traversal.Unk,
      authContext: AuthContext
  ): Traversal.Unk = {
    val propertyPath = FPath(fieldName)
    publicProperties
      .get[Traversal.UnkD, Traversal.UnkDU](propertyPath, traversalType)
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $traversalType not found"))
      .filter
      .existenceTest(propertyPath, traversal, authContext, !isNegate)
  }
}

case class OrFilter(inputFilters: Seq[InputFilter]) extends InputFilter with TraversalOps {
  override def apply(
      publicProperties: PublicProperties,
      traversalType: ru.Type,
      traversal: Traversal.Unk,
      authContext: AuthContext
  ): Traversal.Unk =
    if (isNegate)
      AndFilter(inputFilters.map(_.negate))(publicProperties, traversalType, traversal, authContext)
    else
      inputFilters.map(ff => (t: Traversal.Unk) => ff(publicProperties, traversalType, t, authContext)) match {
        case Seq(f) => traversal.filter(f)
        case Seq()  => traversal.empty
        case f      => traversal.filter(_.or(f: _*))
      }
}

case class AndFilter(inputFilters: Seq[InputFilter]) extends InputFilter {
  override def apply(
      publicProperties: PublicProperties,
      traversalType: ru.Type,
      traversal: Traversal.Unk,
      authContext: AuthContext
  ): Traversal.Unk =
    if (isNegate)
      OrFilter(inputFilters.map(_.negate))(publicProperties, traversalType, traversal, authContext)
    else
      inputFilters
        .map(ff => (t: Traversal.Unk) => ff(publicProperties, traversalType, t, authContext))
        .foldLeft(traversal)((t, f) => f(t))
}

case class YesNoFilter(isYes: Boolean = true) extends InputFilter with TraversalOps {
  override def apply(
      publicProperties: PublicProperties,
      traversalType: ru.Type,
      traversal: Traversal.Unk,
      authContext: AuthContext
  ): Traversal.Unk = if (isYes == isNegate) traversal.empty else traversal
}

case class IdFilter(id: EntityId) extends InputFilter with TraversalOps {
  override def apply(
      publicProperties: PublicProperties,
      traversalType: ru.Type,
      traversal: Traversal.Unk,
      authContext: AuthContext
  ): Traversal.Unk =
    if (isNegate) traversal.cast[Traversal.UnkD, Element].hasNotId(id).asInstanceOf[Traversal.Unk]
    else traversal.cast[Traversal.UnkD, Element].hasId(id).asInstanceOf[Traversal.Unk]
}

trait InputFilter extends InputQuery[Traversal.Unk, Traversal.Unk] {
  protected var isNegate: Boolean = false
  def apply(
      publicProperties: PublicProperties,
      traversalType: ru.Type,
      traversal: Traversal.Unk,
      authContext: AuthContext
  ): Traversal.Unk
  final def negate: this.type = {
    isNegate = !isNegate
    this
  }
}

object InputFilter {
  lazy val logger: Logger                                         = Logger(getClass)
  def is(field: String, value: Any): PredicateFilter              = PredicateFilter(field, P.eq(value))
  def neq(field: String, value: Any): PredicateFilter             = PredicateFilter(field, P.neq(value))
  def lt(field: String, value: Any): PredicateFilter              = PredicateFilter(field, P.lt(value))
  def gt(field: String, value: Any): PredicateFilter              = PredicateFilter(field, P.gt(value))
  def lte(field: String, value: Any): PredicateFilter             = PredicateFilter(field, P.lte(value))
  def gte(field: String, value: Any): PredicateFilter             = PredicateFilter(field, P.gte(value))
  def isDefined(field: String): IsDefinedFilter                   = IsDefinedFilter(field)
  def between(field: String, from: Any, to: Any): PredicateFilter = PredicateFilter(field, P.between(from, to))
  def inside(field: String, from: Any, to: Any): PredicateFilter  = PredicateFilter(field, P.inside(from, to))
  def in(field: String, values: Any*): PredicateFilter            = PredicateFilter(field, P.within(values: _*))
  def startsWith(field: String, value: String): PredicateFilter   = PredicateFilter(field, TextP.startingWith(value))
  def endsWith(field: String, value: String): PredicateFilter     = PredicateFilter(field, TextP.endingWith(value))
  def or(filters: Seq[InputFilter]): OrFilter                     = OrFilter(filters)
  def and(filters: Seq[InputFilter]): AndFilter                   = AndFilter(filters)
  def not(filter: InputFilter): InputFilter                       = filter.negate
  def yes: YesNoFilter                                            = YesNoFilter()
  def withId(id: EntityId): InputFilter                           = IdFilter(id)
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
      properties: PublicProperties,
      globalParser: ru.Type => FieldsParser[InputFilter]
  ): FieldsParser[InputFilter] = {
    def propParser(name: String): FieldsParser[Any] =
      properties
        .get[Traversal.UnkD, Traversal.UnkDU](FPath(name), tpe)
        .getOrElse(throw BadRequestError(s"Property $name for type $tpe not found"))
        .filter
        .fieldsParser
        .asInstanceOf[FieldsParser[Any]]

    FieldsParser("query") {
      case (_, FObjOne("_match", FFieldValue(key, field))) =>
        val property = properties
          .get[Traversal.UnkD, Traversal.UnkDU](FPath(key), tpe)
          .getOrElse(throw BadRequestError(s"Property $key for type $tpe not found"))
        property.filter.fieldsParser.asInstanceOf[FieldsParser[Any]](field).map { value =>
          property.indexType match {
            case IndexType.basic | IndexType.standard | IndexType.unique | IndexType.none => is(key, value)
            case IndexType.fulltext | IndexType.fulltextOnly                              => like(key, value.toString)
          }
        }
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
      case (_, FObjOne("_ne", FFieldValue(key, field)))                        => propParser(key)(field).map(value => neq(key, value))
      case (_, FObjOne("_ne", FDeprecatedObjOne(key, field)))                  => propParser(key)(field).map(value => neq(key, value))
      case (_, FObjOne("_is", FFieldValue(key, field)))                        => propParser(key)(field).map(value => is(key, value))
      case (_, FObjOne("_is", FDeprecatedObjOne(key, field)))                  => propParser(key)(field).map(value => is(key, value))
      case (_, FObjOne("_startsWith", FFieldValue(key, FString(value))))       => Good(startsWith(key, value))
      case (_, FObjOne("_startsWith", FDeprecatedObjOne(key, FString(value)))) => Good(startsWith(key, value))
      case (_, FObjOne("_endsWith", FFieldValue(key, FString(value))))         => Good(endsWith(key, value))
      case (_, FObjOne("_endsWith", FDeprecatedObjOne(key, FString(value))))   => Good(endsWith(key, value))
      case (_, FObjOne("_id", FString(id))) =>
        EntityIdOrName(id) match {
          case entityId: EntityId => Good(withId(entityId))
          case _                  => Bad(One(InvalidFormatAttributeError("_id", "id", Set.empty, FString(id))))
        }
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
