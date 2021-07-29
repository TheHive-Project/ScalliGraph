package org.thp.scalligraph.query

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.structure.Element
import org.scalactic.Accumulation._
import org.scalactic.{Bad, Every, Good, One, Or}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.{FullTextPredicate, IndexType, TextPredicate}
import org.thp.scalligraph.traversal.{Traversal, TraversalOps}
import org.thp.scalligraph.{AttributeError, BadRequestError, EntityId, EntityIdOrName, InvalidFormatAttributeError, UnknownAttributeError}
import play.api.Logger

import scala.reflect.runtime.{universe => ru}

case class PredicateFilter(property: PublicProperty, propertyPath: FPath, predicate: P[_]) extends InputFilter {
  override def apply(
      publicProperties: PublicProperties,
      traversalType: ru.Type,
      traversal: Traversal.Unk,
      authContext: AuthContext
  ): Traversal.Unk =
    property.filter(propertyPath, traversal, authContext, if (isNegate) predicate.negate() else predicate)
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
  lazy val logger: Logger = Logger(getClass)
  def like(value: String): P[Any] = {
    val s = value.headOption.contains('*')
    val e = value.lastOption.contains('*')
    if (s && e) TextPredicate.contains(value.tail.dropRight(1))
    else if (s) TextPredicate.endsWith(value.tail)
    else if (e) TextPredicate.startsWith(value.dropRight(1))
    else TextPredicate.contains(value)
  }.asInstanceOf[P[Any]]
  def tokenLike(value: String): P[Any] = {
    val s = value.headOption.contains('*')
    val e = value.lastOption.contains('*')
    if (s && e) FullTextPredicate.contains(value.tail.dropRight(1))
    else if (s) FullTextPredicate.endsWith(value.tail)
    else if (e) FullTextPredicate.startsWith(value.dropRight(1))
    else FullTextPredicate.contains(value)
  }.asInstanceOf[P[Any]]

  def fieldsParser(
      tpe: ru.Type,
      properties: PublicProperties,
      globalParser: ru.Type => FieldsParser[InputFilter]
  ): FieldsParser[InputFilter] = {

    def prop(name: String, field: Field)(predicate: (PublicProperty, Any) => P[Any]): Or[PredicateFilter, Every[AttributeError]] = {
      val path = FPath(name)
      properties
        .get[Traversal.UnkD, Traversal.UnkDU](path, tpe)
        .fold[Or[PredicateFilter, Every[AttributeError]]](Bad(One(UnknownAttributeError(name, field)))) { p =>
          p.filter.fieldsParser(field).map(v => PredicateFilter(p, path, predicate(p, v)))
        }
    }
    def prop2(name: String, field1: Field, field2: Field)(
        predicate: (PublicProperty, Any, Any) => P[Any]
    ): Or[PredicateFilter, Every[AttributeError]] = {
      val path = FPath(name)
      properties
        .get[Traversal.UnkD, Traversal.UnkDU](path, tpe)
        .fold[Or[PredicateFilter, Every[AttributeError]]](Bad(One(UnknownAttributeError(name, field1)))) { p =>
          withGood(p.filter.fieldsParser(field1), p.filter.fieldsParser(field2))((v1, v2) => PredicateFilter(p, path, predicate(p, v1, v2)))
        }
    }
    def props(name: String, fields: Seq[Field])(
        predicate: (PublicProperty, Seq[Any]) => P[Any]
    ): Or[PredicateFilter, Every[AttributeError]] = {
      val path = FPath(name)
      properties
        .get[Traversal.UnkD, Traversal.UnkDU](path, tpe)
        .fold[Or[PredicateFilter, Every[AttributeError]]](Bad(One(UnknownAttributeError(name, FSeq(fields.toList))))) { p =>
          fields.validatedBy(p.filter.fieldsParser(_)).map(vs => PredicateFilter(p, path, predicate(p, vs)))
        }
    }

    FieldsParser("query") {
      case (_, FObjOne("_match", FFieldValue(key, field))) =>
        prop(key, field) { (p, v) =>
          p.indexType match {
            case IndexType.basic | IndexType.unique | IndexType.none => P.eq(v)
            case IndexType.standard                                  => like(v.toString)
            case IndexType.fulltext | IndexType.fulltextOnly         => tokenLike(v.toString)
          }
        }
      case (path, FObjOne("_and", FSeq(fields))) =>
        fields.zipWithIndex.validatedBy { case (field, index) => globalParser(tpe)((path :/ "_and").toSeq(index), field) }.map(AndFilter)
      case (path, FObjOne("_or", FSeq(fields))) =>
        fields.zipWithIndex.validatedBy { case (field, index) => globalParser(tpe)((path :/ "_or").toSeq(index), field) }.map(OrFilter)
      case (path, FObjOne("_not", field))                      => globalParser(tpe)(path :/ "_not", field).map(_.negate)
      case (_, FObjOne("_any", _))                             => Good(YesNoFilter())
      case (_, FObjOne("_lt", FFieldValue(key, field)))        => prop(key, field)((_, v) => P.lt(v))
      case (_, FObjOne("_lt", FDeprecatedObjOne(key, field)))  => prop(key, field)((_, v) => P.lt(v))
      case (_, FObjOne("_gt", FFieldValue(key, field)))        => prop(key, field)((_, v) => P.gt(v))
      case (_, FObjOne("_gt", FDeprecatedObjOne(key, field)))  => prop(key, field)((_, v) => P.gt(v))
      case (_, FObjOne("_lte", FFieldValue(key, field)))       => prop(key, field)((_, v) => P.lte(v))
      case (_, FObjOne("_lte", FDeprecatedObjOne(key, field))) => prop(key, field)((_, v) => P.lte(v))
      case (_, FObjOne("_gte", FFieldValue(key, field)))       => prop(key, field)((_, v) => P.gte(v))
      case (_, FObjOne("_gte", FDeprecatedObjOne(key, field))) => prop(key, field)((_, v) => P.gte(v))
      case (_, FObjOne("_ne", FFieldValue(key, field)))        => prop(key, field)((_, v) => P.neq(v))
      case (_, FObjOne("_ne", FDeprecatedObjOne(key, field)))  => prop(key, field)((_, v) => P.neq(v))
      case (_, FObjOne("_is", FFieldValue(key, field)))        => prop(key, field)((_, v) => P.eq(v))
      case (_, FObjOne("_is", FDeprecatedObjOne(key, field)))  => prop(key, field)((_, v) => P.eq(v))
      case (_, FObjOne("_startsWith", FFieldValue(key, field))) =>
        prop(key, field)((_, v) => TextPredicate.startsWith(v.toString).asInstanceOf[P[Any]])
      case (_, FObjOne("_startsWith", FDeprecatedObjOne(key, field))) =>
        prop(key, field)((_, v) => TextPredicate.startsWith(v.toString).asInstanceOf[P[Any]])
      case (_, FObjOne("_endsWith", FFieldValue(key, field))) => prop(key, field)((_, v) => TextPredicate.endsWith(v.toString).asInstanceOf[P[Any]])
      case (_, FObjOne("_endsWith", FDeprecatedObjOne(key, field))) =>
        prop(key, field)((_, v) => TextPredicate.endsWith(v.toString).asInstanceOf[P[Any]])
      case (_, FObjOne("_id", FString(id))) =>
        EntityIdOrName(id) match {
          case entityId: EntityId => Good(IdFilter(entityId))
          case _                  => Bad(One(InvalidFormatAttributeError("_id", "id", Set.empty, FString(id))))
        }
      case (_, FObjOne("_between", FFieldFromTo(key, fromField, toField))) =>
        prop2(key, fromField, toField)((_, from, to) => P.between(from, to))
      case (_, FObjOne("_in", o: FObject)) =>
        for {
          key <- FieldsParser.string(o.get("_field"))
          s   <- FSeq.parser(o.get("_values"))
          p   <- props(key, s.values)((_, vs) => P.within(vs: _*))
        } yield p
      case (_, FObjOne("_contains", FString(path))) => Good(IsDefinedFilter(path))
      case (_, FObjOne("_like", FFieldValue(key, field))) =>
        prop(key, field)((_, v) => like(v.toString))
      case (_, FObjOne("_like", FDeprecatedObjOne(key, field))) =>
        prop(key, field)((_, v) => like(v.toString))
      case (_, FFieldValue(key, field))                                        => prop(key, field)((_, v) => P.eq(v))
      case (_, FDeprecatedObjOne(key, field)) if !key.headOption.contains('_') => prop(key, field)((_, v) => P.eq(v))
      case (_, FObject(kv)) if kv.isEmpty                                      => Good(YesNoFilter())
    }
  }
}
