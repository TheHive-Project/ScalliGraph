package org.thp.scalligraph.query

import scala.reflect.runtime.{currentMirror => rm, universe => ru}

import play.api.libs.json.JsValue

import gremlin.scala.{Graph, GremlinScala}
import org.scalactic.Good
import org.thp.scalligraph.BadRequestError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.Database
import org.thp.scalligraph.steps.{BaseTraversal, BaseVertexSteps, TraversalLike}
import org.thp.scalligraph.utils.RichType

// Use global lock because scala reflection subtype operator <:< is not thread safe (scala/bug#10766)
// https://stackoverflow.com/questions/56854716/inconsistent-result-when-checking-type-subtype
object SubType {
  def apply(t1: ru.Type, t2: ru.Type): Boolean = synchronized(t1 <:< t2)
}

abstract class ParamQuery[P: ru.TypeTag] { q =>
  val paramType: ru.Type = ru.typeOf[P]
  def paramParser(tpe: ru.Type, properties: Seq[PublicProperty[_, _]]): FieldsParser[P]
  val name: String
  def checkFrom(t: ru.Type): Boolean
  def toType(t: ru.Type): ru.Type

  def toQuery(param: P): Query = new Query {
    override val name: String                                                     = q.name
    override def checkFrom(t: ru.Type): Boolean                                   = q.checkFrom(t)
    override def toType(t: ru.Type): ru.Type                                      = q.toType(t)
    override def apply(unitParam: Unit, from: Any, authContext: AuthContext): Any = q(param, from, authContext)
  }
  def apply(param: P, from: Any, authContext: AuthContext): Any
}

abstract class Query extends ParamQuery[Unit] { q =>
  override def paramParser(tpe: ru.Type, properties: Seq[PublicProperty[_, _]]): FieldsParser[Unit] = FieldsParser[Unit]("unit") {
    case _ => Good(())
  }

  def andThen(query: Query): Query = new Query {
    override val name: String                                                 = q.name + "/" + query.name
    override def checkFrom(t: ru.Type): Boolean                               = q.checkFrom(t)
    override def toType(t: ru.Type): ru.Type                                  = query.toType(q.toType(t))
    override def apply(param: Unit, from: Any, authContext: AuthContext): Any = query(param, q(param, from, authContext), authContext)
  }
}

object Query {

  def init[T: ru.TypeTag](queryName: String, f: (Graph, AuthContext) => T): Query = new Query {
    override val name: String                                                 = queryName
    override def checkFrom(t: ru.Type): Boolean                               = SubType(t, ru.typeOf[Graph])
    override def toType(t: ru.Type): ru.Type                                  = ru.typeOf[T]
    override def apply(param: Unit, from: Any, authContext: AuthContext): Any = f(from.asInstanceOf[Graph], authContext)
  }

  def initWithParam[P: ru.TypeTag, T: ru.TypeTag](queryName: String, parser: FieldsParser[P], f: (P, Graph, AuthContext) => T): ParamQuery[P] =
    new ParamQuery[P] {
      override def paramParser(tpe: ru.Type, properties: Seq[PublicProperty[_, _]]): FieldsParser[P] = parser
      override val name: String                                                                      = queryName
      override def checkFrom(t: ru.Type): Boolean                                                    = SubType(t, ru.typeOf[Graph])
      override def toType(t: ru.Type): ru.Type                                                       = ru.typeOf[T]
      override def apply(param: P, from: Any, authContext: AuthContext): Any                         = f(param, from.asInstanceOf[Graph], authContext)
    }

  def apply[F: ru.TypeTag, T: ru.TypeTag](queryName: String, f: (F, AuthContext) => T): Query = new Query {
    override val name: String                                                 = queryName
    override def checkFrom(t: ru.Type): Boolean                               = SubType(t, ru.typeOf[F])
    override def toType(t: ru.Type): ru.Type                                  = ru.typeOf[T]
    override def apply(param: Unit, from: Any, authContext: AuthContext): Any = f(from.asInstanceOf[F], authContext)
  }

  def withParam[P: ru.TypeTag, F: ru.TypeTag, T: ru.TypeTag](queryName: String, parser: FieldsParser[P], f: (P, F, AuthContext) => T): ParamQuery[P] =
    new ParamQuery[P] {
      override def paramParser(tpe: ru.Type, properties: Seq[PublicProperty[_, _]]): FieldsParser[P] = parser
      override val name: String                                                                      = queryName
      override def checkFrom(t: ru.Type): Boolean                                                    = SubType(t, ru.typeOf[F])
      override def toType(t: ru.Type): ru.Type                                                       = ru.typeOf[T]
      override def apply(param: P, from: Any, authContext: AuthContext): Any                         = f(param, from.asInstanceOf[F], authContext)
    }

  def output[F: ru.TypeTag, T: ru.TypeTag](implicit toOutput: F => Output[T]): Query = new Query {
    override val name: String                                                 = "toOutput"
    override def checkFrom(t: ru.Type): Boolean                               = SubType(t, ru.typeOf[F])
    override def toType(t: ru.Type): ru.Type                                  = ru.appliedType(ru.typeOf[Output[_]].typeConstructor, ru.typeOf[T])
    override def apply(param: Unit, from: Any, authContext: AuthContext): Any = toOutput(from.asInstanceOf[F])
  }
}

class SortQuery(db: Database, publicProperties: List[PublicProperty[_, _]]) extends ParamQuery[InputSort] {
  override def paramParser(tpe: ru.Type, properties: Seq[PublicProperty[_, _]]): FieldsParser[InputSort] = InputSort.fieldsParser
  override val name: String                                                                              = "sort"
  override def checkFrom(t: ru.Type): Boolean                                                            = SubType(t, ru.typeOf[BaseTraversal])
  override def toType(t: ru.Type): ru.Type                                                               = t
  override def apply(inputSort: InputSort, from: Any, authContext: AuthContext): Any =
    inputSort(
      db,
      publicProperties,
      rm.classSymbol(from.getClass).toType,
      from.asInstanceOf[BaseVertexSteps],
      authContext
    )
}

class FilterQuery(db: Database, publicProperties: List[PublicProperty[_, _]]) extends ParamQuery[InputFilter] {
  override def paramParser(tpe: ru.Type, properties: Seq[PublicProperty[_, _]]): FieldsParser[InputFilter] =
    InputFilter.fieldsParser(tpe, properties)
  override val name: String                   = "filter"
  override def checkFrom(t: ru.Type): Boolean = SubType(t, ru.typeOf[BaseVertexSteps])
  override def toType(t: ru.Type): ru.Type    = t
  override def apply(inputFilter: InputFilter, from: Any, authContext: AuthContext): Any =
    inputFilter(
      db,
      publicProperties,
      rm.classSymbol(from.getClass).toType,
      from.asInstanceOf[BaseVertexSteps], //.asInstanceOf[X forSome { type X <: BaseVertexSteps[_, X] }],
      authContext
    )
}

class AggregationQuery(publicProperties: List[PublicProperty[_, _]]) extends ParamQuery[GroupAggregation[_, _, _]] {
  override def paramParser(tpe: ru.Type, properties: Seq[PublicProperty[_, _]]): FieldsParser[GroupAggregation[_, _, _]] =
    GroupAggregation.fieldsParser
  override val name: String                   = "aggregation"
  override def checkFrom(t: ru.Type): Boolean = SubType(t, ru.typeOf[BaseVertexSteps])
  override def toType(t: ru.Type): ru.Type    = ru.typeOf[JsValue]
  override def apply(aggregation: GroupAggregation[_, _, _], from: Any, authContext: AuthContext): Any =
    aggregation.get(
      publicProperties,
      rm.classSymbol(from.getClass).toType,
      from.asInstanceOf[BaseVertexSteps],
      authContext
    )
}

object ToListQuery extends Query {
  override val name: String = "toList"

  override def checkFrom(t: ru.Type): Boolean = SubType(t, ru.typeOf[BaseTraversal]) || SubType(t, ru.typeOf[GremlinScala[_]])

  override def toType(t: ru.Type): ru.Type = {
    val subType = if (SubType(t, ru.typeOf[TraversalLike[_, _]])) {
      RichType.getTypeArgs(t, ru.typeOf[TraversalLike[_, _]]).head
    } else if (SubType(t, ru.typeOf[GremlinScala[_]])) {
      RichType.getTypeArgs(t, ru.typeOf[GremlinScala[_]]).head
    } else {
      throw BadRequestError(s"toList can't be used with $t. It must be a ScalliSteps or a GremlinScala")
    }
    ru.appliedType(ru.typeOf[List[_]].typeConstructor, subType)
  }

  override def apply(param: Unit, from: Any, authContext: AuthContext): Any = from match {
    case t: TraversalLike[_, _] => t.toList
    case f: GremlinScala[_]     => f.toList
  }
}

trait InputQuery {

  def apply[S <: BaseVertexSteps](
      db: Database,
      publicProperties: List[PublicProperty[_, _]],
      stepType: ru.Type,
      step: S,
      authContext: AuthContext
  ): S

  def getProperty(properties: Seq[PublicProperty[_, _]], stepType: ru.Type, fieldName: String): PublicProperty[_, _] =
    properties
      .find(p => p.stepType =:= stepType && p.propertyName == fieldName.takeWhile(_ != '.'))
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $stepType not found"))
}
