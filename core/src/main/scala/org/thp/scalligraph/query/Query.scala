package org.thp.scalligraph.query

import scala.reflect.runtime.{currentMirror ⇒ rm, universe ⇒ ru}

import play.api.libs.json.JsValue

import gremlin.scala.{Graph, GremlinScala}
import org.scalactic.Good
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.ScalliSteps
import org.thp.scalligraph.{BadRequestError, Output, RichType}

abstract class ParamQuery[P: ru.TypeTag] { q ⇒
  val paramType: ru.Type = ru.typeOf[P]
  val paramParser: FieldsParser[P]
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

abstract class Query extends ParamQuery[Unit] { q ⇒
  override val paramParser: FieldsParser[Unit] = FieldsParser[Unit]("unit") { case _ ⇒ Good(()) }
  def andThen(query: Query): Query = new Query {
    override val name: String                                                 = q.name + "/" + query.name
    override def checkFrom(t: ru.Type): Boolean                               = q.checkFrom(t)
    override def toType(t: ru.Type): ru.Type                                  = query.toType(q.toType(t))
    override def apply(param: Unit, from: Any, authContext: AuthContext): Any = query(param, q(param, from, authContext), authContext)
  }
}

object Query {
  def init[T: ru.TypeTag](queryName: String, f: (Graph, AuthContext) ⇒ T): Query = new Query {
    override val name: String                                                 = queryName
    override def checkFrom(t: ru.Type): Boolean                               = t <:< ru.typeOf[Graph]
    override def toType(t: ru.Type): ru.Type                                  = ru.typeOf[T]
    override def apply(param: Unit, from: Any, authContext: AuthContext): Any = f(from.asInstanceOf[Graph], authContext)
  }

  def initWithParam[P: ru.TypeTag, T: ru.TypeTag](queryName: String, parser: FieldsParser[P], f: (P, Graph, AuthContext) ⇒ T): ParamQuery[P] =
    new ParamQuery[P] {
      override val paramParser: FieldsParser[P]                              = parser
      override val name: String                                              = queryName
      override def checkFrom(t: ru.Type): Boolean                            = t <:< ru.typeOf[Graph]
      override def toType(t: ru.Type): ru.Type                               = ru.typeOf[T]
      override def apply(param: P, from: Any, authContext: AuthContext): Any = f(param, from.asInstanceOf[Graph], authContext)
    }

  def apply[F: ru.TypeTag, T: ru.TypeTag](queryName: String, f: (F, AuthContext) ⇒ T): Query = new Query {
    override val name: String                                                 = queryName
    override def checkFrom(t: ru.Type): Boolean                               = t <:< ru.typeOf[F]
    override def toType(t: ru.Type): ru.Type                                  = ru.typeOf[T]
    override def apply(param: Unit, from: Any, authContext: AuthContext): Any = f(from.asInstanceOf[F], authContext)
  }

  def withParam[P: ru.TypeTag, F: ru.TypeTag, T: ru.TypeTag](queryName: String, parser: FieldsParser[P], f: (P, F, AuthContext) ⇒ T): ParamQuery[P] =
    new ParamQuery[P] {
      override val paramParser: FieldsParser[P]                              = parser
      override val name: String                                              = queryName
      override def checkFrom(t: ru.Type): Boolean                            = t <:< ru.typeOf[F]
      override def toType(t: ru.Type): ru.Type                               = ru.typeOf[T]
      override def apply(param: P, from: Any, authContext: AuthContext): Any = f(param, from.asInstanceOf[F], authContext)
    }

  def output[F: ru.TypeTag, T: ru.TypeTag](implicit toOutput: F ⇒ Output[T]): Query = new Query {
    override val name: String                                                 = "toOutput"
    override def checkFrom(t: ru.Type): Boolean                               = t <:< ru.typeOf[F]
    override def toType(t: ru.Type): ru.Type                                  = ru.appliedType(ru.typeOf[Output[_]].typeConstructor, ru.typeOf[T])
    override def apply(param: Unit, from: Any, authContext: AuthContext): Any = toOutput(from.asInstanceOf[F])
  }
}

class SortQuery(publicProperties: List[PublicProperty[_, _]]) extends ParamQuery[InputSort] {
  override val paramParser: FieldsParser[InputSort] = InputSort.fieldsParser
  override val name: String                         = "sort"
  override def checkFrom(t: ru.Type): Boolean       = t <:< ru.typeOf[ScalliSteps[_, _, _]]
  override def toType(t: ru.Type): ru.Type          = t
  override def apply(inputSort: InputSort, from: Any, authContext: AuthContext): Any =
    inputSort(publicProperties, rm.classSymbol(from.getClass).toType, from.asInstanceOf[ScalliSteps[_, _, _ <: AnyRef]], authContext)
}

class FilterQuery(publicProperties: List[PublicProperty[_, _]]) extends ParamQuery[InputFilter] {
  override val paramParser: FieldsParser[InputFilter] = InputFilter.fieldsParser
  override val name: String                           = "filter"
  override def checkFrom(t: ru.Type): Boolean         = t <:< ru.typeOf[ScalliSteps[_, _, _]]
  override def toType(t: ru.Type): ru.Type            = t
  override def apply(inputFilter: InputFilter, from: Any, authContext: AuthContext): Any =
    inputFilter(publicProperties, rm.classSymbol(from.getClass).toType, from.asInstanceOf[ScalliSteps[_, _, _ <: AnyRef]], authContext)
}

class AggregationQuery(publicProperties: List[PublicProperty[_, _]]) extends ParamQuery[GroupAggregation[_, _]] {
  override val paramParser: FieldsParser[GroupAggregation[_, _]] = GroupAggregation.fieldsParser
  override val name: String                                      = "aggregation"
  override def checkFrom(t: ru.Type): Boolean                    = t <:< ru.typeOf[ScalliSteps[_, _, _]]
  override def toType(t: ru.Type): ru.Type                       = ru.typeOf[JsValue]
  override def apply(aggregation: GroupAggregation[_, _], from: Any, authContext: AuthContext): Any =
    aggregation.get(publicProperties, rm.classSymbol(from.getClass).toType, from.asInstanceOf[ScalliSteps[_, _, _]], authContext)
}

object ToListQuery extends Query {
  override val name: String = "toList"

  override def checkFrom(t: ru.Type): Boolean = t <:< ru.typeOf[ScalliSteps[_, _, _]] || t <:< ru.typeOf[GremlinScala[_]]

  override def toType(t: ru.Type): ru.Type = {
    val subType = if (t <:< ru.typeOf[ScalliSteps[_, _, _]]) {
      RichType.getTypeArgs(t, ru.typeOf[ScalliSteps[_, _, _]]).head
    } else if (t <:< ru.typeOf[GremlinScala[_]]) {
      RichType.getTypeArgs(t, ru.typeOf[GremlinScala[_]]).head
    } else {
      throw BadRequestError(s"toList can't be used with $t. It must be a ScalliSteps or a GremlinScala")
    }
    ru.appliedType(ru.typeOf[List[_]].typeConstructor, subType)
  }

  override def apply(param: Unit, from: Any, authContext: AuthContext): Any = from match {
    case f: ScalliSteps[_, _, _] ⇒ f.toList()
    case f: GremlinScala[_]      ⇒ f.toList
  }
}

trait InputQuery {
  def apply[S <: ScalliSteps[_, _, _]](publicProperties: List[PublicProperty[_, _]], stepType: ru.Type, step: S, authContext: AuthContext): S

  def getProperty(properties: Seq[PublicProperty[_, _]], stepType: ru.Type, fieldName: String): PublicProperty[_, _] =
    properties
      .find(p ⇒ p.stepType =:= stepType && p.propertyName == fieldName)
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $stepType not found"))
}
