package org.thp.scalligraph.query

import gremlin.scala.Graph
import org.scalactic.Good
import org.thp.scalligraph.BadRequestError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.Database
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{Converter, PagedResult, Traversal, UntypedTraversal}

import scala.reflect.runtime.{currentMirror => rm, universe => ru}

// Use global lock because scala reflection subtype operator <:< is not thread safe (scala/bug#10766)
// https://stackoverflow.com/questions/56854716/inconsistent-result-when-checking-type-subtype
object SubType {
  def apply(t1: ru.Type, t2: ru.Type): Boolean = synchronized(t1 <:< t2)
}

abstract class ParamQuery[P: ru.TypeTag] { q =>
  val paramType: ru.Type = ru.typeOf[P]
  def paramParser(tpe: ru.Type): FieldsParser[P]
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
  override def paramParser(tpe: ru.Type): FieldsParser[Unit] = FieldsParser[Unit]("unit") {
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
      override def paramParser(tpe: ru.Type): FieldsParser[P]                = parser
      override val name: String                                              = queryName
      override def checkFrom(t: ru.Type): Boolean                            = SubType(t, ru.typeOf[Graph])
      override def toType(t: ru.Type): ru.Type                               = ru.typeOf[T]
      override def apply(param: P, from: Any, authContext: AuthContext): Any = f(param, from.asInstanceOf[Graph], authContext)
    }

  def apply[F: ru.TypeTag, T: ru.TypeTag](queryName: String, f: (F, AuthContext) => T): Query =
    new Query {
      override val name: String                                                 = queryName
      override def checkFrom(t: ru.Type): Boolean                               = SubType(t, ru.typeOf[F])
      override def toType(t: ru.Type): ru.Type                                  = ru.typeOf[T]
      override def apply(param: Unit, from: Any, authContext: AuthContext): Any = f(from.asInstanceOf[F], authContext)
    }

  def withParam[P: ru.TypeTag, F: ru.TypeTag, T: ru.TypeTag](queryName: String, parser: FieldsParser[P], f: (P, F, AuthContext) => T): ParamQuery[P] =
    new ParamQuery[P] {
      override def paramParser(tpe: ru.Type): FieldsParser[P]                = parser
      override val name: String                                              = queryName
      override def checkFrom(t: ru.Type): Boolean                            = SubType(t, ru.typeOf[F])
      override def toType(t: ru.Type): ru.Type                               = ru.typeOf[T]
      override def apply(param: P, from: Any, authContext: AuthContext): Any = f(param, from.asInstanceOf[F], authContext)
    }

  def output[E: Renderer: ru.TypeTag]: Query = new Query {
    override val name: String                                                 = "output"
    override def checkFrom(t: ru.Type): Boolean                               = SubType(t, ru.typeOf[E])
    override def toType(t: ru.Type): ru.Type                                  = ru.typeOf[Output[_]]
    override def apply(param: Unit, from: Any, authContext: AuthContext): Any = implicitly[Renderer[E]].toOutput(from.asInstanceOf[E])
  }

  def output[E: Renderer: ru.TypeTag, F <: Traversal[E, _]: ru.TypeTag]: Query = output(identity[F])

  def outputWithContext[E: ru.TypeTag, F: ru.TypeTag](transform: (F, AuthContext) => Traversal[E, _])(implicit renderer: Renderer[E]): Query =
    new Query {
      override val name: String                   = "output"
      override def checkFrom(t: ru.Type): Boolean = SubType(t, ru.typeOf[F])
      override def toType(t: ru.Type): ru.Type    = ru.appliedType(ru.typeOf[PagedResult[_]].typeConstructor, ru.typeOf[E])
      override def apply(param: Unit, from: Any, authContext: AuthContext): Any =
        PagedResult(transform(from.asInstanceOf[F], authContext), None)(renderer)
    }
  def output[E: ru.TypeTag, F: ru.TypeTag](transform: F => Traversal[E, _])(implicit renderer: Renderer[E]): Query =
    new Query {
      override val name: String                   = "output"
      override def checkFrom(t: ru.Type): Boolean = SubType(t, ru.typeOf[F])
      override def toType(t: ru.Type): ru.Type    = ru.appliedType(ru.typeOf[PagedResult[_]].typeConstructor, ru.typeOf[E])
      override def apply(param: Unit, from: Any, authContext: AuthContext): Any =
        PagedResult(transform(from.asInstanceOf[F]), None)(renderer)
    }
}

class SortQuery(db: Database, publicProperties: List[PublicProperty[_, _]]) extends ParamQuery[InputSort] {
  override def paramParser(tpe: ru.Type): FieldsParser[InputSort] = InputSort.fieldsParser
  override val name: String                                       = "sort"
  override def checkFrom(t: ru.Type): Boolean                     = SubType(t, ru.typeOf[UntypedTraversal])
  override def toType(t: ru.Type): ru.Type                        = t
  override def apply(inputSort: InputSort, from: Any, authContext: AuthContext): Any =
    inputSort(
      db,
      publicProperties,
      rm.classSymbol(from.getClass).toType,
      from.asInstanceOf[UntypedTraversal],
      authContext
    )
}

object FilterQuery {
  def apply(db: Database, publicProperties: List[PublicProperty[_, _]])(
      fieldsParser: (ru.Type, ru.Type => FieldsParser[InputFilter]) => FieldsParser[InputFilter]
  ): FilterQuery = new FilterQuery(db, publicProperties, fieldsParser :: Nil)
  def default(db: Database, publicProperties: List[PublicProperty[_, _]]): FilterQuery =
    apply(db, publicProperties)(InputFilter.fieldsParser(_, publicProperties, _))
  def empty(db: Database, publicProperties: List[PublicProperty[_, _]]) = new FilterQuery(db, publicProperties)
}

final class FilterQuery(
    db: Database,
    publicProperties: List[PublicProperty[_, _]],
    protected val fieldsParsers: List[(ru.Type, ru.Type => FieldsParser[InputFilter]) => FieldsParser[InputFilter]] = Nil
) extends ParamQuery[InputFilter] { filterQuery =>
  def paramParser(tpe: ru.Type): FieldsParser[InputFilter] =
    fieldsParsers.foldLeft(FieldsParser.empty[InputFilter])((fp, f) => fp orElse f(tpe, t => paramParser(t)))
  override val name: String                   = "filter"
  override def checkFrom(t: ru.Type): Boolean = SubType(t, ru.typeOf[UntypedTraversal])
  override def toType(t: ru.Type): ru.Type    = t
  override def apply(inputFilter: InputFilter, from: Any, authContext: AuthContext): Any =
    inputFilter(
      db,
      publicProperties,
      rm.classSymbol(from.getClass).toType,
      from.asInstanceOf[UntypedTraversal], //.asInstanceOf[X forSome { type X <: BaseVertexSteps[_, X] }],
      authContext
    )
//  def addParser(parser: (ru.Type, () => FieldsParser[InputFilter])): FilterQuery = new FilterQuery(db, publicProperties, parser :: fieldsParsers)
  def ++(other: FilterQuery): FilterQuery = new FilterQuery(db, publicProperties, filterQuery.fieldsParsers ::: other.fieldsParsers)
}

class AggregationQuery(publicProperties: List[PublicProperty[_, _]]) extends ParamQuery[GroupAggregation[_]] {
  override def paramParser(tpe: ru.Type): FieldsParser[GroupAggregation[_]] = GroupAggregation.fieldsParser
  override val name: String                                                 = "aggregation"
  override def checkFrom(t: ru.Type): Boolean                               = SubType(t, ru.typeOf[UntypedTraversal])
  override def toType(t: ru.Type): ru.Type                                  = ru.typeOf[Output[_]]
  override def apply(aggregation: GroupAggregation[_], from: Any, authContext: AuthContext): Any =
    aggregation.get(
      publicProperties,
      rm.classSymbol(from.getClass).toType,
      from.asInstanceOf[UntypedTraversal],
      authContext
    )
}

object CountQuery extends Query {
  override val name: String                                                  = "count"
  override def checkFrom(t: ru.Type): Boolean                                = SubType(t, ru.typeOf[UntypedTraversal])
  override def toType(t: ru.Type): ru.Type                                   = ru.typeOf[Long]
  override def apply(param: Unit, from: Any, authContext: AuthContext): Long = from.asInstanceOf[UntypedTraversal].getCount
}

trait InputQuery[PD, PG, PC <: Converter[PD, PG]] {
  def getProperty(
      properties: Seq[PublicProperty[_, _, _]],
      traversalType: ru.Type,
      fieldName: String,
      authContext: AuthContext
  ): PublicProperty[PD, PG, PC] = {
    val path = FPath(fieldName)
    properties
      .iterator
      .collectFirst {
        case p if p.traversalType =:= traversalType && path.startsWith(p.propertyPath).isDefined => p.asInstanceOf[PublicProperty[PD, PG, PC]]
      }
      .getOrElse(throw BadRequestError(s"Property $fieldName for type $traversalType not found"))
  }

  def apply[D, G, C <: Converter[D, G]](
      db: Database,
      publicProperties: List[PublicProperty[_, _, _]],
      traversalType: ru.Type,
      traversal: Traversal[D, G, C],
      authContext: AuthContext
  ): Traversal[D, G, C]
}
