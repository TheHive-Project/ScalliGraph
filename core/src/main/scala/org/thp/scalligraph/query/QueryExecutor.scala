package org.thp.scalligraph.query

import scala.reflect.runtime.{universe => ru}

import play.api.Logger
import play.api.libs.json.{JsNull, Json}

import gremlin.scala.Graph
import org.scalactic._
import org.thp.scalligraph._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.PagedResult

abstract class QueryExecutor { executor =>
  val version: (Int, Int)
  lazy val publicProperties: List[PublicProperty[_, _]] = Nil
  lazy val queries: Seq[ParamQuery[_]]                  = Nil
  lazy val logger                                       = Logger(getClass)

  final lazy val allQueries = queries :+ sortQuery :+ filterQuery :+ aggregationQuery :+ ToListQuery
  lazy val sortQuery        = new SortQuery(publicProperties)
  lazy val filterQuery      = new FilterQuery(publicProperties)
  lazy val aggregationQuery = new AggregationQuery(publicProperties)

  def versionCheck(v: Int): Boolean = version._1 <= v && v <= version._2

  def execute(q: Query)(implicit authGraph: AuthGraph): Output[_] =
    execute(q, authGraph.graph, authGraph.auth)

  def execute(q: Query, graph: Graph, authContext: AuthContext): Output[_] = {
    val outputType  = q.toType(ru.typeOf[Graph])
    val outputValue = q((), graph, authContext)
    toOutput(outputValue, outputType, authContext)
  }

  private def toOutput(value: Any, tpe: ru.Type, authContext: AuthContext): Output[_] =
    value match {
      case o: Output[_] => o
      case s: Seq[o] =>
        val subType = RichType.getTypeArgs(tpe, ru.typeOf[Seq[_]]).head
        val result  = s.map(x => toOutput(x, subType, authContext).toJson)
        Output(s, Json.obj("result" -> result))
      case s: Option[o] =>
        s.fold[Output[_]](Output(None, JsNull)) { v =>
          val subType = RichType.getTypeArgs(tpe, ru.typeOf[Option[_]]).head
          toOutput(v, subType, authContext)
        }
      case PagedResult(result, _) =>
        val subType = RichType.getTypeArgs(tpe, ru.typeOf[PagedResult[_]]).head
        val seqType = ru.appliedType(ru.typeOf[Seq[_]].typeConstructor, subType)
        val lOutput = toOutput(result, seqType, authContext)
        Output(value, lOutput.toJson)
      case _ =>
        allQueries
          .find(q => q.checkFrom(tpe) && q.toType(tpe) <:< ru.typeOf[Output[_]] && q.paramType == ru.typeOf[Unit])
          .map(q => q.asInstanceOf[Query]((), value, authContext))
          .getOrElse {
            throw BadRequestError(s"Value of type $tpe ($value) can't be output")
          }
          .asInstanceOf[Output[_]]
    }

  private def getQuery(tpe: ru.Type, field: Field): Or[Query, Every[AttributeError]] = {
    def applyQuery[P](query: ParamQuery[P], from: Field): Or[Query, Every[AttributeError]] =
      if (query.checkFrom(tpe)) { // FIXME why double check of type ?
        query
          .paramParser(tpe, publicProperties)(from)
          .map(p => query.toQuery(p))
      } else {
        logger.warn(s"getQuery($tpe, $field).applyQuery($query, $from) fails because $query.checkFrom($tpe) returns false")
        Bad(One(InvalidFormatAttributeError("_name", "query", allQueries.filter(_.checkFrom(tpe)).map(_.name).toSet, field)))
      }

    field match {
      case FNamedObj(name, f) =>
        val potentialQueries = allQueries
          .filter(q => q.name == name && q.checkFrom(tpe))
          .map(q => applyQuery(q, f))
        potentialQueries
          .find(_.isGood)
          .getOrElse {
            potentialQueries
              .collect { case Bad(x) => x }
              .reduceOption(_ ++ _)
              .map(Bad(_))
              .getOrElse {
                logger.warn(s"getQuery($tpe, $field) fails because query $name ($f) has potential queries $potentialQueries")
                Bad(One(InvalidFormatAttributeError("_name", "query", allQueries.filter(_.checkFrom(tpe)).map(_.name).toSet, field)))
              }
          }
      case _ =>
        logger.warn(s"getQuery($tpe, $field) fails because field has no name")
        Bad(One(InvalidFormatAttributeError("_name", "query", allQueries.filter(_.checkFrom(tpe)).map(_.name).toSet, field)))
    }
  }

  def parser: FieldsParser[Query] = FieldsParser[Query]("query") {
    case (_, FSeq(fields)) =>
      val initQuery = getQuery(ru.typeOf[Graph], fields.head)
      fields
        .tail
        .foldLeft(initQuery.map(q => q.toType(ru.typeOf[Graph]) -> q)) {
          case (Good((tpe, query)), field) => getQuery(tpe, field).map(q => q.toType(tpe) -> query.andThen(q))
          case (b: Bad[_], _)              => b
        }
        .map(_._2)
  }

  def ++(other: QueryExecutor): QueryExecutor = new QueryExecutor {
    override val version: (Int, Int) = math.max(executor.version._1, other.version._1) -> math.min(executor.version._2, other.version._2)
    override lazy val publicProperties: List[PublicProperty[_, _]] =
      (executor.publicProperties :: other.publicProperties).asInstanceOf[List[PublicProperty[_, _]]]
    override lazy val queries: Seq[ParamQuery[_]] = executor.queries ++ other.queries
  }
}
