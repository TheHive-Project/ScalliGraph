package org.thp.scalligraph.query

import gremlin.scala.{Graph, GremlinScala}
import org.scalactic._
import org.thp.scalligraph._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import org.thp.scalligraph.models.Database
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{PagedResult, Traversal, TraversalLike}
import org.thp.scalligraph.utils.RichType
import play.api.Logger
import play.api.libs.json.{JsArray, JsNull, JsNumber, JsString, JsValue}
import play.api.mvc.{Result, Results}

import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

abstract class QueryExecutor { executor =>
  val version: (Int, Int)
  lazy val publicProperties: List[PublicProperty[_, _]] = Nil
  lazy val queries: Seq[ParamQuery[_]]                  = Nil
  lazy val logger: Logger                               = Logger(getClass)
  val db: Database

  final lazy val allQueries                         = queries :+ sortQuery :+ filterQuery :+ aggregationQuery :+ CountQuery
  final lazy val sortQuery: SortQuery               = new SortQuery(db, publicProperties)
  final lazy val aggregationQuery: AggregationQuery = new AggregationQuery(db, publicProperties, filterQuery)
  final lazy val filterQuery: FilterQuery           = FilterQuery.default(db, publicProperties) ++ customFilterQuery
  val customFilterQuery: FilterQuery                = FilterQuery.empty(db, publicProperties)

  def versionCheck(v: Int): Boolean = version._1 <= v && v <= version._2

  final def execute(query: Query, authContext: AuthContext): Try[Result] = {
    val outputType = query.toType(ru.typeOf[Graph])
    getRenderer(outputType, authContext).map {
      case renderer if renderer.isStreamable =>
        val (source, totalSize) = db.source[JsValue, Option[Long]] { graph =>
          val pagedResult = renderer.toOutput(query((), graph, authContext)).asInstanceOf[PagedResult[Any]]
          pagedResult.result.toIterator.map(pagedResult.subRenderer.toJson) -> pagedResult.totalSize.map(_.head())
        }
        val result = Results.Ok.chunked(source.map(_.toString).intersperse("[", ",", "]"), Some("application/json"))
        totalSize.fold(result)(s => result.withHeaders("X-Total" -> s.toString))
      case renderer =>
        db.roTransaction { g =>
          val value = query((), g, authContext)
          Results.Ok(renderer.toJson(value))
        }
    }
  }

  final def execute(q: Query)(implicit authGraph: AuthGraph): Try[Output[_]] =
    execute(q, authGraph.graph, authGraph.auth)

  final def execute(q: Query, graph: Graph, authContext: AuthContext): Try[Output[_]] = {
    val outputType  = q.toType(ru.typeOf[Graph])
    val outputValue = q((), graph, authContext)
    getRenderer(outputType, authContext).map(_.toOutput(outputValue))
  }

  private def getRenderer(tpe: ru.Type, authContext: AuthContext): Try[Renderer[Any]] =
    if (SubType(tpe, ru.typeOf[PagedResult[_]])) Success(Renderer.stream[Any](_.asInstanceOf[PagedResult[Any]]))
    else if (SubType(tpe, ru.typeOf[Output[_]])) Success(Renderer[Any](_.asInstanceOf[Output[Any]]))
    else if (SubType(tpe, ru.typeOf[AnyVal])) Success(Renderer[Any] {
      case l: Long    => Output(l)
      case d: Double  => Output(d)
      case f: Float   => Output(f)
      case i: Int     => Output(i)
      case c: Char    => Output(c, JsString(c.toString))
      case s: Short   => Output(s)
      case b: Byte    => Output(b)
      case u: Unit    => Output(u, JsNull)
      case b: Boolean => Output(b)
    })
    else if (SubType(tpe, ru.typeOf[Number])) Success(Renderer(n => Output(n, JsNumber(BigDecimal(n.toString)))))
    else if (SubType(tpe, ru.typeOf[Seq[_]])) {
      val subType = RichType.getTypeArgs(tpe, ru.typeOf[Seq[_]]).head
      getRenderer(subType, authContext).map { subRenderer =>
        Renderer[Any] { value =>
          Output(value, JsArray(value.asInstanceOf[Seq[Any]].map(v => subRenderer.toJson(v))))
        }
      }
    } else if (SubType(tpe, ru.typeOf[Option[_]])) {
      val subType = RichType.getTypeArgs(tpe, ru.typeOf[Option[_]]).head
      getRenderer(subType, authContext).map { subRenderer =>
        Renderer[Any] { value =>
          value
            .asInstanceOf[Option[Any]]
            .fold[Output[_]](Output(None, JsNull))(v => subRenderer.toOutput(v))
        }
      }
    } else if (SubType(tpe, ru.typeOf[JsValue])) {
      Success(Renderer[Any](value => Output(value.asInstanceOf[JsValue])))
    } else
      allQueries
        .find(q => q.checkFrom(tpe) && SubType(q.toType(tpe), ru.typeOf[Output[_]]) && q.paramType == ru.typeOf[Unit])
        .fold[Try[Renderer[Any]]] {
          if (SubType(tpe, ru.typeOf[TraversalLike[_, _]])) {
            val subType = RichType.getTypeArgs(tpe, ru.typeOf[TraversalLike[_, _]]).head
            getRenderer(subType, authContext).map(subRenderer =>
              Renderer.stream[Any](traversal => PagedResult(traversal.asInstanceOf[TraversalLike[Any, _]], None)(subRenderer))
            )
          } else if (SubType(tpe, ru.typeOf[GremlinScala[_]])) {
            val subType = RichType.getTypeArgs(tpe, ru.typeOf[GremlinScala[_]]).head
            getRenderer(subType, authContext).map(subRenderer =>
              Renderer.stream[Any](traversal => PagedResult(Traversal(traversal.asInstanceOf[GremlinScala[Any]]), None)(subRenderer))
            )
          } else Failure(BadRequestError(s"Value of type $tpe can't be output"))
        } { q =>
          if (SubType(q.toType(tpe), ru.typeOf[PagedResult[_]]))
            Success(Renderer.stream(value => q.asInstanceOf[Query]((), value, authContext).asInstanceOf[PagedResult[Any]]))
          else
            Success(Renderer(value => q.asInstanceOf[Query]((), value, authContext).asInstanceOf[Output[Any]]))
        }

  private def getQuery(tpe: ru.Type, field: Field): Or[Query, Every[AttributeError]] = {
    def applyQuery[P](query: ParamQuery[P], from: Field): Or[Query, Every[AttributeError]] =
      if (query.checkFrom(tpe)) { // FIXME why double check of type ?
        query
          .paramParser(tpe)(from)
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

  final def parser: FieldsParser[Query] = FieldsParser[Query]("query") {
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
    override val db: Database                                      = other.db
    override val version: (Int, Int)                               = math.max(executor.version._1, other.version._1) -> math.min(executor.version._2, other.version._2)
    override lazy val publicProperties: List[PublicProperty[_, _]] = executor.publicProperties ::: other.publicProperties
    override lazy val queries: Seq[ParamQuery[_]]                  = executor.queries ++ other.queries
    override val customFilterQuery: FilterQuery                    = executor.customFilterQuery ++ other.customFilterQuery
  }
}
