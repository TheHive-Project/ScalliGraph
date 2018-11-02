package org.thp.scalligraph.query

import gremlin.scala.{Element, Graph}
import org.scalactic._
import org.thp.scalligraph._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers._
import play.api.libs.json.{JsNull, Json, OWrites, Writes}

import scala.reflect.runtime.{universe ⇒ ru}

abstract class QueryExecutor { executor ⇒
  val version: (Int, Int)                                     = 1 → 1
  val publicProperties: List[PublicProperty[_ <: Element, _]] = Nil
  val queries: Seq[ParamQuery[_]]                             = Nil

  final lazy val allQueries = queries :+ sortQuery :+ filterQuery :+ ToListQuery
  lazy val sortQuery        = new SortQuery(publicProperties)
  lazy val filterQuery      = new FilterQuery(publicProperties)

  def versionCheck(v: Int): Boolean = version._1 <= v && v <= version._2

  def execute(q: Query)(implicit authGraph: AuthGraph): Output[_] =
    execute(q, authGraph.graph, authGraph.auth)

  def execute(q: Query, graph: Graph, authContext: Option[AuthContext]): Output[_] = {
    val outputType  = q.toType(ru.typeOf[Graph])
    val outputValue = q((), graph, authContext)
    toOutput(outputValue, outputType, authContext)
  }

  private def toOutput(value: Any, tpe: ru.Type, authContext: Option[AuthContext]): Output[_] =
    value match {
      case o: Output[_] ⇒ o
      case s: Seq[o] ⇒
        val subType = RichType.getTypeArgs(tpe, ru.typeOf[Seq[_]]).head
        val writes  = OWrites[Seq[o]](x ⇒ Json.obj("result" → x.map(toOutput(_, subType, authContext).toJson)))
        new Output[Seq[o]](s)(writes)
      case s: Option[o] ⇒
        val subType = RichType.getTypeArgs(tpe, ru.typeOf[Option[_]]).head
        val writes = Writes[Option[o]] {
          case None    ⇒ JsNull
          case Some(x) ⇒ toOutput(x, subType, authContext).toJson
        }
        new Output[None.type](None)(OWrites[None.type](_ ⇒ Json.obj("result" → JsNull)))
      case o ⇒
        allQueries
          .find(q ⇒ q.checkFrom(tpe) && q.toType(tpe) <:< ru.typeOf[Output[_]] && q.paramType == ru.typeOf[Unit])
          .map(q ⇒ q.asInstanceOf[Query]((), value, authContext))
          .getOrElse {
            throw BadRequestError(s"Value of type $tpe can't be output")
          }
          .asInstanceOf[Output[_]]
    }

  private def getQuery(tpe: ru.Type, field: Field): Or[Query, Every[AttributeError]] = {
    def applyQuery[P](query: ParamQuery[P], from: Field): Or[Query, Every[AttributeError]] =
      if (query.checkFrom(tpe)) {
        query
          .paramParser(from)
          .map(p ⇒ query.toQuery(p))
      } else Bad(One(InvalidFormatAttributeError("_name", "query", allQueries.filter(_.checkFrom(tpe)).map(_.name), field)))

    field match {
      case FNamedObj(name, f) ⇒
        val potentialQueries = allQueries
          .filter(_.name == name)
          .map(q ⇒ applyQuery(q, f))
        potentialQueries
          .find(_.isGood)
          .getOrElse {
            potentialQueries
              .collect { case Bad(x) ⇒ x }
              .reduceOption(_ ++ _)
              .map(Bad(_))
              .getOrElse(Bad(One(InvalidFormatAttributeError("_name", "query", allQueries.filter(_.checkFrom(tpe)).map(_.name), field))))
          }
      case _ ⇒ Bad(One(InvalidFormatAttributeError("_name", "query", allQueries.filter(_.checkFrom(tpe)).map(_.name), field)))
    }
  }

  def parser: FieldsParser[Query] = FieldsParser[Query]("query") {
    case (_, FSeq(fields)) ⇒
      val initQuery = getQuery(ru.typeOf[Graph], fields.head)
      fields.tail
        .foldLeft(initQuery.map(q ⇒ q.toType(ru.typeOf[Graph]) → q)) {
          case (Good((tpe, query)), field) ⇒ getQuery(tpe, field).map(q ⇒ q.toType(tpe) → query.andThen(q))
          case (b: Bad[_], _)              ⇒ b
        }
        .map(_._2)
  }

  def ++(other: QueryExecutor): QueryExecutor = new QueryExecutor {
    override val version: (Int, Int) = math.max(executor.version._1, other.version._1) → math.min(executor.version._2, other.version._2)
    override val publicProperties: List[PublicProperty[_ <: Element, _]] =
      (executor.publicProperties :: other.publicProperties).asInstanceOf[List[PublicProperty[_ <: Element, _]]]
    override val queries: Seq[ParamQuery[_]] = executor.queries ++ other.queries
  }
}