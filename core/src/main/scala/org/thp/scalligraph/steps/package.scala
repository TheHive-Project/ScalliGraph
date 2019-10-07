package org.thp.scalligraph

import java.util.Date

import scala.util.{Failure, Try}

import play.api.Logger
import play.api.libs.json.JsObject

import gremlin.scala.dsl.Converter
import gremlin.scala.{GremlinScala, Vertex}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models._
import org.thp.scalligraph.query.PropertyUpdater

package object steps extends TraversalOps {

  lazy val logger = Logger(classOf[Traversal[_, _]])

  implicit class BaseVertexStepsOps[S <: BaseVertexSteps](val steps: S) {
    type EndDomain = steps.EndDomain
    def raw: GremlinScala[Vertex]                             = steps.raw.asInstanceOf[GremlinScala[Vertex]]
    def converter: Converter.Aux[EndDomain, Vertex]           = steps.converter
    private def newInstance0(newRaw: GremlinScala[Vertex]): S = steps.newInstance(newRaw).asInstanceOf[S]

    def getByIds(ids: String*): S = newInstance0(raw.hasId(ids: _*))

    def get(vertex: Vertex): S = newInstance0(raw.hasId(vertex.id()))

    def get(entity: Entity): S = newInstance0(raw.hasId(entity._id))

    private[scalligraph] def updateProperties(
        propertyUpdaters: Seq[PropertyUpdater]
    )(implicit authContext: AuthContext): Try[(S, JsObject)] = {
      val myClone = newInstance0(raw.clone())
      logger.debug(s"Execution of $raw")
      raw.headOption().fold[Try[(S, JsObject)]](Failure(NotFoundError(s"${steps.typeName} not found"))) { vertex =>
        logger.trace(s"Update ${vertex.id()} by ${authContext.userId}")
        val db = steps.db
        propertyUpdaters
          .toTry(u => u(vertex, db, steps.graph, authContext))
          .map { o =>
            db.setOptionProperty(vertex, "_updatedAt", Some(new Date), db.updatedAtMapping)
            db.setOptionProperty(vertex, "_updatedBy", Some(authContext.userId), db.updatedByMapping)
            myClone -> o.reduceOption(_ ++ _).getOrElse(JsObject.empty)
          }
      }
    }

    def _id                              = new Traversal[String, AnyRef](raw.id(), IdMapping)
    def label: Traversal[String, String] = new Traversal(raw.label(), UniMapping.string)

    def property[DD, GG](name: String, mapping: Mapping[_, DD, GG]): Traversal[DD, GG] =
      new Traversal[DD, GG](raw.values[GG](name), mapping)
    def _createdBy: Traversal[String, String] = property("_createdBy", org.thp.scalligraph.models.UniMapping.string)
    def _createdAt: Traversal[Date, Date]     = property("_createdAt", org.thp.scalligraph.models.UniMapping.date)
    def _updatedBy: Traversal[String, String] = property("_updatedBy", org.thp.scalligraph.models.UniMapping.string)
    def _updatedAt: Traversal[Date, Date]     = property("_updatedAt", org.thp.scalligraph.models.UniMapping.date)

  }

  implicit class VertexStepsOps[E <: Product](val steps: VertexSteps[E]) {

    def update(fields: (String, Any)*)(implicit authContext: AuthContext): Try[E with Entity] =
      steps.db.update[E](steps.raw, fields, steps.model, steps.graph, authContext)
  }

  implicit class EdgeStepsOps[E <: Product](val steps: EdgeSteps[E, _, _]) {

    def update(fields: (String, Any)*)(implicit authContext: AuthContext): Try[E with Entity] =
      steps.db.update[E](steps.raw, fields, steps.model, steps.graph, authContext)
  }
}
