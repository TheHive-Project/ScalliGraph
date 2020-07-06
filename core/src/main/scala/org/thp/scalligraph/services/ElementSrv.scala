package org.thp.scalligraph.services

import gremlin.scala.{Element, Graph, Vertex}
import org.thp.scalligraph.models.{Entity, Model}
import org.thp.scalligraph.steps.{BaseElementSteps, Traversal}
import org.thp.scalligraph.steps.StepsOps._
import play.api.Logger

abstract class ElementSrv[E <: Product, G <: Element] {
  lazy val logger: Logger = Logger(getClass)

  val model: Model.Base[E]

  def initSteps(implicit graph: Graph): Traversal[E with Entity, G]

  def get(idOrName: String)(implicit graph: Graph): Traversal[E with Entity, G] = getByIds(idOrName)

  def getByIds(ids: String*)(implicit graph: Graph): Traversal[E with Entity, G]

  def get(e: Entity)(implicit graph: Graph): Traversal[E with Entity, G] = getByIds(e._id)

  def count(implicit graph: Graph): Long = initSteps.getCount
}
