package org.thp.scalligraph.services

import gremlin.scala.Graph
import org.thp.scalligraph.models.{Entity, Model}
import org.thp.scalligraph.steps.BaseElementSteps
import org.thp.scalligraph.steps.StepsOps._
import play.api.Logger

abstract class ElementSrv[E <: Product, S <: BaseElementSteps] {
  lazy val logger: Logger = Logger(getClass)

  val model: Model.Base[E]

  def initSteps(implicit graph: Graph): S

  def get(idOrName: String)(implicit graph: Graph): S = getByIds(idOrName)

  def getByIds(ids: String*)(implicit graph: Graph): S

  def get(e: Entity)(implicit graph: Graph): S = getByIds(e._id)

  def count(implicit graph: Graph): Long = initSteps.getCount
}
