package org.thp.scalligraph.services

import scala.util.{Failure, Success, Try}

import gremlin.scala.Graph
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.models.{ElementSteps, Entity, Model}

abstract class ElementSrv[E <: Product, S <: ElementSteps[E, _, S]] {
  val model: Model.Base[E]

  def initSteps(implicit graph: Graph): S

  def get(idOrName: String)(implicit graph: Graph): S = getByIds(idOrName)

  def getByIds(ids: String*)(implicit graph: Graph): S

  def get(e: Entity)(implicit graph: Graph): S = getByIds(e._id)

  def getOrFail(id: String)(implicit graph: Graph): Try[E with Entity] =
    get(id).headOption().fold[Try[E with Entity]](Failure(NotFoundError(s"${model.label} $id not found")))(Success.apply)

  def count(implicit graph: Graph): Long = initSteps.count
}
