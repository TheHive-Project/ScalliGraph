package org.thp.scalligraph.services

import org.apache.tinkerpop.gremlin.structure.{Element, Graph}
import org.thp.scalligraph.models.{Entity, Model}
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Traversal}
import play.api.Logger

abstract class ElementSrv[E <: Product, G <: Element] {
  lazy val logger: Logger = Logger(getClass)

  val model: Model.Base[E]

  def startTraversal(implicit graph: Graph): Traversal[E with Entity, G, Converter[E with Entity, G]]
  def filterTraversal(traversal: Traversal[G, G, Converter.Identity[G]]): Traversal[E with Entity, G, Converter[E with Entity, G]]

  def get(idOrName: String)(implicit graph: Graph): Traversal[E with Entity, G, Converter[E with Entity, G]] = getByIds(idOrName)

  def getByIds(ids: String*)(implicit graph: Graph): Traversal[E with Entity, G, Converter[E with Entity, G]]

  def get(e: Entity)(implicit graph: Graph): Traversal[E with Entity, G, Converter[E with Entity, G]] = getByIds(e._id)

  def count(implicit graph: Graph): Long = startTraversal.getCount
}
