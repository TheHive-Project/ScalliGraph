package org.thp.scalligraph.traversal

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.{Edge, Transaction, Vertex, Graph => TinkerGraph}
import org.thp.scalligraph.EntityId
import org.thp.scalligraph.models.{Database, Model}

trait Graph {
  def underlying: TinkerGraph
  def addVertex(label: String): Vertex
  def tx(): Transaction
  def traversal(): GraphTraversalSource
  def variables: TinkerGraph.Variables
  def db: Database
  def V[D <: Product](ids: EntityId*)(implicit model: Model.Vertex[D]): Traversal.V[D]        = db.V[D](ids: _*)(model, this)
  def V(label: String, ids: EntityId*): Traversal[Vertex, Vertex, Converter.Identity[Vertex]] = db.V(label, ids: _*)(this)
  def E[D <: Product](ids: EntityId*)(implicit model: Model.Edge[D]): Traversal.E[D]          = db.E[D](ids: _*)(model, this)
  def E(label: String, ids: EntityId*): Traversal[Edge, Edge, Converter.Identity[Edge]]       = db.E(label, ids: _*)(this)
  var printByteCode: Boolean                                                                  = false
  var printStrategies: Boolean                                                                = false
  var printExplain: Boolean                                                                   = false
  var printProfile: Boolean                                                                   = false
}

class GraphWrapper(override val db: Database, val underlying: TinkerGraph) extends Graph {
  printByteCode = db.printByteCode
  printStrategies = db.printStrategies
  printExplain = db.printExplain
  printProfile = db.printProfile
  override def addVertex(label: String): Vertex  = underlying.addVertex(label)
  override def tx(): Transaction                 = underlying.tx()
  override def traversal(): GraphTraversalSource = underlying.traversal()
  override val variables: TinkerGraph.Variables  = underlying.variables()
}

object AnonymousGraph extends Graph {
  override def underlying: TinkerGraph           = ???
  override def addVertex(label: String): Vertex  = ???
  override def tx(): Transaction                 = ???
  override def traversal(): GraphTraversalSource = ???
  override def variables: TinkerGraph.Variables  = ???
  override def db: Database                      = ???
}
