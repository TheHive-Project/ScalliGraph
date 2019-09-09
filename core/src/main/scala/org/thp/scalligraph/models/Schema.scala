package org.thp.scalligraph.models

import scala.collection.immutable
import scala.util.{Success, Try}

import gremlin.scala.Graph
import javax.inject.{Inject, Provider, Singleton}
import org.thp.scalligraph.auth.AuthContext

case class InitialValue[V <: Product](model: Model.Vertex[V], value: V) {

  def create()(implicit db: Database, graph: Graph, authContext: AuthContext): V with Entity =
    db.createVertex[V](graph, authContext, model, value)
}

trait Schema { schema =>
  def modelList: Seq[Model]
  def initialValues: Seq[InitialValue[_]]                              = Nil
  final def getModel(label: String): Option[Model]                     = modelList.find(_.label == label)
  def init(implicit graph: Graph, authContext: AuthContext): Try[Unit] = Success(())

  def +(other: Schema): Schema = new Schema {
    override def modelList: Seq[Model]                                            = schema.modelList ++ other.modelList
    override def initialValues: Seq[InitialValue[_]]                              = schema.initialValues ++ other.initialValues
    override def init(implicit graph: Graph, authContext: AuthContext): Try[Unit] = schema.init.flatMap(_ => other.init)
  }
}

object Schema {

  def empty: Schema = new Schema {
    override def modelList: Seq[Model] = Nil
  }
}

@Singleton
class GlobalSchema @Inject()(schemas: immutable.Set[Schema]) extends Provider[Schema] {
  override def get(): Schema = schemas.reduceOption(_ + _).getOrElse(Schema.empty)
}
