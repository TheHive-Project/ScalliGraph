package org.thp.scalligraph.models

import gremlin.scala.Graph
import javax.inject.{Inject, Provider, Singleton}
import org.thp.scalligraph.auth.AuthContext
import play.api.Logger

import scala.collection.immutable
import scala.util.{Success, Try}

case class InitialValue[V <: Product](model: Model.Vertex[V], value: V) {

  def create()(implicit db: Database, graph: Graph, authContext: AuthContext): V with Entity =
    db.createVertex[V](graph, authContext, model, value)
}

trait UpdatableSchema { _: Schema =>
  val operations: Operations
  def update(db: Database)(implicit authContext: AuthContext): Try[Unit] = operations.execute(db, this)
}

trait Schema { schema =>
  def modelList: Seq[Model]
  def initialValues: Seq[InitialValue[_]]                                            = Nil
  final def getModel(label: String): Option[Model]                                   = modelList.find(_.label == label)
  def init(db: Database)(implicit graph: Graph, authContext: AuthContext): Try[Unit] = Success(())

  def +(other: Schema): Schema = new Schema {
    override def modelList: Seq[Model]                                                          = schema.modelList ++ other.modelList
    override def initialValues: Seq[InitialValue[_]]                                            = schema.initialValues ++ other.initialValues
    override def init(db: Database)(implicit graph: Graph, authContext: AuthContext): Try[Unit] = schema.init(db).flatMap(_ => other.init(db))
  }
}

object Schema {

  def empty: Schema = new Schema {
    override def modelList: Seq[Model] = Nil
  }
}

@Singleton
class GlobalSchema @Inject() (schemas: immutable.Set[Schema]) extends Provider[Schema] {
  lazy val logger: Logger = Logger(getClass)
  lazy val schema: Schema = {
    logger.debug(s"Build global schema from ${schemas.map(_.getClass.getSimpleName).mkString("+")}")
    schemas.reduceOption(_ + _).getOrElse(Schema.empty)
  }
  override def get(): Schema = schema
}
