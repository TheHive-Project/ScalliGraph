package org.thp.scalligraph.models

import org.thp.scalligraph.auth.AuthContext
import play.api.Logger

import javax.inject.{Inject, Provider, Singleton}
import scala.collection.immutable
import scala.util.Try

case class SchemaStatus(name: String, currentVersion: Int, expectedVersion: Int, error: Option[Throwable])

trait UpdatableSchema extends Schema {
  val authContext: AuthContext
  val operations: Operations
  lazy val name: String                           = operations.schemaName
  def schemaStatus: Option[SchemaStatus]          = _updateStatus
  private var _updateStatus: Option[SchemaStatus] = None
  def update(db: Database): Try[Unit] = {
    val result = operations.execute(db, this)(authContext)
    _updateStatus = Some(SchemaStatus(name, db.version(name), operations.operations.length + 1, result.fold(Some(_), _ => None)))
    result
  }
}

trait Schema { schema =>
  def modelList: Seq[Model]
  final def getModel(label: String): Option[Model] = modelList.find(_.label == label)

  def +(other: Schema): Schema =
    new Schema {
      override def modelList: Seq[Model] = schema.modelList ++ other.modelList
    }
}

object Schema {

  def empty: Schema =
    new Schema {
      override def modelList: Seq[Model] = Nil
    }
}

@Singleton
class GlobalSchema @Inject() (schemas: immutable.Set[UpdatableSchema]) extends Provider[Schema] {
  lazy val logger: Logger = Logger(getClass)
  lazy val schema: Schema = {
    logger.debug(s"Build global schema from ${schemas.map(_.getClass.getSimpleName).mkString("+")}")
    schemas.reduceOption[Schema](_ + _).getOrElse(Schema.empty)
  }
  override def get(): Schema = schema
}
