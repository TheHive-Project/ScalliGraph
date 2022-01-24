package org.thp.scalligraph.models

import org.apache.tinkerpop.gremlin.structure.Vertex
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.traversal.Traversal
import play.api.Logger

import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Success, Try}

sealed trait Operation {
  def info: String
  def execute(db: Database, logger: String => Unit): Try[Unit]
}

case class AddVertexModel(label: String) extends Operation {
  override def info                                                     = s"Add vertex model $label to schema"
  override def execute(db: Database, logger: String => Unit): Try[Unit] = db.addVertexModel(label, Map.empty)
}

case class AddEdgeModel(label: String, mapping: Map[String, Mapping[_, _, _]]) extends Operation {
  override def info: String                                             = s"Add edge model $label to schema"
  override def execute(db: Database, logger: String => Unit): Try[Unit] = db.addEdgeModel(label, mapping)
}

case class AddProperty(model: String, propertyName: String, mapping: Mapping[_, _, _]) extends Operation {
  override def info: String                                             = s"Add property $propertyName to $model"
  override def execute(db: Database, logger: String => Unit): Try[Unit] = db.addProperty(model, propertyName, mapping)
}

case class RemoveProperty(model: String, propertyName: String, usedOnlyByThisModel: Boolean, mapping: Mapping[_, _, _]) extends Operation {
  override def info: String                                             = s"Remove property $propertyName from $model"
  override def execute(db: Database, logger: String => Unit): Try[Unit] = db.removeProperty(model, propertyName, usedOnlyByThisModel, mapping)
}

case class UpdateGraph(
    model: String,
    filter: Traversal.Identity[Vertex] => Traversal.Identity[Vertex],
    update: Traversal.Identity[Vertex] => Try[Unit],
    comment: String,
    pageSize: Long = 100
) extends Operation {
  override def info: String = s"Update graph: $comment"

  override def execute(db: Database, logger: String => Unit): Try[Unit] =
    db
      .pagedTraversal(pageSize.toInt, db.labelFilter(model, _))(update.andThen(Some(_)))
      .zipWithIndex
      .map {
        case (t, i) =>
          logger(s"Update graph in progress (${i * pageSize}): $comment")
          t
      }
      .find(_.isFailure)
      .fold[Try[Unit]](Success(()))(_.recoverWith {
        case error => Failure(InternalError(s"Unable to execute migration operation: $comment", error))
      })
}

case class AddIndex(model: String, indexType: IndexType.Value, properties: Seq[String]) extends Operation {
  override def info: String                                             = s"Add index in $model for properties: ${properties.mkString(", ")}"
  override def execute(db: Database, logger: String => Unit): Try[Unit] = db.addIndex(model, Seq(indexType -> properties))
}

object ReindexData extends Operation {
  override def info: String                                             = "Reindex all data"
  override def execute(db: Database, logger: String => Unit): Try[Unit] = Try(db.reindexData())
}

object NoOperation extends Operation {
  override def info: String                                             = "No operation"
  override def execute(db: Database, logger: String => Unit): Try[Unit] = Success(())
}

case class RemoveIndex(model: String, indexType: IndexType.Value, fields: Seq[String]) extends Operation {
  override def info: String                                             = s"Remove index $model:${fields.mkString(",")}"
  override def execute(db: Database, logger: String => Unit): Try[Unit] = db.removeIndex(model, indexType, fields)
}

case class DBOperation[DB <: Database: ClassTag](comment: String, op: DB => Try[Unit]) extends Operation {
  override def info: String = s"Update database: $comment"
  override def execute(db: Database, logger: String => Unit): Try[Unit] =
    if (classTag[DB].runtimeClass.isAssignableFrom(db.getClass)) op(db.asInstanceOf[DB])
    else Success(())
}

object Operations {
  def apply(schemaName: String): Operations = new Operations(schemaName, Nil)
}

case class Operations private (schemaName: String, operations: Seq[Operation]) {
  lazy val logger: Logger                               = Logger(getClass)
  val lastVersion: Int                                  = operations.length + 2
  private def addOperations(op: Operation*): Operations = copy(operations = operations ++ op)
  def addVertexModel[T](label: String): Operations =
    addOperations(AddVertexModel(label))
  def addEdgeModel[T](label: String, properties: Seq[String])(implicit mapping: UMapping[T]): Operations =
    addOperations(AddEdgeModel(label, properties.map(p => p -> mapping.toMapping).toMap))
  def addProperty[T](model: String, propertyName: String)(implicit mapping: UMapping[T]): Operations =
    addOperations(AddProperty(model, propertyName, mapping.toMapping))
  def removeProperty[T](model: String, propertyName: String, usedOnlyByThisModel: Boolean)(implicit mapping: UMapping[T]): Operations =
    addOperations(RemoveProperty(model, propertyName, usedOnlyByThisModel, mapping.toMapping))
  def updateGraph(comment: String, model: String, filter: Traversal.Identity[Vertex] => Traversal.Identity[Vertex] = identity)(
      update: Traversal.Identity[Vertex] => Try[Unit]
  ): Operations =
    addOperations(UpdateGraph(model, filter, update, comment))
  def addIndex(model: String, indexType: IndexType.Value, properties: String*): Operations =
    addOperations(AddIndex(model, indexType, properties))
  def dbOperation[DB <: Database: ClassTag](comment: String)(op: DB => Try[Unit]): Operations =
    addOperations(DBOperation[DB](comment, op))
  def noop: Operations                                                                    = addOperations(NoOperation)
  def reindexData: Operations                                                             = addOperations(ReindexData)
  def removeIndex(model: String, indexType: IndexType.Value, fields: String*): Operations = addOperations(RemoveIndex(model, indexType, fields))

  def execute(db: Database, schema: Schema)(implicit authContext: AuthContext): Try[Unit] =
    db.version(schemaName) match {
      case 0 =>
        logger.info(s"*** UPDATE SCHEMA OF $schemaName (${operations.length + 1}): Create database schema")
        db.createSchemaFrom(schema)
          .flatMap(_ => db.setVersion(schemaName, operations.length + 1))
      case version =>
        operations.zipWithIndex.foldLeft[Try[Unit]](Success(())) {
          case (Success(_), (ops, v)) if v + 1 >= version =>
            logger.info(s"*** UPDATE SCHEMA OF $schemaName (${v + 1}): ${ops.info}")
            ops
              .execute(db, l => logger.info(l))
              .flatMap(_ => db.setVersion(schemaName, v + 2))
          case (acc, _) => acc
        }
    }
}
