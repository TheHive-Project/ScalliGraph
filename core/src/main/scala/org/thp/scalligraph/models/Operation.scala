package org.thp.scalligraph.models

import gremlin.scala._
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.steps.Traversal
import play.api.Logger

import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Success, Try}

sealed trait Operation

case class AddProperty(model: String, propertyName: String, mapping: Mapping[_, _, _])                 extends Operation
case class RemoveProperty(model: String, propertyName: String, usedOnlyByThisModel: Boolean)           extends Operation
case class UpdateGraph(model: String, update: Traversal[Vertex, Vertex] => Try[Unit], comment: String) extends Operation
case class AddIndex(model: String, indexType: IndexType.Value, properties: Seq[String])                extends Operation
case class DBOperation[DB <: Database: ClassTag](comment: String, op: DB => Try[Unit]) extends Operation {
  def apply(db: Database): Try[Unit] =
    if (classTag[DB].runtimeClass.isAssignableFrom(db.getClass))
      op(db.asInstanceOf[DB])
    else
      Success(())
}

object Operations {
  def apply(schemaName: String, schema: Schema): Operations = new Operations(schemaName, schema, Nil)
}

case class Operations private (schemaName: String, schema: Schema, operations: Seq[Operation]) {
  lazy val logger: Logger                               = Logger(getClass)
  private def addOperations(op: Operation*): Operations = copy(operations = operations ++ op)
  def addProperty[T](model: String, propertyName: String)(implicit mapping: UniMapping[T]): Operations =
    addOperations(AddProperty(model, propertyName, mapping.toMapping))
  def removeProperty[T](model: String, propertyName: String, usedOnlyByThisModel: Boolean): Operations =
    addOperations(RemoveProperty(model, propertyName, usedOnlyByThisModel))
  def updateGraph(comment: String, model: String)(update: Traversal[Vertex, Vertex] => Try[Unit]): Operations =
    addOperations(UpdateGraph(model, update, comment))
  def addIndex(model: String, indexType: IndexType.Value, properties: String*): Operations =
    addOperations(AddIndex(model, indexType, properties))
  def dbOperation[DB <: Database: ClassTag](comment: String)(op: DB => Try[Unit]): Operations =
    addOperations(DBOperation[DB](comment, op))

  def execute(db: Database)(implicit authContext: AuthContext): Try[Unit] =
    db.version(schemaName) match {
      case 0 =>
        logger.info("Create database schema")
        db.createSchemaFrom(schema)
      case version =>
        operations.zipWithIndex.foldLeft[Try[Unit]](Success(())) {
          case (Success(_), (ops, v)) if v >= version =>
            (ops match {
              case AddProperty(model, propertyName, mapping) =>
                logger.info(s"Add property $propertyName to $model")
                db.addProperty(model, propertyName, mapping)
              case RemoveProperty(model, propertyName, usedOnlyByThisModel) =>
                logger.info(s"Remove property $propertyName from $model")
                db.removeProperty(model, propertyName, usedOnlyByThisModel)
              case UpdateGraph(model, update, comment) =>
                logger.info(s"Update graph: $comment")
                db.tryTransaction(graph => update(Traversal(db.labelFilter(model)(graph.V))))
                  .recoverWith { case error => Failure(InternalError(s"Unable to execute migration operation: $comment", error)) }
              case AddIndex(model, indexType, properties) =>
                logger.info(s"Add index in $model for properties: ${properties.mkString(", ")}")
                db.addIndex(model, indexType, properties)
              case dbOperation: DBOperation[_] =>
                logger.info(s"Update database: ${dbOperation.comment}")
                dbOperation(db)
            }).flatMap(_ => db.setVersion(schemaName, v))
          case (acc, _) => acc
        }
    }
}
