package org.thp.scalligraph.models

import org.apache.tinkerpop.gremlin.structure.Vertex
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.traversal.{Converter, Traversal}
import play.api.Logger

import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Success, Try}

sealed trait Operation

case class AddVertexModel(label: String, mapping: Map[String, Mapping[_, _, _]])
  extends Operation
case class AddEdgeModel(label: String, mapping: Map[String, Mapping[_, _, _]])
  extends Operation
case class AddProperty(model: String, propertyName: String, mapping: Mapping[_, _, _])
  extends Operation
case class RemoveProperty(model: String, propertyName: String, usedOnlyByThisModel: Boolean)
  extends Operation
case class UpdateGraph(model: String, update: Traversal[Vertex, Vertex, Converter.Identity[Vertex]] => Try[Unit], comment: String)
  extends Operation
case class AddVertices()
case class AddIndex(model: String, indexType: IndexType.Value, properties: Seq[String])
  extends Operation
object RebuildIndexes
  extends Operation

case class DBOperation[DB <: Database: ClassTag](comment: String, op: DB => Try[Unit]) extends Operation {
  def apply(db: Database): Try[Unit] =
    if (classTag[DB].runtimeClass.isAssignableFrom(db.getClass))
      op(db.asInstanceOf[DB])
    else
      Success(())
}
object NoOperation extends Operation

object Operations {
  def apply(schemaName: String): Operations = new Operations(schemaName, Nil)
}

case class Operations private (schemaName: String, operations: Seq[Operation]) {
  lazy val logger: Logger                               = Logger(getClass)
  val lastVersion: Int                                  = operations.length + 2
  private def addOperations(op: Operation*): Operations = copy(operations = operations ++ op)
  def addVertexModel[T](label: String, properties: Seq[String])(implicit mapping: UMapping[T]): Operations =
    addOperations(AddVertexModel(label, properties.map(p => p -> mapping.toMapping).toMap))
  def addEdgeModel[T](label: String, properties: Seq[String])(implicit mapping: UMapping[T]): Operations =
    addOperations(AddEdgeModel(label, properties.map(p => p -> mapping.toMapping).toMap))
  def addProperty[T](model: String, propertyName: String)(implicit mapping: UMapping[T]): Operations =
    addOperations(AddProperty(model, propertyName, mapping.toMapping))
  def removeProperty[T](model: String, propertyName: String, usedOnlyByThisModel: Boolean): Operations =
    addOperations(RemoveProperty(model, propertyName, usedOnlyByThisModel))
  def updateGraph(comment: String, model: String)(update: Traversal[Vertex, Vertex, Converter.Identity[Vertex]] => Try[Unit]): Operations =
    addOperations(UpdateGraph(model, update, comment))
  def addIndex(model: String, indexType: IndexType.Value, properties: String*): Operations =
    addOperations(AddIndex(model, indexType, properties))
  def dbOperation[DB <: Database: ClassTag](comment: String)(op: DB => Try[Unit]): Operations =
    addOperations(DBOperation[DB](comment, op))
  def noop: Operations           = addOperations(NoOperation)
  def rebuildIndexes: Operations = addOperations(RebuildIndexes)

  def execute(db: Database, schema: Schema)(implicit authContext: AuthContext): Try[Unit] =
    db.version(schemaName) match {
      case 0 =>
        logger.info(s"$schemaName: Create database schema")
        db.createSchemaFrom(schema)
          .flatMap(_ => db.addSchemaIndexes(schema))
          .flatMap(_ => db.setVersion(schemaName, operations.length + 1))
      case version =>
        operations.zipWithIndex.foldLeft[Try[Unit]](Success(())) {
          case (Success(_), (ops, v)) if v + 1 >= version =>
            (ops match {
              case AddVertexModel(label, mapping) =>
                logger.info(s"Add vertex model $label to schema")
                db.addVertexModel(label, mapping)
              case AddEdgeModel(label, mapping) =>
                logger.info(s"Add edge model $label to schema")
                db.addEdgeModel(label, mapping)
              case AddProperty(model, propertyName, mapping) =>
                logger.info(s"$schemaName: Add property $propertyName to $model")
                db.addProperty(model, propertyName, mapping)
              case RemoveProperty(model, propertyName, usedOnlyByThisModel) =>
                logger.info(s"$schemaName: Remove property $propertyName from $model")
                db.removeProperty(model, propertyName, usedOnlyByThisModel)
              case UpdateGraph(model, update, comment) =>
                logger.info(s"$schemaName: Update graph: $comment")
                db.tryTransaction(graph => update(db.labelFilter(model)(Traversal.V()(graph))))
                  .recoverWith { case error => Failure(InternalError(s"Unable to execute migration operation: $comment", error)) }
              case AddIndex(model, indexType, properties) =>
                logger.info(s"$schemaName: Add index in $model for properties: ${properties.mkString(", ")}")
                db.addIndex(model, indexType, properties)
              case dbOperation: DBOperation[_] =>
                logger.info(s"$schemaName: Update database: ${dbOperation.comment}")
                dbOperation(db)
              case NoOperation => Success(())
              case RebuildIndexes =>
                logger.info(s"$schemaName: Remove all indexes")
                db.removeAllIndexes()
                logger.info(s"$schemaName: Add schema indexes")
                db.addSchemaIndexes(schema)
            }).flatMap(_ => db.setVersion(schemaName, v + 2))
          case (acc, _) => acc
        }
    }
}
