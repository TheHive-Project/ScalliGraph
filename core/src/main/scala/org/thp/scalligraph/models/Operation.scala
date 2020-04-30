package org.thp.scalligraph.models

import gremlin.scala._
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.steps.Traversal

import scala.util.{Failure, Success, Try}

sealed trait Operation

case class AddProperty(model: String, propertyName: String, mapping: Mapping[_, _, _])                 extends Operation
case class RemoveProperty(model: String, propertyName: String, usedOnlyByThisModel: Boolean)           extends Operation
case class UpdateGraph(model: String, update: Traversal[Vertex, Vertex] => Try[Unit], comment: String) extends Operation

object Operations {
  def apply(schemaName: String, schema: Schema): Operations = Operations(schemaName, schema, 1, Map.empty)
}

case class Operations private (schemaName: String, schema: Schema, currentVersion: Int, operations: Map[Int, Seq[Operation]]) {
  private def addOperations(op: Operation*): Operations =
    copy(operations = operations + (currentVersion -> (operations.getOrElse(currentVersion, Nil) ++ op)))
  def forVersion(v: Int): Operations = copy(currentVersion = v)
  def addProperty[T](model: String, propertyName: String)(implicit mapping: UniMapping[T]): Operations =
    addOperations(AddProperty(model, propertyName, mapping.toMapping))
  def removeProperty[T](model: String, propertyName: String, usedOnlyByThisModel: Boolean): Operations =
    addOperations(RemoveProperty(model, propertyName, usedOnlyByThisModel))
  def updateGraph(comment: String, model: String)(update: Traversal[Vertex, Vertex] => Try[Unit]): Operations =
    addOperations(UpdateGraph(model, update, comment))

  def execute(db: Database)(implicit authContext: AuthContext): Try[Unit] =
    db.version(schemaName) match {
      case 0 => db.createSchemaFrom(schema)
      case version =>
        operations.toSeq.sortBy(_._1).foldLeft[Try[Unit]](Success(())) {
          case (acc, (v, ops)) if v > version =>
            ops
              .foldLeft(acc) {
                case (_: Success[_], AddProperty(model, propertyName, mapping)) => db.addProperty(model, propertyName, mapping)
                case (_: Success[_], RemoveProperty(model, propertyName, usedOnlyByThisModel)) =>
                  db.removeProperty(model, propertyName, usedOnlyByThisModel)
                case (_: Success[_], UpdateGraph(model, update, comment)) =>
                  db.tryTransaction(graph => update(Traversal(db.labelFilter(model)(graph.V))))
                    .recoverWith { case error => Failure(InternalError(s"Unable to execute migration operation: $comment", error)) }
                case (f: Failure[_], _) => f
              }
              .flatMap(_ => db.setVersion(schemaName, v))
          case (acc, _) => acc
        }
    }

}
