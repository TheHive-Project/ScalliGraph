package org.thp.scalligraph.macros

import scala.reflect.macros.blackbox

import org.thp.scalligraph.MacroLogger
import org.thp.scalligraph.models.Model

class ModelMacro(val c: blackbox.Context) extends MappingMacro with IndexMacro with MacroLogger {

  import c.universe._

  /**
    * Create a model from entity type e
    */
  def mkVertexModel[E <: Product: WeakTypeTag]: Expr[Model.Vertex[E]] = {
    val entityType: Type = weakTypeOf[E]
    initLogger(entityType.typeSymbol)
    debug(s"Building vertex model for $entityType")
    val label: String      = entityType.toString.split("\\.").last
    val mappings           = getEntityMappings[E]
    val mappingDefinitions = mappings.map(m => q"val ${m.valName} = ${m.definition}")
    val fieldMap           = mappings.map(m => q"${m.name} -> ${m.valName}")
    val setProperties      = mappings.map(m => q"db.setProperty(vertex, ${m.name}, e.${TermName(m.name)}, ${m.valName})")
    val domainBuilder = mappings.map { m =>
      q"""
        try {
          db.getProperty(element, ${m.name}, ${m.valName})
        } catch {
          case t: Throwable ⇒
            throw InternalError($label + " " + element.id + " doesn't comply with its schema, field `" + ${m.name} + "` is missing (" + element.value(${m.name}) + "): " + Model.printElement(element), t)
        }
        """
    }
    val model = c.Expr[Model.Vertex[E]](q"""
      import java.util.Date
      import scala.concurrent.{ ExecutionContext, Future }
      import gremlin.scala.{ Graph, Vertex }
      import scala.util.{ Failure, Try }
      import org.thp.scalligraph.{ FPath, InternalError }
      import org.thp.scalligraph.models.{ Database, Entity, IndexType, Mapping, Model, UniMapping, VertexModel }

      new VertexModel { thisModel ⇒
        override type E = $entityType

        override val label: String = $label

        override val indexes: Seq[(IndexType.Value, Seq[String])] = ${getIndexes[E]}

        ..$mappingDefinitions

        override def create(e: E)(implicit db: Database, graph: Graph): Vertex = {
          val vertex = graph.addVertex(label)
          ..$setProperties
          vertex
        }

        override val fields: Map[String, Mapping[_, _, _]] = Map(..$fieldMap)
        override def toDomain(element: ElementType)(implicit db: Database): EEntity = new $entityType(..$domainBuilder) with Entity {
          val _id        = element.value[String]("_id")
          val _model     = thisModel
          val _createdAt = db.getProperty(element, "_createdAt", UniMapping.dateMapping)
          val _createdBy = db.getProperty(element, "_createdBy", UniMapping.stringMapping)
          val _updatedAt = db.getProperty(element, "_updatedAt", UniMapping.dateMapping.optional)
          val _updatedBy = db.getProperty(element, "_updatedBy", UniMapping.stringMapping.optional)
        }
      }
      """)
    ret(s"Vertex model $entityType", model)
  }

  def mkEdgeModel[E <: Product: WeakTypeTag, FROM <: Product: WeakTypeTag, TO <: Product: WeakTypeTag]: Expr[Model.Edge[E, FROM, TO]] = {
    val entityType: Type = weakTypeOf[E]
    val fromType: Type   = weakTypeOf[FROM]
    val toType: Type     = weakTypeOf[TO]
    initLogger(entityType.typeSymbol)
    debug(s"Building vertex model for $entityType")
    val label: String      = entityType.toString.split("\\.").last
    val fromLabel: String  = fromType.toString.split("\\.").last
    val toLabel: String    = toType.toString.split("\\.").last
    val mappings           = getEntityMappings[E]
    val mappingDefinitions = mappings.map(m => q"val ${m.valName} = ${m.definition}")
    val fieldMap           = mappings.map(m => q"${m.name} -> ${m.valName}")
    val setProperties      = mappings.map(m => q"db.setProperty(edge, ${m.name}, e.${TermName(m.name)}, ${m.valName})")
    val domainBuilder = mappings.map { m =>
      q"""
        try {
          db.getProperty(element, ${m.name}, ${m.valName})
        } catch {
          case t: Throwable ⇒
            throw InternalError($label + " " + element.id + " doesn't comply with its schema, field `" + ${m.name} + "` is missing (" + element.value(${m.name}) + "): " + Model.printElement(element), t)
        }
        """
    }
    val model = c.Expr[Model.Edge[E, FROM, TO]](q"""
      import java.util.Date
      import scala.concurrent.{ ExecutionContext, Future }
      import scala.util.Try
      import gremlin.scala.{ Edge, Graph, Vertex }
      import org.thp.scalligraph.{ FPath, InternalError }
      import org.thp.scalligraph.models.{ Database, EdgeModel, Entity, IndexType, Mapping, Model, UniMapping }

      new EdgeModel[$fromType, $toType] { thisModel ⇒
        override type E = $entityType

        override val label: String = $label

        override val fromLabel: String = $fromLabel

        override val toLabel: String = $toLabel

        override val indexes: Seq[(IndexType.Value, Seq[String])] = ${getIndexes[E]}

        ..$mappingDefinitions

        override def create(e: E, from: Vertex, to: Vertex)(implicit db: Database, graph: Graph): Edge = {
          val edge = from.addEdge(label, to)
          ..$setProperties
          edge
        }
        override val fields: Map[String, Mapping[_, _, _]] = Map(..$fieldMap)
        override def toDomain(element: ElementType)(implicit db: Database): EEntity = new $entityType(..$domainBuilder) with Entity {
          val _id        = element.value[String]("_id")
          val _model     = thisModel
          val _createdAt = db.getProperty(element, "_createdAt", UniMapping.dateMapping)
          val _createdBy = db.getProperty(element, "_createdBy", UniMapping.stringMapping)
          val _updatedAt = db.getProperty(element, "_updatedAt", UniMapping.dateMapping.optional)
          val _updatedBy = db.getProperty(element, "_updatedBy", UniMapping.stringMapping.optional)

        }
      }
      """)
    ret(s"Edge model $entityType", model)
  }
}
