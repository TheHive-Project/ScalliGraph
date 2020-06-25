package org.thp.scalligraph.macros

import org.thp.scalligraph.models.Model

import scala.reflect.macros.blackbox

class ModelMacro(val c: blackbox.Context) extends MappingMacroHelper with IndexMacro with MacroLogger {

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
    val initialValues =
      try {
        val entityTypeModule = entityType.typeSymbol.companion
        if (entityTypeModule.typeSignature.members.exists(_.name.toString == "initialValues"))
          q"$entityTypeModule.initialValues"
        else q"Nil"
      } catch {
        case e: Throwable =>
          error(s"Unable to get initialValues from $label: $e")
          q"Nil"
      }
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
      import scala.concurrent.{ExecutionContext, Future}
      import gremlin.scala.{Graph, Vertex}
      import scala.util.{Failure, Try}
      import org.thp.scalligraph.InternalError
      import org.thp.scalligraph.controllers.FPath
      import org.thp.scalligraph.models.{Database, Entity, IndexType, Mapping, Model, UniMapping, VertexModel}

      new VertexModel { thisModel ⇒
        override type E = $entityType

        override val label: String = $label

        override val initialValues: Seq[E] = $initialValues
        override val indexes: Seq[(IndexType.Value, Seq[String])] = ${getIndexes[E]}

        ..$mappingDefinitions

        override def create(e: E)(implicit db: Database, graph: Graph): Vertex = {
          val vertex = graph.addVertex(label)
          ..$setProperties
          vertex
        }

        override val fields: Map[String, Mapping[_, _, _]] = Map(..$fieldMap)

        override def toDomain(element: ElementType)(implicit db: Database): EEntity = new $entityType(..$domainBuilder) with Entity {
          val _id        = element.id().toString
          val _model     = thisModel
          val _createdAt = db.getProperty(element, "_createdAt", UniMapping.date)
          val _createdBy = db.getProperty(element, "_createdBy", UniMapping.string)
          val _updatedAt = db.getProperty(element, "_updatedAt", UniMapping.date.optional)
          val _updatedBy = db.getProperty(element, "_updatedBy", UniMapping.string.optional)
        }

        override def addEntity(e: $entityType, entity: Entity): EEntity = new $entityType(..${mappings
      .map(m => q"e.${TermName(m.name)}")}) with Entity {
          override def _id: String                = entity._id
          override def _model: Model              = entity._model
          override def _createdBy: String         = entity._createdBy
          override def _updatedBy: Option[String] = entity._updatedBy
          override def _createdAt: Date           = entity._createdAt
          override def _updatedAt: Option[Date]   = entity._updatedAt
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
    val getProperties = mappings.map { m =>
      q"""(${m.valName}.cardinality match {
          case MappingCardinality.option => properties.get(${m.name}).map(v => ${m.valName}.toDomain(v.head.asInstanceOf[${m.valName}.GraphType]))
          case MappingCardinality.single => ${m.valName}.toDomain(properties.getOrElse(${m.name}, fail(${m.name})).head.asInstanceOf[${m.valName}.GraphType])
          case MappingCardinality.list   => properties.getOrElse(${m.name}, Nil).map(v => ${m.valName}.toDomain(v.asInstanceOf[${m.valName}.GraphType]))
          case MappingCardinality.set    => properties.getOrElse(${m.name}, Nil).toSet[Any].map(v => ${m.valName}.toDomain(v.asInstanceOf[${m.valName}.GraphType]))
         }).asInstanceOf[${m.tpe}]
       """
    }
    val model = c.Expr[Model.Edge[E, FROM, TO]](q"""
      import java.util.Date
      import scala.concurrent.{ExecutionContext, Future}
      import scala.util.Try
      import gremlin.scala.{Edge, Graph, Vertex}
      import org.thp.scalligraph.InternalError
      import org.thp.scalligraph.controllers.FPath
      import org.thp.scalligraph.models.{Database, EdgeModel, Entity, IndexType, Mapping, MappingCardinality, Model, UniMapping}

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

        override def toDomain(element: ElementType)(implicit db: Database): EEntity = {
          def fail(name: String): Nothing =
            throw InternalError($label + " " + element.id + " doesn't comply with its schema, field `" + name + "` is missing (" + element.value(name) + "): " + Model.printElement(element))
          val properties = db.getAllProperties(element)
          new $entityType(..$getProperties) with Entity {
            val _id        = element.id().toString
            val _model     = thisModel
            val _createdAt = UniMapping.date.toDomain(properties.getOrElse("_createdAt", fail("_createdAt")).head.asInstanceOf[Date])
            val _createdBy = UniMapping.string.toDomain(properties.getOrElse("_createdBy", fail("_createdBy")).head.asInstanceOf[String])
            val _updatedAt = properties.get("_updatedAt").map(v => UniMapping.date.toDomain(v.head.asInstanceOf[Date]))
            val _updatedBy = properties.get("_updatedBy").map(v => UniMapping.string.toDomain(v.head.asInstanceOf[String]))
          }
        }

        override def addEntity(e: $entityType, entity: Entity): EEntity =
          new $entityType(..${mappings.map(m => q"e.${TermName(m.name)}")}) with Entity {
            override def _id: String                = entity._id
            override def _model: Model              = entity._model
            override def _createdBy: String         = entity._createdBy
            override def _updatedBy: Option[String] = entity._updatedBy
            override def _createdAt: Date           = entity._createdAt
            override def _updatedAt: Option[Date]   = entity._updatedAt
          }
      }
      """)
    ret(s"Edge model $entityType", model)
  }
}
