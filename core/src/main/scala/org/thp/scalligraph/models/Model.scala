package org.thp.scalligraph.models

import java.util.Date

import scala.reflect.runtime.{universe => ru}
import gremlin.scala._
import org.thp.scalligraph.InternalError
import org.thp.scalligraph.steps.Converter

import scala.util.{Failure, Try}
//import gremlin.scala.dsl.{Converter, DomainRoot}
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.macros.ModelMacro

import scala.annotation.StaticAnnotation
import scala.collection.JavaConverters._
import scala.language.experimental.macros

class Readonly extends StaticAnnotation

class WithMapping[D, +G](mapping: Mapping[D, _, G]) extends StaticAnnotation

object IndexType extends Enumeration {
  val basic, standard, unique, fulltext = Value
}
class DefineIndex(indexType: IndexType.Value, fields: String*) extends StaticAnnotation

trait HasModel[E <: Product] {
  val model: Model.Base[E]
}

trait HasVertexModel[E <: Product] extends HasModel[E] {
  val model: Model.Vertex[E]
}

trait HasEdgeModel[E <: Product, FROM <: Product, TO <: Product] extends HasModel[E] {
  val model: Model.Edge[E, FROM, TO]
}

trait Entity {
  def _id: String
  def _model: Model
  def _createdBy: String
  def _updatedBy: Option[String]
  def _createdAt: Date
  def _updatedAt: Option[Date]
}

object Model {

  type Base[E0 <: Product] = Model {
    type E = E0
  }

  type Vertex[E0 <: Product] = VertexModel {
    type E = E0
  }

  type Edge[E0 <: Product, FROM <: Product, TO <: Product] = EdgeModel[FROM, TO] {
    type E = E0
  }

  def vertex[E <: Product]: Vertex[E] = macro ModelMacro.mkVertexModel[E]

  def edge[E <: Product, FROM <: Product, TO <: Product]: Edge[E, FROM, TO] =
    macro ModelMacro.mkEdgeModel[E, FROM, TO]

  def printElement(e: Element): String =
    e +
      e.keys()
        .asScala
        .map(key =>
          s"\n - $key = ${e.properties[Any](key).asScala.map(_.value()).mkString(",")} (${e.properties[Any](key).asScala.toSeq.headOption.fold("empty")(_.value.getClass.toString)})"
        )
        .mkString

  def getVertexModel[E <: Product: ru.TypeTag]: Model.Vertex[E] = {
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    Try(rm.reflectModule(ru.typeOf[E].typeSymbol.companion.asModule).instance)
      .collect {
        case hm: HasVertexModel[_] => hm.model.asInstanceOf[Model.Vertex[E]]
      }
      .recoverWith {
        case error => Failure(InternalError(s"${ru.typeOf[E].typeSymbol} is not a vertex model", error))
      }
      .get
  }

  def getEdgeModel[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product]: Model.Edge[E, FROM, TO] = {
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    Try(rm.reflectModule(ru.typeOf[E].typeSymbol.companion.asModule).instance)
      .collect {
        case hm: HasEdgeModel[_, _, _] => hm.model.asInstanceOf[Model.Edge[E, FROM, TO]]
      }
      .recoverWith {
        case error => Failure(InternalError(s"${ru.typeOf[E].typeSymbol} is not an edge model", error))
      }
      .get
  }
}

abstract class Model {
  thisModel =>
  type E <: Product
  type EEntity = E with Entity
  type ElementType <: Element

  val label: String

  val indexes: Seq[(IndexType.Value, Seq[String])]

  def get(id: String)(implicit db: Database, graph: Graph): ElementType
  val fields: Map[String, Mapping[_, _, _]]
  def toDomain(element: ElementType)(implicit db: Database): EEntity
  def addEntity(e: E, entity: Entity): EEntity

  val converter: Converter[EEntity, ElementType] = ???
//    new Converter[EEntity] {
//      override type GraphType = ElementType
//      override def toDomain(v: ElementType): E with Entity = thisModel.toDomain(v)(db)
//      override def toGraph(e: EEntity): GraphType          = thisModel.get(e._id)(db, graph)
//    }
}

abstract class VertexModel extends Model { thisModel =>
  override type ElementType = Vertex

  val initialValues: Seq[E]                  = Nil
  def getInitialValues: Seq[InitialValue[E]] = initialValues.map(iv => InitialValue(this.asInstanceOf[Model.Vertex[E]], iv))

  def create(e: E)(implicit db: Database, graph: Graph): Vertex

  override def get(id: String)(implicit db: Database, graph: Graph): Vertex =
    graph.V(id).headOption().getOrElse(throw NotFoundError(s"Vertex $id not found"))
}

abstract class EdgeModel[FROM <: Product, TO <: Product] extends Model { thisModel =>
  override type ElementType = Edge

  val fromLabel: String
  val toLabel: String

  def create(e: E, from: Vertex, to: Vertex)(implicit db: Database, graph: Graph): Edge

  override def get(id: String)(implicit db: Database, graph: Graph): Edge =
    graph.E(id).headOption().getOrElse(throw NotFoundError(s"Edge $id not found"))
}

class EdgeEntity[From <: VertexEntity, To <: VertexEntity]

class VertexEntity
