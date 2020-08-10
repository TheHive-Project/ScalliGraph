package org.thp.scalligraph.models

import java.util.Date

import org.apache.tinkerpop.gremlin.structure.{Edge, Element, Graph, Vertex}
import org.thp.scalligraph.NotFoundError
import org.thp.scalligraph.macros.ModelMacro
import org.thp.scalligraph.traversal.TraversalOps._
import org.thp.scalligraph.traversal.{Converter, Traversal}

import scala.annotation.StaticAnnotation
import scala.collection.JavaConverters._
import scala.language.experimental.macros

class Readonly extends StaticAnnotation

class WithMapping[D, +G](mapping: Mapping[D, _, G]) extends StaticAnnotation

object IndexType extends Enumeration {
  val basic, standard, unique, fulltext = Value
}
class DefineIndex(indexType: IndexType.Value, fields: String*) extends StaticAnnotation

trait HasModel {
  val model: Model
}
//
//trait HasVertexModel[E <: Product] extends HasModel[E] {
//  val model: Model.Vertex[E]
//}
//
//trait HasEdgeModel[E <: Product, FROM <: Product, TO <: Product] extends HasModel[E] {
//  val model: Model.Edge[E, FROM, TO]
//}

trait Entity { _: Product =>
  def _id: String
  def _label: String
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

  type Edge[E0 <: Product] = EdgeModel {
    type E = E0
  }

  def buildVertexModel[E <: Product]: Model.Vertex[E] = macro ModelMacro.mkVertexModel[E]

  def buildEdgeModel[E <: Product]: Model.Edge[E] =
    macro ModelMacro.mkEdgeModel[E]

  def printElement(e: Element): String =
    e +
      e.keys()
        .asScala
        .map(key =>
          s"\n - $key = ${e.properties[Any](key).asScala.map(_.value()).mkString(",")} (${e.properties[Any](key).asScala.toSeq.headOption.fold("empty")(_.value.getClass.toString)})"
        )
        .mkString

  implicit def vertex[E <: Product]: Vertex[E] = macro ModelMacro.getModel[E]
  implicit def edge[E <: Product]: Edge[E] = macro ModelMacro.getModel[E]
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
  def addEntity(e: E, entity: Entity): EEntity
  val converter: Converter[EEntity, ElementType]

  //    new Converter[EEntity] {
//      override type GraphType = ElementType
//      override def toDomain(v: ElementType): E with Entity = thisModel.toDomain(v)(db)
//      override def toGraph(e: EEntity): GraphType          = thisModel.get(e._id)(db, graph)
//    }
//  def setProperty[V](
//      traversal: Traversal[EEntity, ElementType, Converter[EEntity, ElementType]],
//      name: String,
//      value: V
//  ): Traversal[EEntity, ElementType, Converter[EEntity, ElementType]] = {
//    lazy val label = UUID.randomUUID().toString
//    val mapping    = fields(name)
//
//    mapping match {
//      case m: SingleMapping[V, _] =>
//        m.toGraphOpt(value).fold(traversal.removeProperty(name))(v => traversal.onDeepRaw(_.property(name, v)))
//      case m: OptionMapping[v, _] =>
//        value
//          .asInstanceOf[Option[v]]
//          .flatMap(m.toGraphOpt)
//          .fold(traversal.removeProperty(name))(v => traversal.onDeepRaw(_.property(name, v)))
//      case m: ListMapping[v, _] =>
//        traversal.onDeepRaw { gt =>
//          value
//            .asInstanceOf[Seq[v]]
//            .flatMap(m.toGraphOpt)
//            .foldLeft(gt.as(label).properties(name).drop().iterate().select(label))(_.property(Cardinality.list, name, _))
//            .asInstanceOf[gt.type]
//        }
//      case m: SetMapping[v, _] =>
//        traversal.onDeepRaw { gt =>
//          value
//            .asInstanceOf[Set[v]]
//            .flatMap(m.toGraphOpt)
//            .foldLeft(gt.as(label).properties(name).drop().iterate().select(label))(_.property(Cardinality.set, name, _))
//            .asInstanceOf[gt.type]
//        }
//    }
//  }
//
//  def setProperty[V](
//      element: ElementType,
//      name: String,
//      value: V
//  ): Unit = setProperty(element, name, value, fields(name))
//
//  def setProperty[V](
//      element: ElementType,
//      name: String,
//      value: V,
//      mapping: Mapping[_, _, _]
//  ): Unit =
//    (element, mapping) match {
//      case (_, m: SingleMapping[V, _]) =>
//        m.toGraphOpt(value).fold(element.property(name).remove())(v => element.property(name, v))
//      case (_, m: OptionMapping[v, _]) =>
//        value
//          .asInstanceOf[Option[v]]
//          .flatMap(m.toGraphOpt)
//          .fold(element.property(name).remove())(v => element.property(name, v))
//      case (v: Vertex, m: ListMapping[v, _]) =>
//        value
//          .asInstanceOf[Seq[v]]
//          .flatMap(m.toGraphOpt)
//          .foreach(v.property(Cardinality.list, name, _))
//      case (v: Vertex, m: SetMapping[v, _]) =>
//        value
//          .asInstanceOf[Set[v]]
//          .flatMap(m.toGraphOpt)
//          .foreach(v.property(Cardinality.set, name, _))
//    }
}

abstract class VertexModel extends Model { thisModel =>
  override type ElementType = Vertex

  val initialValues: Seq[E]                  = Nil
  def getInitialValues: Seq[InitialValue[E]] = initialValues.map(iv => InitialValue(this.asInstanceOf[Model.Vertex[E]], iv))

  def create(e: E)(implicit db: Database, graph: Graph): Vertex

  override def get(id: String)(implicit db: Database, graph: Graph): Vertex =
    Traversal.V(id).headOption.getOrElse(throw NotFoundError(s"Vertex $id not found"))
}

abstract class EdgeModel extends Model { thisModel =>
  override type ElementType = Edge

  def create(e: E, from: Vertex, to: Vertex)(implicit db: Database, graph: Graph): Edge

  override def get(id: String)(implicit db: Database, graph: Graph): Edge =
    Traversal.E(id).headOption.getOrElse(throw NotFoundError(s"Edge $id not found"))
}

class EdgeEntity[From <: VertexEntity, To <: VertexEntity]

class VertexEntity
