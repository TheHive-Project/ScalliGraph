package org.thp.scalligraph.models
import java.io.InputStream

import scala.reflect.runtime.{universe ⇒ ru}

import gremlin.scala._
import org.thp.scalligraph.FPath
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.controllers.UpdateOps

trait CreateVertex[V <: Product] extends ((Graph, AuthContext, Model.Vertex[V], V) ⇒ V with Entity) {
  def compose(hook: CreateVertexHook[V]): CreateVertex[V] =
    (graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V) ⇒ {
      hook(this)(graph, authContext, model, v)
    }
}

trait CreateVertexHook[V <: Product] extends (CreateVertex[V] ⇒ CreateVertex[V])

trait CreateEdge[E <: Product, FROM <: Product, TO <: Product]
    extends ((Graph, AuthContext, Model.Edge[E, FROM, TO], E, FROM with Entity, TO with Entity) ⇒ E with Entity) {
  def compose(hook: CreateEdgeHook[E, FROM, TO]): CreateEdge[E, FROM, TO] =
    (graph: Graph, authContext: AuthContext, model: Model.Edge[E, FROM, TO], e: E, from: FROM with Entity, to: TO with Entity) ⇒ {
      hook(this)(graph, authContext, model, e, from, to)
    }
}

trait CreateEdgeHook[E <: Product, FROM <: Product, TO <: Product] extends (CreateEdge[E, FROM, TO] ⇒ CreateEdge[E, FROM, TO])

trait Update extends ((Graph, AuthContext, Model, String, Map[FPath, UpdateOps.Type]) ⇒ Unit) {
  def compose(hook: UpdateHook): Update =
    (graph: Graph, authContext: AuthContext, model: Model, id: String, fields: Map[FPath, UpdateOps.Type]) ⇒ {
      hook(this)(graph, authContext, model, id, fields)
    }
}

trait UpdateHook extends (Update ⇒ Update)

class HookableDatabase(db: Database) extends Database {

  override def noTransaction[A](body: Graph ⇒ A): A   = db.noTransaction(body)
  override def transaction[A](body: Graph ⇒ A): A     = db.transaction(body)
  override def createSchema(models: Seq[Model]): Unit = db.createSchema(models)

  /* create vertex */
  private var createVertexComposition: CreateVertex[Product] =
    (graph: Graph, authContext: AuthContext, model: Model.Vertex[Product], v: Product) ⇒ db.createVertex(graph, authContext, model, v)

  def registerCreateVertexHook(vertexHook: CreateVertexHook[Product]): Unit =
    createVertexComposition = createVertexComposition.compose(vertexHook)

  override def createVertex[V <: Product](graph: Graph, authContext: AuthContext, model: Model.Vertex[V], v: V): V with Entity =
    createVertexComposition.asInstanceOf[CreateVertex[V]](graph, authContext, model, v)

  /* create edge */
  private var createEdgeComposition: CreateEdge[Product, Product, Product] =
    (
        graph: Graph,
        authContext: AuthContext,
        model: Model.Edge[Product, Product, Product],
        e: Product,
        from: Product with Entity,
        to: Product with Entity) ⇒ db.createEdge(graph, authContext, model, e, from, to)

  def registerCreateEdgeHook[E <: Product, FROM <: Product, TO <: Product](edgeHook: CreateEdgeHook[E, FROM, TO]): Unit =
    createEdgeComposition = createEdgeComposition.compose(edgeHook.asInstanceOf[CreateEdgeHook[Product, Product, Product]])

  override def createEdge[E <: Product, FROM <: Product, TO <: Product](
      graph: Graph,
      authContext: AuthContext,
      model: Model.Edge[E, FROM, TO],
      e: E,
      from: FROM with Entity,
      to: TO with Entity): E with Entity =
    createEdgeComposition
      .asInstanceOf[CreateEdge[E, FROM, TO]](graph, authContext, model, e, from, to)

  /* update */
  private var updateComposition: Update =
    (graph: Graph, authContext: AuthContext, model: Model, id: String, fields: Map[FPath, UpdateOps.Type]) ⇒
      db.update(graph, authContext, model, id, fields)

  def registerUpdateHook(updateHook: UpdateHook): Unit =
    updateComposition = updateComposition.compose(updateHook)

  override def update(graph: Graph, authContext: AuthContext, model: Model, id: String, fields: Map[FPath, UpdateOps.Type]): Unit =
    updateComposition(graph, authContext, model, id, fields)

  override def getModel[E <: Product: ru.TypeTag]: Model.Base[E] = db.getModel[E]

  override def getVertexModel[E <: Product: ru.TypeTag]: Model.Vertex[E] = db.getVertexModel[E]

  override def getEdgeModel[E <: Product: ru.TypeTag, FROM <: Product, TO <: Product]: Model.Edge[E, FROM, TO] = db.getEdgeModel[E, FROM, TO]

  override def getProperty[D](element: Element, key: String, mapping: Mapping[D, _, _]): D = db.getProperty(element, key, mapping)

  override def setProperty[D](element: Element, key: String, value: D, mapping: Mapping[D, _, _]): Unit = db.setProperty(element, key, value, mapping)

  override def loadBinary(id: String)(implicit graph: Graph): InputStream = db.loadBinary(id)

  override def loadBinary(initialVertex: Vertex)(implicit graph: Graph): InputStream = db.loadBinary(initialVertex)

  override def saveBinary(is: InputStream)(implicit graph: Graph): Vertex = db.saveBinary(is)
}
