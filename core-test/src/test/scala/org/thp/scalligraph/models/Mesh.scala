package org.thp.scalligraph.models

import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.services.{EdgeSrv, VertexSrv}
import org.thp.scalligraph.traversal.{Graph, Traversal}
import org.thp.scalligraph.{BuildEdgeEntity, BuildVertexEntity, EntityId}

import scala.util.Try

@BuildVertexEntity
case class A(name: String)

@BuildVertexEntity
case class B(
    name: String,
    oneA: EntityId,
    maybeA: Option[EntityId],
    aName: String,
    maybeAName: Option[String],
    someA: Seq[EntityId],
    someAName: Seq[String]
)

@BuildEdgeEntity[B, A]
case class BAOne()
@BuildEdgeEntity[B, A]
case class BAMaybe()
@BuildEdgeEntity[B, A]
case class BAName()
@BuildEdgeEntity[B, A]
case class BAMaybeName()
@BuildEdgeEntity[B, A]
case class BASome()
@BuildEdgeEntity[B, A]
case class BASomeName()

class ASrv extends VertexSrv[A] {
  def create(a: A)(implicit graph: Graph, authContext: AuthContext): Try[A with Entity] = createEntity(a)
  override def getByName(name: String)(implicit graph: Graph): Traversal.V[A]           = startTraversal.has(_.name, name)
}

class BSrv extends VertexSrv[B] {
  def create(b: B)(implicit graph: Graph, authContext: AuthContext): Try[B with Entity] = createEntity(b)
  override def getByName(name: String)(implicit graph: Graph): Traversal.V[B]           = startTraversal.has(_.name, name)
}

object MeshSchema extends Schema {
  val aSrv           = new ASrv
  val bSrv           = new BSrv
  val baOneSrv       = new EdgeSrv[BAOne, B, A]
  val baMaybeSrv     = new EdgeSrv[BAMaybe, B, A]
  val baNameSrv      = new EdgeSrv[BAName, B, A]
  val baMaybeNameSrv = new EdgeSrv[BAMaybeName, B, A]
  val baSomeSrv      = new EdgeSrv[BASome, B, A]
  val baSomeNameSrv  = new EdgeSrv[BASomeName, B, A]

  override def modelList: Seq[Model] = Seq(aSrv.model, bSrv.model)
}

object MeshDatabaseBuilder {
  def build(implicit db: Database, authContext: AuthContext): Try[Unit] =
    db.createSchemaFrom(MeshSchema)
      .flatMap(_ => db.addSchemaIndexes(MeshSchema))
      .flatMap { _ =>
        db.tryTransaction { implicit graph =>
          for {
            a <- MeshSchema.aSrv.create(A("a"))
            _ = Thread.sleep(1)
            b <- MeshSchema.aSrv.create(A("b"))
            _ = Thread.sleep(1)
            c <- MeshSchema.aSrv.create(A("c"))
            _ = Thread.sleep(1)
            d <- MeshSchema.aSrv.create(A("d"))
            _ = Thread.sleep(1)
            e <- MeshSchema.aSrv.create(A("e"))
            _ = Thread.sleep(1)
            f <- MeshSchema.aSrv.create(A("f"))
            _ = Thread.sleep(1)
            g <- MeshSchema.aSrv.create(A("g"))
            _ = Thread.sleep(1)
            h <- MeshSchema.aSrv.create(A("h"))
            _ = Thread.sleep(1)
            i  <- MeshSchema.aSrv.create(A("i"))
            b1 <- MeshSchema.bSrv.create(B("b1", a._id, Some(b._id), "c", Some("d"), Seq(e._id, f._id), Seq("g", "h", "i")))
            _  <- MeshSchema.baOneSrv.create(new BAOne, b1, a)
            _  <- MeshSchema.baMaybeSrv.create(new BAMaybe, b1, b)
            _  <- MeshSchema.baNameSrv.create(new BAName, b1, c)
            _  <- MeshSchema.baMaybeNameSrv.create(new BAMaybeName, b1, d)
            _  <- MeshSchema.baSomeSrv.create(new BASome, b1, e)
            _  <- MeshSchema.baSomeSrv.create(new BASome, b1, f)
            _  <- MeshSchema.baSomeNameSrv.create(new BASomeName, b1, g)
            _  <- MeshSchema.baSomeNameSrv.create(new BASomeName, b1, h)
            _  <- MeshSchema.baSomeNameSrv.create(new BASomeName, b1, i)
          } yield ()
        }
      }
}
