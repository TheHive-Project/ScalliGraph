package org.thp.scalligraph.models

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.thp.scalligraph._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.services._
import org.thp.scalligraph.traversal.TraversalOps
import org.thp.scalligraph.traversal.{Converter, Graph, Traversal}

import scala.util.Try

@BuildVertexEntity
case class Person(name: String, age: Int)

object Person {
  val initialValues = Seq(Person("marc", 34), Person("franck", 28))
}

@BuildVertexEntity
case class Software(name: String, lang: String)

@BuildEdgeEntity[Person, Person]
case class Knows(weight: Double)

@BuildEdgeEntity[Person, Software]
case class Created(weight: Double)

trait ModernOps extends TraversalOps {
  implicit class PersonOpsDefs(traversal: Traversal.V[Person]) {
    def created: Traversal.V[Software]               = traversal.out[Created].v[Software]
    def getByName(name: String): Traversal.V[Person] = traversal.has(_.name, name)
    def created(predicate: P[Double]): Traversal.V[Software] =
      traversal
        .outE[Created]
        .has(_.weight, predicate)
        .inV
        .v[Software]
    def connectedEdge: Seq[String]                            = traversal.outE().label.toSeq
    def knownLevels: Seq[Double]                              = traversal.outE[Knows].property("weight", Converter.double).toSeq
    def knows: Traversal.V[Person]                            = traversal.out[Knows].v[Person]
    def friends(threshold: Double = 0.8): Traversal.V[Person] = traversal.outE[Knows].has(_.weight, P.gte(threshold)).inV.v[Person]
  }

  implicit class SoftwareOpsDefs(traversal: Traversal.V[Software]) {
    def createdBy: Traversal.V[Person]  = traversal.in("Created").v[Person]
    def isRipple: Traversal.V[Software] = traversal.has(_.name, "ripple")

  }
}

class PersonSrv extends VertexSrv[Person] with ModernOps {
  def create(e: Person)(implicit graph: Graph, authContext: AuthContext): Try[Person with Entity] = createEntity(e)

  override def getByName(name: String)(implicit graph: Graph): Traversal.V[Person] =
    startTraversal.getByName(name)
}

class SoftwareSrv extends VertexSrv[Software] {
  def create(e: Software)(implicit graph: Graph, authContext: AuthContext): Try[Software with Entity] = createEntity(e)
}

object ModernSchema extends Schema {
  override def modelList: Seq[Model] = Seq(Model.vertex[Person], Model.vertex[Software], Model.edge[Knows], Model.edge[Created])
}

object ModernDatabaseBuilder {

  val personSrv   = new PersonSrv
  val softwareSrv = new SoftwareSrv
  val knowsSrv    = new EdgeSrv[Knows, Person, Person]
  val createdSrv  = new EdgeSrv[Created, Person, Software]

  def build(db: Database)(implicit authContext: AuthContext): Try[Unit] =
    db.createSchemaFrom(ModernSchema)
      .flatMap(_ => db.addSchemaIndexes(ModernSchema))
      .flatMap { _ =>
        db.tryTransaction { implicit graph =>
          for {
            vadas  <- personSrv.create(Person("vadas", 27))
            marko  <- personSrv.create(Person("marko", 29))
            josh   <- personSrv.create(Person("josh", 32))
            peter  <- personSrv.create(Person("peter", 35))
            lop    <- softwareSrv.create(Software("lop", "java"))
            ripple <- softwareSrv.create(Software("ripple", "java"))
            _      <- knowsSrv.create(Knows(0.5), marko, vadas)
            _      <- knowsSrv.create(Knows(1), marko, josh)
            _      <- createdSrv.create(Created(0.4), marko, lop)
            _      <- createdSrv.create(Created(1), josh, ripple)
            _      <- createdSrv.create(Created(0.4), josh, lop)
            _      <- createdSrv.create(Created(0.2), peter, lop)
          } yield ()
        }
      }
}
