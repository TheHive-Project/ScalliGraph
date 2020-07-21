package org.thp.scalligraph.models

import gremlin.scala.{Graph, P}
import javax.inject.{Inject, Singleton}
import org.thp.scalligraph._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.services._
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.{Converter, Traversal}

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

object ModernOps {
  implicit class PersonOps(traversal: Traversal.V[Person]) {
    def created: Traversal.V[Software]                        = traversal.out[Created].vertexModel[Software]
    def getByName(name: String): Traversal.V[Person]          = traversal.has("name", name)
    def created(predicate: P[Double]): Traversal.V[Software]  = traversal.outE[Created].has("weight", predicate).inV.vertexModel[Software]
    def connectedEdge: List[String]                           = traversal.outE().label.toList
    def knownLevels: List[Double]                             = traversal.outE[Knows].property("weight", Converter.double).toList
    def knows: Traversal.V[Person]                            = traversal.out[Knows].vertexModel[Person]
    def friends(threshold: Double = 0.8): Traversal.V[Person] = traversal.outE[Knows].has("weight", P.gte(threshold)).inV.vertexModel[Person]
  }

  implicit class SoftwareOps(traversal: Traversal.V[Software]) {
    def createdBy: Traversal.V[Person]  = traversal.in("Created").vertexModel[Person]
    def isRipple: Traversal.V[Software] = traversal.has("name", "ripple")

  }
}

import org.thp.scalligraph.models.ModernOps._

@Singleton
class PersonSrv @Inject() (implicit db: Database) extends VertexSrv[Person] {
  def create(e: Person)(implicit graph: Graph, authContext: AuthContext): Try[Person with Entity] = createEntity(e)
  override def get(idOrNumber: String)(implicit graph: Graph): Traversal.V[Person] =
    if (db.isValidId(idOrNumber)) initSteps.getByIds(idOrNumber)
    else initSteps.getByName(idOrNumber)
}

@Singleton
class SoftwareSrv @Inject() (implicit db: Database) extends VertexSrv[Software] {
  def create(e: Software)(implicit graph: Graph, authContext: AuthContext): Try[Software with Entity] = createEntity(e)
}

@Singleton
class ModernSchema @Inject() (implicit db: Database) extends Schema {
  val personSrv                                    = new PersonSrv
  val softwareSrv                                  = new SoftwareSrv
  val knowsSrv                                     = new EdgeSrv[Knows, Person, Person]
  val createdSrv                                   = new EdgeSrv[Created, Person, Software]
  val vertexServices: Seq[VertexSrv[_]]            = Seq(personSrv, softwareSrv)
  override def modelList: Seq[Model]               = (vertexServices :+ knowsSrv :+ createdSrv).map(_.model)
  override def initialValues: Seq[InitialValue[_]] = vertexServices.map(_.model.getInitialValues).flatten
}

object ModernDatabaseBuilder {

  def build(schema: ModernSchema)(implicit db: Database, authContext: AuthContext): Try[Unit] =
    db.createSchemaFrom(schema)
      .flatMap(_ => db.addSchemaIndexes(schema))
      .flatMap { _ =>
        db.tryTransaction { implicit graph =>
          for {
            vadas  <- schema.personSrv.create(Person("vadas", 27))
            marko  <- schema.personSrv.create(Person("marko", 29))
            josh   <- schema.personSrv.create(Person("josh", 32))
            peter  <- schema.personSrv.create(Person("peter", 35))
            lop    <- schema.softwareSrv.create(Software("lop", "java"))
            ripple <- schema.softwareSrv.create(Software("ripple", "java"))
            _      <- schema.knowsSrv.create(Knows(0.5), marko, vadas)
            _      <- schema.knowsSrv.create(Knows(1), marko, josh)
            _      <- schema.createdSrv.create(Created(0.4), marko, lop)
            _      <- schema.createdSrv.create(Created(1), josh, ripple)
            _      <- schema.createdSrv.create(Created(0.4), josh, lop)
            _      <- schema.createdSrv.create(Created(0.2), peter, lop)
          } yield ()
        }
      }
}
