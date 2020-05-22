package org.thp.scalligraph.models

import scala.util.{Success, Try}
import gremlin.scala.{Graph, GremlinScala, Key, P, Vertex}
import javax.inject.{Inject, Singleton}
import org.thp.scalligraph._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.services._
import org.thp.scalligraph.steps.StepsOps._
import org.thp.scalligraph.steps.VertexSteps

@VertexEntity
case class Person(name: String, age: Int)

@VertexEntity
case class Software(name: String, lang: String)

@EdgeEntity[Person, Person]
case class Knows(weight: Double)

@EdgeEntity[Person, Software]
case class Created(weight: Double)

@EntitySteps[Person]
class PersonSteps(raw: GremlinScala[Vertex])(implicit db: Database, graph: Graph) extends VertexSteps[Person](raw) {
  def created = new SoftwareSteps(this.outTo[Created].raw)

  def getByName(name: String): PersonSteps = this.has("name", name)

  def created(predicate: P[Double]) = new SoftwareSteps(this.outToE[Created].has("weight", predicate).inV().raw)

  def connectedEdge: List[String] = this.outE().label.toList

  def knownLevels: List[Double] = this.outToE[Knows].value[Double](Key("weight")).toList

  def knows: PersonSteps = new PersonSteps(this.outTo[Knows].raw)

  def friends(threshold: Double = 0.8): PersonSteps = new PersonSteps(this.outToE[Knows].has("weight", P.gte(threshold)).inV().raw)

  override def newInstance(newRaw: GremlinScala[Vertex]): PersonSteps = new PersonSteps(newRaw)
}

@EntitySteps[Software]
class SoftwareSteps(raw: GremlinScala[Vertex])(implicit db: Database, graph: Graph) extends VertexSteps[Software](raw) {
  def createdBy = new PersonSteps(raw.in("Created"))

  def isRipple: SoftwareSteps = this.has("name", "ripple")

  override def newInstance(newRaw: GremlinScala[Vertex]): SoftwareSteps = new SoftwareSteps(newRaw)
  override def newInstance(): SoftwareSteps                             = new SoftwareSteps(raw.clone())
}

@Singleton
class PersonSrv @Inject() (implicit db: Database) extends VertexSrv[Person, PersonSteps] {
  override val initialValues: Seq[Person]                                                         = Seq(Person("marc", 34), Person("franck", 28))
  override def steps(raw: GremlinScala[Vertex])(implicit graph: Graph): PersonSteps               = new PersonSteps(raw)
  def create(e: Person)(implicit graph: Graph, authContext: AuthContext): Try[Person with Entity] = createEntity(e)
  override def get(idOrNumber: String)(implicit graph: Graph): PersonSteps =
    if (db.isValidId(idOrNumber)) initSteps.getByIds(idOrNumber)
    else initSteps.getByName(idOrNumber)
}

@Singleton
class SoftwareSrv @Inject() (implicit db: Database) extends VertexSrv[Software, SoftwareSteps] {
  override def steps(raw: GremlinScala[Vertex])(implicit graph: Graph): SoftwareSteps                 = new SoftwareSteps(raw)
  def create(e: Software)(implicit graph: Graph, authContext: AuthContext): Try[Software with Entity] = createEntity(e)
}

@Singleton
class ModernSchema @Inject() (implicit db: Database) extends Schema {
  val personSrv                                    = new PersonSrv
  val softwareSrv                                  = new SoftwareSrv
  val knowsSrv                                     = new EdgeSrv[Knows, Person, Person]
  val createdSrv                                   = new EdgeSrv[Created, Person, Software]
  val vertexServices: Seq[VertexSrv[_, _]]         = Seq(personSrv, softwareSrv)
  override def modelList: Seq[Model]               = (vertexServices :+ knowsSrv :+ createdSrv).map(_.model)
  override def initialValues: Seq[InitialValue[_]] = vertexServices.map(_.getInitialValues).flatten
}

object ModernDatabaseBuilder {

  def build(schema: ModernSchema)(implicit db: Database, authContext: AuthContext): Try[Unit] =
    db.createSchemaFrom(schema).flatMap { _ =>
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
