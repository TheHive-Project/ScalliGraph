package org.thp.scalligraph.models

import scala.util.{Success, Try}

import gremlin.scala.{asScalaGraph, Graph, GremlinScala, Key, P, Vertex}
import javax.inject.{Inject, Singleton}
import org.thp.scalligraph._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.services._

@VertexEntity
case class Person(name: String, age: Int)

@VertexEntity
case class Software(name: String, lang: String)

@EdgeEntity[Person, Person]
case class Knows(weight: Double)

@EdgeEntity[Person, Software]
case class Created(weight: Double)

@EntitySteps[Person]
class PersonSteps(raw: GremlinScala[Vertex])(implicit db: Database, graph: Graph) extends BaseVertexSteps[Person, PersonSteps](raw) {
  override def get(id: String): PersonSteps = new PersonSteps(graph.V().has(Key("name") of id))

  def created = new SoftwareSteps(raw.outTo[Created])

  def created(predicate: P[Double]) = new SoftwareSteps(raw.outToE[Created].has(Key[Double]("weight"), predicate).inV())

  def connectedEdge: List[String] = raw.outE().label().toList

  def knownLevels: List[Double] = raw.outToE[Knows].value[Double]("weight").toList()

  def knows: PersonSteps = new PersonSteps(raw.outTo[Knows])

  def friends(threshold: Double = 0.8): PersonSteps = new PersonSteps(raw.outToE[Knows].has(Key[Double]("weight"), P.gte(threshold)).inV())

  def remove(): Boolean = {
    raw.drop().iterate()
    true
  }

  override def newInstance(raw: GremlinScala[Vertex]): PersonSteps = new PersonSteps(raw)
}

@EntitySteps[Software]
class SoftwareSteps(raw: GremlinScala[Vertex])(implicit db: Database, graph: Graph) extends BaseVertexSteps[Software, SoftwareSteps](raw) {
  def createdBy = new PersonSteps(raw.in("Created"))

  def isRipple = new SoftwareSteps(raw.has(Key("name") of "ripple"))

  override def newInstance(raw: GremlinScala[Vertex]): SoftwareSteps = new SoftwareSteps(raw)
  override def get(id: String): SoftwareSteps                        = new SoftwareSteps(graph.V().has(Key("name") of id))
}

@Singleton
class PersonSrv @Inject()(implicit db: Database) extends VertexSrv[Person, PersonSteps] {
  override val initialValues: Seq[Person]                                           = Seq(Person("marc", 34), Person("franck", 28))
  override def steps(raw: GremlinScala[Vertex])(implicit graph: Graph): PersonSteps = new PersonSteps(raw)
  override def get(id: String)(implicit graph: Graph): PersonSteps                  = initSteps.get(id)
}

@Singleton
class SoftwareSrv @Inject()(implicit db: Database) extends VertexSrv[Software, SoftwareSteps] {
  override def steps(raw: GremlinScala[Vertex])(implicit graph: Graph): SoftwareSteps = new SoftwareSteps(raw)
  override def get(id: String)(implicit graph: Graph): SoftwareSteps                  = initSteps.get(id)
}

@Singleton
class ModernSchema @Inject()(implicit db: Database) extends Schema {
  val personSrv                                    = new PersonSrv
  val softwareSrv                                  = new SoftwareSrv
  val knowsSrv                                     = new EdgeSrv[Knows, Person, Person]
  val createdSrv                                   = new EdgeSrv[Created, Person, Software]
  val vertexServices: Seq[VertexSrv[_, _]]         = Seq(personSrv, softwareSrv)
  override def modelList: Seq[Model]               = (vertexServices :+ knowsSrv :+ createdSrv).map(_.model)
  override def initialValues: Seq[InitialValue[_]] = vertexServices.map(_.getInitialValues).flatten
}

object DatabaseBuilder {

  def build(schema: ModernSchema)(implicit db: Database, authContext: AuthContext): Try[Unit] =
    for {
      _ <- db.createSchemaFrom(schema)
      _ <- db.tryTransaction { implicit graph =>
        val vadas  = schema.personSrv.create(Person("vadas", 27))
        val marko  = schema.personSrv.create(Person("marko", 29))
        val josh   = schema.personSrv.create(Person("josh", 32))
        val peter  = schema.personSrv.create(Person("peter", 35))
        val lop    = schema.softwareSrv.create(Software("lop", "java"))
        val ripple = schema.softwareSrv.create(Software("ripple", "java"))
        schema.knowsSrv.create(Knows(0.5), marko, vadas)
        schema.knowsSrv.create(Knows(1), marko, josh)
        schema.createdSrv.create(Created(0.4), marko, lop)
        schema.createdSrv.create(Created(1), josh, ripple)
        schema.createdSrv.create(Created(0.4), josh, lop)
        schema.createdSrv.create(Created(0.2), peter, lop)
        Success(())
      }
    } yield ()
}
