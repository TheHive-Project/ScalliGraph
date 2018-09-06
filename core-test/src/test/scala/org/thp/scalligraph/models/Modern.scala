package org.thp.scalligraph.models

import gremlin.scala.{asScalaGraph, Graph, GremlinScala, Key, Vertex}
import org.thp.scalligraph._
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.services.{EdgeSrv, VertexSrv}

@VertexEntity
case class Person(name: String, age: Int)

object Person {
  val initialValues = Seq(Person("marc", 34), Person("franck", 28))
}

@JsonOutput
@VertexEntity
case class Software(name: String, lang: String)

@EdgeEntity[Person, Person]
case class Knows(weight: Double)

@EdgeEntity[Person, Software]
case class Created(weight: Double)

@EntitySteps[Person]
class PersonSteps(raw: GremlinScala[Vertex])(implicit db: Database) extends BaseVertexSteps[Person, PersonSteps](raw) {
  def created = new SoftwareSteps(raw.out("Created"))

  def connectedEdge: List[String] = raw.outE().label().toList

  def knownLevels: List[Double] = raw.outE("Knows").value[Double]("weight").toList()

  def remove(): Boolean = {
    raw.drop().iterate()
    true
  }

  override def newInstance(raw: GremlinScala[Vertex]): PersonSteps = new PersonSteps(raw)
  //def name = new Steps[String, String, Labels](raw.map(_.value[String]("name")))

}

@EntitySteps[Software]
class SoftwareSteps(raw: GremlinScala[Vertex])(implicit db: Database) extends BaseVertexSteps[Software, SoftwareSteps](raw) {
  def createdBy = new PersonSteps(raw.in("Created"))

  def isRipple = new SoftwareSteps(raw.has(Key("name") of "ripple"))

  override def newInstance(raw: GremlinScala[Vertex]): SoftwareSteps = new SoftwareSteps(raw)
}

class PersonOps(person: Person with Entity) {
//  def update(graph: Graph, field: String, value: String): Boolean = {
//    ModernSchema.personModel.update(person._id, field, value)(graph)
//    true
//  }
}

class PersonSrv(implicit db: Database) extends VertexSrv[Person, PersonSteps] {
  override val initialValues                                       = Seq(Person("marc", 34), Person("franck", 28))
  override def steps(raw: GremlinScala[Vertex]): PersonSteps       = new PersonSteps(raw)
  override def get(id: String)(implicit graph: Graph): PersonSteps = new PersonSteps(graph.V().has(Key("name") of id))
}

class SoftwareSrv(implicit db: Database) extends VertexSrv[Software, SoftwareSteps] {
  override def steps(raw: GremlinScala[Vertex]): SoftwareSteps       = new SoftwareSteps(raw)
  override def get(id: String)(implicit graph: Graph): SoftwareSteps = new SoftwareSteps(graph.V().has(Key("name") of id))
}

class ModernSchema(implicit db: Database, authContext: AuthContext) {
  val personSrv   = new PersonSrv
  val softwareSrv = new SoftwareSrv
  val knowsSrv    = new EdgeSrv[Knows, Person, Person]
  val createdSrv  = new EdgeSrv[Created, Person, Software]

  db.createSchemaFrom(this)
  db.transaction { implicit graph ⇒
    val marko  = personSrv.create(Person("marko", 29))
    val vadas  = personSrv.create(Person("vadas", 17))
    val josh   = personSrv.create(Person("josh", 32))
    val peter  = personSrv.create(Person("peter", 25))
    val lop    = softwareSrv.create(Software("lop", "java"))
    val ripple = softwareSrv.create(Software("ripple", "java"))
    knowsSrv.create(Knows(0.5), marko, vadas)
    knowsSrv.create(Knows(1), marko, josh)
    createdSrv.create(Created(0.4), marko, lop)
    createdSrv.create(Created(1), josh, ripple)
    createdSrv.create(Created(0.4), josh, lop)
    createdSrv.create(Created(0.2), peter, lop)
  }
}
