package org.thp.scalligraph.models

import gremlin.scala.{Element, Key, P, Vertex}
import org.thp.scalligraph.controllers.FieldsParser
import org.thp.scalligraph.query._
import org.thp.scalligraph.Output
import play.api.libs.json.{Json, OWrites}

import scala.language.implicitConversions

case class OutputPerson(createdBy: String, label: String, name: String, age: Int)
object OutputPerson {
  implicit val writes: OWrites[OutputPerson] = Json.writes[OutputPerson]
}

case class OutputSoftware(createdBy: String, name: String, lang: String)
object OutputSoftware {
  implicit val writes: OWrites[OutputSoftware] = Json.writes[OutputSoftware]
}

object ModernOutputs {
  implicit def toOutputPerson(person: Person with Entity): Output[OutputPerson] =
    new Output(new OutputPerson(person._createdBy, s"Mister ${person.name}", person.name, person.age))
  implicit def toOutputSoftware(software: Software with Entity): Output[OutputSoftware] =
    new Output(new OutputSoftware(software._createdBy, software.name, software.lang))
}

case class SeniorAgeThreshold(age: Int)
object SeniorAgeThreshold {
  implicit val parser: FieldsParser[SeniorAgeThreshold] = FieldsParser[SeniorAgeThreshold]
}
case class FriendLevel(level: Double)
object FriendLevel {
  implicit val reads: FieldsParser[FriendLevel] = FieldsParser[FriendLevel]
}

class ModernQueryExecutor(implicit val db: Database) extends QueryExecutor {
  import ModernOutputs._
  val personSrv   = new PersonSrv
  val softwareSrv = new SoftwareSrv

  override val publicProperties: List[PublicProperty[_ <: Element, _]] = {
    val labelMapping = SingleMapping[String, String](
      toGraphOptFn = {
        case d if d startsWith "Mister " ⇒ Some(d.drop(7))
        case d                           ⇒ Some(d)
      },
      toDomainFn = (g: String) ⇒ "Mister " + g
    )
    // format: off
    PublicPropertyListBuilder[PersonSteps, Vertex]
      .property[String]("createdBy").rename("_createdBy")
      .property[String]("label")(labelMapping).rename("name")
      .property[String]("name").simple
      .property[Int]("age").simple
      .build :::
    PublicPropertyListBuilder[SoftwareSteps, Vertex]
      .property[String]("createdBy").rename("_createdBy")
      .property[String]("name").simple
      .property[String]("lang").simple
      .property[String]("any").seq(
        _.simple("_createdBy")
         .simple("name")
         .simple("lang"))
      .build
  }

  override val queries: Seq[ParamQuery[_]] = Seq(
    Query.init[PersonSteps]("allPeople", (graph, _) ⇒ personSrv.initSteps(graph)),
    Query.init[SoftwareSteps]("allSoftware", (graph, _) ⇒ softwareSrv.initSteps(graph)),
    Query.initWithParam[SeniorAgeThreshold, PersonSteps]("seniorPeople", { (seniorAgeThreshold, graph, _) ⇒
      personSrv.initSteps(graph).where(_.has(Key[Int]("age"), P.gte(seniorAgeThreshold.age)))
    }),
    Query[PersonSteps, SoftwareSteps]("created", (personSteps, _) ⇒ personSteps.created),
    Query.withParam[FriendLevel, PersonSteps, PersonSteps]("friends", (friendLevel, personSteps, _) ⇒ personSteps.friends(friendLevel.level)),
    Query[Person with Entity, Output[OutputPerson]]("output", (person, _) ⇒ person),
    Query[Software with Entity, Output[OutputSoftware]]("output", (software, _) ⇒ software)
  )
}
