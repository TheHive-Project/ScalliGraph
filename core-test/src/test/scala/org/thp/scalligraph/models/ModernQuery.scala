package org.thp.scalligraph.models

import scala.language.implicitConversions

import play.api.libs.json.{Json, OWrites}

import gremlin.scala.{Key, P}
import org.thp.scalligraph.Output
import org.thp.scalligraph.controllers.FieldsParser
import org.thp.scalligraph.query._

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
    Output(new OutputPerson(person._createdBy, s"Mister ${person.name}", person.name, person.age))
  implicit def toOutputSoftware(software: Software with Entity): Output[OutputSoftware] =
    Output(new OutputSoftware(software._createdBy, software.name, software.lang))
}

case class SeniorAgeThreshold(age: Int)
case class FriendLevel(level: Double)

class ModernQueryExecutor(implicit val db: Database) extends QueryExecutor {
  import ModernOutputs._
  val personSrv   = new PersonSrv
  val softwareSrv = new SoftwareSrv

  override val version: (Int, Int) = 1 -> 1

  override val publicProperties: List[PublicProperty[_, _]] = {
    val labelMapping = SingleMapping[String, String](
      "",
      toGraphOptFn = {
        case d if d startsWith "Mister " => Some(d.drop(7))
        case d                           => Some(d)
      },
      toDomainFn = (g: String) => "Mister " + g
    )
    PublicPropertyListBuilder[PersonSteps]
      .property[String]("createdBy")(_.rename("_createdBy").readonly)
      .property[String]("label")(_.rename("name").updatable)(labelMapping)
      .property[String]("name")(_.simple.updatable)
      .property[Int]("age")(_.simple.updatable)
      .build :::
      PublicPropertyListBuilder[SoftwareSteps]
        .property[String]("createdBy")(_.rename("_createdBy").readonly)
        .property[String]("name")(_.simple.updatable)
        .property[String]("lang")(_.simple.updatable)
        .property[String]("any")(_.derived(_.value[String]("_createdBy"), _.value[String]("name"), _.value[String]("lang")).readonly)
        .build
  }

  override val queries: Seq[ParamQuery[_]] = Seq(
    Query.init[PersonSteps]("allPeople", (graph, _) => personSrv.initSteps(graph)),
    Query.init[SoftwareSteps]("allSoftware", (graph, _) => softwareSrv.initSteps(graph)),
    Query.initWithParam[SeniorAgeThreshold, PersonSteps](
      "seniorPeople",
      FieldsParser[SeniorAgeThreshold], { (seniorAgeThreshold, graph, _) =>
        personSrv.initSteps(graph).has(Key[Int]("age"), P.gte(seniorAgeThreshold.age))
      }
    ),
    Query[PersonSteps, SoftwareSteps]("created", (personSteps, _) => personSteps.created),
    Query.withParam[FriendLevel, PersonSteps, PersonSteps](
      "friends",
      FieldsParser[FriendLevel],
      (friendLevel, personSteps, _) => personSteps.friends(friendLevel.level)
    ),
    Query[Person with Entity, Output[OutputPerson]]("output", (person, _) => person),
    Query[Software with Entity, Output[OutputSoftware]]("output", (software, _) => software)
  )
}
