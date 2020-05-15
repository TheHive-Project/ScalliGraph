package org.thp.scalligraph.models

import gremlin.scala.P
import org.thp.scalligraph.controllers.{FieldsParser, Renderer}
import org.thp.scalligraph.query._
import org.thp.scalligraph.steps.StepsOps._
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
  implicit val personOutput: Renderer[Person with Entity] =
    Renderer.json[Person with Entity, OutputPerson](person => new OutputPerson(person._createdBy, s"Mister ${person.name}", person.name, person.age))
  implicit val softwareOutput: Renderer[Software with Entity] =
    Renderer.json[Software with Entity, OutputSoftware](software => new OutputSoftware(software._createdBy, software.name, software.lang))
}

case class SeniorAgeThreshold(age: Int)
case class FriendLevel(level: Double)

class ModernQueryExecutor(implicit val db: Database) extends QueryExecutor {
  import ModernOutputs._
  val personSrv   = new PersonSrv
  val softwareSrv = new SoftwareSrv

  override val version: (Int, Int) = 1 -> 1

  override lazy val publicProperties: List[PublicProperty[_, _]] = {
    val labelMapping = SingleMapping[String, String](
      toGraphOptFn = {
        case d if d startsWith "Mister " => Some(d.drop(7))
        case d                           => Some(d)
      },
      toDomainFn = (g: String) => "Mister " + g
    )
    PublicPropertyListBuilder[PersonSteps]
      .property("createdBy", UniMapping.string)(_.rename("_createdBy").readonly)
      .property("label", labelMapping)(_.rename("name").updatable)
      .property("name", UniMapping.string)(_.field.updatable)
      .property("age", UniMapping.int)(_.field.updatable)
      .build :::
      PublicPropertyListBuilder[SoftwareSteps]
        .property("createdBy", UniMapping.string)(_.rename("_createdBy").readonly)
        .property("name", UniMapping.string)(_.field.updatable)
        .property("lang", UniMapping.string)(_.field.updatable)
        .property("any", UniMapping.string)(
          _.select(_.property("_createdBy", UniMapping.string), _.property("name", UniMapping.string), _.property("lang", UniMapping.string)).readonly
        )
        .build
  }

  override lazy val queries: Seq[ParamQuery[_]] = Seq(
    Query.init[PersonSteps]("allPeople", (graph, _) => personSrv.initSteps(graph)),
    Query.init[SoftwareSteps]("allSoftware", (graph, _) => softwareSrv.initSteps(graph)),
    Query.initWithParam[SeniorAgeThreshold, PersonSteps](
      "seniorPeople",
      FieldsParser[SeniorAgeThreshold], { (seniorAgeThreshold, graph, _) =>
        personSrv.initSteps(graph).has("age", P.gte(seniorAgeThreshold.age))
      }
    ),
    Query[PersonSteps, SoftwareSteps]("created", (personSteps, _) => personSteps.created),
    Query.withParam[FriendLevel, PersonSteps, PersonSteps](
      "friends",
      FieldsParser[FriendLevel],
      (friendLevel, personSteps, _) => personSteps.friends(friendLevel.level)
    ),
    Query.output[Person with Entity, PersonSteps],
    Query.output[Software with Entity, SoftwareSteps]
  )
}
