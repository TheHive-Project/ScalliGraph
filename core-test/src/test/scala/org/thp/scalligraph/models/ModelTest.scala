package org.thp.scalligraph.models

import java.util.Date

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.thp.scalligraph.JsonOutput
import play.api.libs.json.{JsNull, Json}

//@VertexEntity
//case class MyEntity(name: String, value: Int)

class ModelTest extends Specification with Mockito {

//  case class MyEntity(u: String, e: String, d: Double, v: Boolean)

  "model macro" should {

    "create model companion" in {

      MyEntity.model must_!== null
      MyEntity.model.label must_=== "MyEntity"
    }

    "output case class" in {
      @JsonOutput
      case class MyDTO(s: String, i: Int, u: String, v: Boolean)

      val expectedOutput = Json.obj("s" → "sParam", "i" → 42, "u" → "uParam", "v" → true)
      MyDTO("sParam", 42, "uParam", v = true).toJson.fields must containTheSameElementsAs(expectedOutput.fields)
    }

    "output entity" in {
      @JsonOutput
      case class MyOtherEntity(u: String, e: String, d: Double, v: Boolean)
      val myEntity: MyOtherEntity with Entity = new MyOtherEntity("uParam", "eParam", 42.1, true) with Entity {
        val _id                   = "entityId"
        val _model: Model         = null
        val _createdBy            = "me"
        val _updatedBy: None.type = None
        val _createdAt            = new Date(1507878244000L)
        val _updatedAt: None.type = None
      }
      val expectedOutput = Json.obj(
        "u"          → "uParam",
        "e"          → "eParam",
        "d"          → 42.1,
        "v"          → true,
        "_id"        → "entityId",
        "_createdAt" → 1507878244000L,
        "_createdBy" → "me",
        "_updatedAt" → JsNull,
        "_updatedBy" → JsNull,
        "_type"      → "MyOtherEntity"
      )
      myEntity.toJson(myEntity).fields must containTheSameElementsAs(expectedOutput.fields)
    }
  }
}
