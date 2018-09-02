package org.thp.scalligraph.models

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.thp.scalligraph.WithOutput
import play.api.libs.json._

class JsonMacroTest extends Specification with TestUtils with Mockito {
  "Json macro" should {
    "write simple entity" in {
      case class SimpleClass(name: String, value: Int)
      val writes = getJsonWrites[SimpleClass]

      val simpleClass = SimpleClass("simple_class", 42)

      val expectedOutput = Json.obj("name" → "simple_class", "value" → 42)

      writes.writes(simpleClass).as[JsObject].fields must containTheSameElementsAs(expectedOutput.fields)
    }

    "write entity with option, sequence and sub-class attributes" in {
      case class SubClass1(name: String, option: Option[Int])
      case class ComplexClass(name: String, value: Int, subClasses: Seq[SubClass1])
      val writes = getJsonWrites[ComplexClass]

      val complexClassEntity = ComplexClass("complex_class", 42, Seq(SubClass1("sc1", Some(12)), SubClass1("sc2", None)))

      val expectedOutput = Json.obj(
        "name"  → "complex_class",
        "value" → 42,
        "subClasses" → Json.arr(
          Json.obj("name" → "sc1", "option" → 12),
          Json.obj("name" → "sc2", "option" → JsNull),
        ))

      writes.writes(complexClassEntity).as[JsObject].fields must containTheSameElementsAs(expectedOutput.fields)
    }

    "write simple entity with annotation" in {
      val nameWrites = Writes[String] { name ⇒
        JsArray(name.toSeq.map(c ⇒ JsString(c.toString)))
      }
      val subClassWrites1 = Writes[SubClass] { subClass ⇒
        JsString(s"SubClass1(${subClass.name})")
      }
      val subClassWrites2 = Writes[SubClass] { subClass ⇒
        JsString(s"SubClass2(${subClass.name})")
      }
      @WithOutput(subClassWrites2)
      case class SubClass(name: String)
      case class SimpleClass(
          @WithOutput(nameWrites)
          name: String,
          value: Int,
          @WithOutput(subClassWrites1)
          sc1: SubClass,
          sc2: SubClass)
      val writes = getJsonWrites[SimpleClass]

      val simpleClassEntity = SimpleClass("simple_class", 42, SubClass("sc1"), SubClass("sc2"))
      val expectedOutput = Json.obj(
        "name"  → Json.arr("s", "i", "m", "p", "l", "e", "_", "c", "l", "a", "s", "s"),
        "value" → 42,
        "sc1"   → "SubClass1(sc1)",
        "sc2"   → "SubClass2(sc2)")

      writes.writes(simpleClassEntity).as[JsObject].fields must containTheSameElementsAs(expectedOutput.fields)
    }

    "write simple entity with implicit" in {
      implicit val subClassWrites: Writes[SubClass] = Writes[SubClass] { subClass ⇒
        JsString(s"subClass(${subClass.name})")
      }
      case class SubClass(name: String)
      case class SimpleClass(name: String, value: Int, subClass: SubClass)
      val writes = getJsonWrites[SimpleClass]

      val simpleClassEntity = SimpleClass("simple_class", 42, SubClass("value"))
      val expectedOutput    = Json.obj("name" → "simple_class", "value" → 42, "subClass" → "subClass(value)")

      writes.writes(simpleClassEntity).as[JsObject].fields must containTheSameElementsAs(expectedOutput.fields)
    }
  }
}
