package org.thp.scalligraph.controllers

import scala.util.Try

import org.scalactic.{ Bad, Good, One, Or }
import org.thp.scalligraph.FPath

class UpdateFieldsParserMacroTest extends Specification with TestUtils {

  "UpdateFieldParser macro" should {

    "parse a simple class" in {
      val fieldsParser = getUpdateFieldsParser[SimpleClassForFieldsParserMacroTest]
      val fields       = FObject("name" -> FString("simpleClass"))
      val updates      = Map(FPath("name") -> UpdateOps.SetAttribute("simpleClass"))
      fieldsParser(fields) must_=== Good(updates)
    }

    "make all fields of complex class updatable" in {
      val fieldsParser = getUpdateFieldsParser[ComplexClassForFieldsParserMacroTest]
      fieldsParser.parsers.keys.map(_.toString) must contain(
        exactly("", "name", "value", "subClasses", "subClasses[]", "subClasses[].name", "subClasses[].option"))
    }

    "parse complex class" in {
      val fieldsParser = getUpdateFieldsParser[ComplexClassForFieldsParserMacroTest]
      val fields       = FObject("subClasses[0].name" -> FString("sc1"), "subClasses[1].option" -> FNull)
      val updates      = Map(FPath("subClasses[0].name") -> UpdateOps.SetAttribute("sc1"), FPath("subClasses[1].option") -> UpdateOps.UnsetAttribute)
      fieldsParser(fields) must_=== Good(updates)
    }

    "parse class with annotation" in {
      val fieldsParser = getUpdateFieldsParser[ClassWithAnnotation]
      val fields       = FObject("valueFr" -> FString("un"), "valueEn" -> FString("three"))
      val updates      = Map(FPath("valueFr") -> UpdateOps.SetAttribute(LocaleInt(1)), FPath("valueEn") -> UpdateOps.SetAttribute(LocaleInt(3)))
      fieldsParser(fields) must_=== Good(updates)
    }

    "parse class with implicit" in {
      implicit val subClassFieldsParser: UpdateFieldsParser[SubClassForFieldsParserMacroTest] = UpdateFieldsParser[SubClassForFieldsParserMacroTest](
        "SubClassForFieldsParserMacroTest")(FPath("option") -> FieldsParser[UpdateOps.Type]("Optional Int") {
        case (_, FString(intStr)) =>
          Or.from(Try(intStr.toInt)) match {
            case Good(i) => Good(UpdateOps.SetAttribute(i))
            case Bad(_)  => Good(UpdateOps.UnsetAttribute)
          }
      })
      val fieldsParser = getUpdateFieldsParser[ComplexClassForFieldsParserMacroTest]
      val fields       = FObject("subClasses[0].option" -> FString("3"), "subClasses[1].option" -> FString("invalid => unset attribute"))
      val updates      = Map(FPath("subClasses[0].option") -> UpdateOps.SetAttribute(3), FPath("subClasses[1].option") -> UpdateOps.UnsetAttribute)
      fieldsParser(fields) must_=== Good(updates)
    }

    "return an error if provided fields is not correct" in {
      val fieldsParser = getUpdateFieldsParser[SimpleClassForFieldsParserMacroTest]
      val fields       = FObject("name" -> FNumber(12)) // invalid format

      fieldsParser(fields) must_=== Bad(One(InvalidFormatAttributeError("name", "string", Seq("string"), FNumber(12))))
    }
  }
}
