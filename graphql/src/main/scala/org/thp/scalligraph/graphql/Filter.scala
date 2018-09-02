package org.thp.scalligraph.graphql

import gremlin.scala.{Element, Key, P}
import org.apache.tinkerpop.gremlin.process.traversal.{P ⇒ JavaP}
import org.thp.scalligraph.models.EntityFilter
import sangria.marshalling.{CoercedScalaResultMarshaller, FromInput, ResultMarshaller}
import sangria.schema._
import sangria.util.tag.@@

import scala.reflect.macros.blackbox

object Filter {
  def startsWith(value: String) =
    new JavaP((a: String, b: String) ⇒ a.startsWith(b), value)
  def string[G <: Element](fieldName: String): List[FilterField[G, Any]] =
    List(
      // text: String # matches all nodes with exact value
      FilterField[G, String](fieldName, StringType, value ⇒ _.has(Key[String](fieldName), P.is(value))),
      // text_not: String # matches all nodes with different value
      FilterField[G, String](s"${fieldName}_not", StringType, value ⇒ _.has(Key[String](fieldName), P.neq(value))),
      // text_in: [String!] # matches all nodes with value in the passed list
      FilterField[G, Seq[String]](s"${fieldName}_in", ListInputType(StringType), value ⇒ _.has(Key[String](fieldName), P.within(value))),
      //  text_not_in: [String!] # matches all nodes with value not in the passed list
      FilterField[G, Seq[String]](s"${fieldName}_not_in", ListInputType(StringType), value ⇒ _.has(Key[String](fieldName), P.without(value))),
      // text_lt: String # matches all nodes with lesser value
      FilterField[G, String](s"${fieldName}_lt", StringType, value ⇒ _.has(Key[String](fieldName), P.lt(value))),
      // text_lte: String # matches all nodes with lesser or equal value
      FilterField[G, String](s"${fieldName}_lte", StringType, value ⇒ _.has(Key[String](fieldName), P.lte(value))),
      // text_gt: String # matches all nodes with greater value
      FilterField[G, String](s"${fieldName}_gt", StringType, value ⇒ _.has(Key[String](fieldName), P.gt(value))),
      // text_gte: String # matches all nodes with greater or equal value
      FilterField[G, String](s"${fieldName}_gte", StringType, value ⇒ _.has(Key[String](fieldName), P.gte(value))),
      // text_contains: String # matches all nodes with a value that contains given substring
      FilterField[G, String](s"${fieldName}_contains", StringType, value ⇒ _.where(_.map(_.value[String](fieldName)).filterOnEnd(_.contains(value)))),
      // text_not_contains: String # matches all nodes with a value that does not contain given substring
      FilterField[G, String](
        s"${fieldName}_not_contains",
        StringType,
        value ⇒ _.where(_.map(_.value[String](fieldName)).filterOnEnd(!_.contains(value)))),
      // text_starts_with: String # matches all nodes with a value that starts with given substring
      FilterField[G, String](
        s"${fieldName}_starts_with",
        StringType,
        value ⇒ _.where(_.map(_.value[String](fieldName)).filterOnEnd(_.startsWith(value)))),
      // text_not_starts_with: String # matches all nodes with a value that does not start with given substring
      FilterField[G, String](
        s"${fieldName}_not_starts_with",
        StringType,
        value ⇒ _.where(_.map(_.value[String](fieldName)).filterOnEnd(!_.startsWith(value)))),
      // text_ends_with: String # matches all nodes with a value that ends with given substring
      FilterField[G, String](
        s"${fieldName}_ends_with",
        StringType,
        value ⇒ _.where(_.map(_.value[String](fieldName)).filterOnEnd(_.endsWith(value)))),
      // text_not_ends_with: String # matches all nodes with a value that does not end with given substring
      FilterField[G, String](
        s"${fieldName}_not_ends_with",
        StringType,
        value ⇒ _.where(_.map(_.value[String](fieldName)).filterOnEnd(!_.endsWith(value))))
    ).asInstanceOf[List[FilterField[G, Any]]]

  def int[G <: Element](fieldName: String): List[FilterField[G, Any]] =
    List(
      // number: Int # matches all nodes with exact value
      FilterField[G, Int](fieldName, IntType, value ⇒ _.has(Key(fieldName) of value)),
      // number_not: Int # matches all nodes with different value
      FilterField[G, Int](s"${fieldName}_not", IntType, value ⇒ _.hasNot(Key(fieldName) of value)),
      // number_in: [Int!] # matches all nodes wit
      // h value in the passed list
      FilterField[G, Seq[Int]](s"${fieldName}_in", ListInputType(IntType), value ⇒ _.has(Key[Int](fieldName), P.within(value))),
      // number_not_in: [Int!] # matches all nodes with value not in the passed list
      FilterField[G, Seq[Int]](s"${fieldName}_not_in", ListInputType(IntType), value ⇒ _.has(Key[Int](fieldName), P.without(value))),
      // number_lt: Int # matches all nodes with lesser value
      FilterField[G, Int](s"${fieldName}_lt", IntType, value ⇒ _.has(Key[Int](fieldName), P.lt(value))),
      // number_lte: Int # matches all nodes with lesser or equal value
      FilterField[G, Int](s"${fieldName}_lte", IntType, value ⇒ _.has(Key[Int](fieldName), P.lte(value))),
      // number_gt: Int # matches all nodes with greater value
      FilterField[G, Int](s"${fieldName}_gt", IntType, value ⇒ _.has(Key[Int](fieldName), P.gt(value))),
      // number_gte: Int # matches all nodes with greater or equal value
      FilterField[G, Int](s"${fieldName}_gte", IntType, value ⇒ _.has(Key[Int](fieldName), P.gte(value))),
    ).asInstanceOf[List[FilterField[G, Any]]]

  def fromInput[G <: Element](fields: List[FilterField[G, Any]]): FromInput[EntityFilter[G] @@ FromInput.InputObjectResult] = {
    val fi: FromInput[EntityFilter[G]] = new FromInput[EntityFilter[G]] {
      override val marshaller: ResultMarshaller = CoercedScalaResultMarshaller.default

      override def fromResult(node: marshaller.Node): EntityFilter[G] =
        EntityFilter.build[G](node.asInstanceOf[Map[String, Option[Any]]], fields.map(f ⇒ f.name → f).toMap)
    }
    fi.asInstanceOf[FromInput[EntityFilter[G] @@ FromInput.InputObjectResult]]
  }

  def inputObjectType[G <: Element](objectName: String, fields: List[FilterField[G, _]]): InputObjectType[EntityFilter[G]] =
    InputObjectType[EntityFilter[G]](objectName, fields.map(_.inputField))
}

trait FilterMacro { _: SchemaMacro ⇒
  val c: blackbox.Context

  import c.universe._

  def buildEntityFilterFields(gTpe: Type, eTpe: Type): Tree =
    eTpe match {
      case CaseClassType(symbols @ _*) ⇒
        symbols
          .map { symbol ⇒
            val sTpe = symbol.typeSignature
            if (sTpe <:< typeOf[String])
              q"org.thp.scalligraph.graphql.Filter.string[$gTpe, $eTpe](${symbol.name.toString})"
            else if (sTpe <:< typeOf[Int])
              q"org.thp.scalligraph.graphql.Filter.int[$gTpe, $eTpe](${symbol.name.toString})"
            else {
              warn(s"Field ${symbol.name} in $eTpe has an unrecognized type for filter ($sTpe)")
              q"Nil"
            }
          }
          .reduceOption((f1, f2) ⇒ q"$f1 ::: $f2")
          .getOrElse(q"Nil")
      // TODO? add field for native type. Fieldname="value" ? => NO filter is applied on the document, not on the field directly.
      case RefinedType(baseType :: _, _) ⇒ buildEntityFilterFields(gTpe, baseType)
      case _ ⇒
        warn(s"Filter can't be built for $eTpe as it is not a case class")
        q"Nil"
    }
}
