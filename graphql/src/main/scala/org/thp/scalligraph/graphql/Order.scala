package org.thp.scalligraph.graphql

import scala.reflect.{classTag, ClassTag}
import scala.util.Try

import gremlin.scala.{__, By, Element, OrderBy}
import org.thp.scalligraph.auth.AuthContext
import org.thp.scalligraph.models.ScalliSteps
import org.thp.scalligraph.query.{AuthGraph, PublicProperty}
import sangria.marshalling.{CoercedScalaResultMarshaller, FromInput, ResultMarshaller}
import sangria.schema._
import sangria.util.tag.@@

object Order {

  lazy val orderEnumeration = EnumType(
    "Order",
    values = List(
      EnumValue("decr", value = org.apache.tinkerpop.gremlin.process.traversal.Order.decr),
      EnumValue("incr", value = org.apache.tinkerpop.gremlin.process.traversal.Order.incr),
      EnumValue("shuffle", value = org.apache.tinkerpop.gremlin.process.traversal.Order.shuffle)
    )
  )

  def getField[S <: ScalliSteps[_, E, S]: ClassTag, E <: Element](
      properties: List[PublicProperty[_ <: Element, _, _]],
      stepType: OutputType[S]): Option[Field[AuthGraph, S]] = {

    case class FieldOrder(property: PublicProperty[_ <: Element, _, _], order: org.apache.tinkerpop.gremlin.process.traversal.Order) {
      def orderBy(authContext: Option[AuthContext]): OrderBy[_] = By(property(__, authContext), order)
    }

    val fields = properties.map(p ⇒ InputField(p.propertyName, OptionInputType(orderEnumeration)))
    val inputType: InputObjectType[Seq[FieldOrder]] =
      InputObjectType[Seq[FieldOrder]](classTag[S].runtimeClass.getSimpleName + "Order", fields)

    val fromInput: FromInput[Seq[FieldOrder]] = new FromInput[Seq[FieldOrder]] {
      override val marshaller: ResultMarshaller = CoercedScalaResultMarshaller.default

      override def fromResult(node: marshaller.Node): Seq[FieldOrder] = {
        val input = node.asInstanceOf[Map[String, Option[Any]]]
        for {
          (key, valueMaybe) ← input.toSeq
          value             ← valueMaybe
          order             ← Try(org.apache.tinkerpop.gremlin.process.traversal.Order.valueOf(value.toString)).toOption
          property          ← properties.find(_.propertyName == key)
        } yield FieldOrder(property, order)
      }
    }
    val arg = Argument("order", inputType)(
      fromInput.asInstanceOf[FromInput[Seq[FieldOrder] @@ FromInput.InputObjectResult]],
      WithoutInputTypeTags.ioArgTpe[Seq[FieldOrder]]
    )
    Some(
      Field[AuthGraph, S, S, S](
        "order",
        stepType,
        arguments = List(arg),
        resolve = ctx ⇒ ctx.value.sort(ctx.arg(arg).map(_.orderBy(ctx.ctx.auth)): _*)))
  }
}
