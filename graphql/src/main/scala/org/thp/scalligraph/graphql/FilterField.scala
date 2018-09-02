package org.thp.scalligraph.graphql

import gremlin.scala.{Element, GremlinScala}
import org.thp.scalligraph.models.EntityFilter
import sangria.schema.{InputField, InputType, OptionInputType}

/**
  * FilterField builds an EntityFilter from a parameter.
  *
  * @param name      name of the field the FilterField applies on
  * @param inputType used to read parameter value from user input
  * @param filter    the function to build the EntityFilter from the parameter value
  * @tparam G graph type of the document which contains the field (Vertex or Edge)
  * @tparam V type of the parameter
  */
class FilterField[G <: Element, V](val name: String, inputType: InputType[V], filter: V ⇒ EntityFilter[G]) extends (V ⇒ EntityFilter[G]) {
  def apply(value: V): EntityFilter[G] = filter(value)

  def inputField: InputField[Option[V]] = InputField(name, OptionInputType(inputType))
}

object FilterField {
  def apply[G <: Element, V](name: String, inputType: InputType[V], filter: V ⇒ GremlinScala[G] ⇒ GremlinScala[G]): FilterField[G, V] =
    new FilterField[G, V](name, inputType, filter.andThen(EntityFilter[G]))
}
