package org.thp.scalligraph.services

import org.apache.tinkerpop.gremlin.structure.Element
import org.thp.scalligraph.models.UMapping

trait ElementOps {
  implicit class ElementOpsDef[E <: Element](element: E) {
    def getProperty[T](propertyName: String)(implicit mapping: UMapping[T]): T =
      mapping.toMapping.getProperty(element, propertyName)

    def setProperty[T](propertyName: String, value: T)(implicit mapping: UMapping[T]): E = {
      mapping.toMapping.setProperty(element, propertyName, value)
      element
    }
  }
}

object ElementOps extends ElementOps
