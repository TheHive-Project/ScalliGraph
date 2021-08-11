package org.thp.scalligraph.utils

import org.thp.scalligraph.`macro`.FieldSelectorMacro

import scala.language.experimental.macros
import scala.reflect.runtime.{universe => ru}

case class FieldSelector[S, T: ru.TypeTag](name: String) {
  override def toString: String = s"Selector($name, ${ru.typeOf[T]})"
}
object FieldSelector {
  implicit def selectorProvider[S, T](select: T): FieldSelector[S, T] = macro FieldSelectorMacro.selectorProviderImpl[S, T]
}
